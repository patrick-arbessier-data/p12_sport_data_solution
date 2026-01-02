#-------------------------------------------------
# mod50_simuler_activites.py
#-------------------------------------------------

"""
Générer un fichier CSV simulé d'activités sportives pour
alimenter le pipeline (dev/test ou simulation volumétrique).

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\gene_lignes\\mod50_simuler_activites.py

Arguments
---------
--origin : origine d'exécution (CLI|KESTRA).
          Défaut : détection automatique via variables d'environnement.
--log-level : niveau de logs (DEBUG|INFO|WARNING|ERROR).
             Défaut : P12_LOG_LEVEL sinon INFO.
--mapping : chemin vers activite_mapping.yml (optionnel).
           Défaut : src/utils/activite_mapping.yml.

Objectifs
---------
- Produire un fichier CSV simulant une activité sportive réaliste.
- Respecter les contraintes de volumétrie et de distribution paramétrées.
- Garantir l'unicité des activités (1 activité / salarié / jour).
- Fournir des données pour les tests de charge et le développement local.

Entrées
-------
- Fichiers :
  - <repo>/data/raw/*rh*.xlsx (référentiel RH - effectif cible).
  - <repo>/data/raw/*sport*.xlsx (référentiel Sport - préférences).
  - <repo>/src/utils/config_pipeline.yml (paramètres de simulation, scénarios).
  - <repo>/src/utils/activite_mapping.yml (mapping colonnes).

- Variables d'environnement :
  - P12_PSEUDO_SALT (grain de sel pour pseudonymisation stable).
  - PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (métriques).

Sorties
-------
- Fichiers :
  - <repo>/data/raw/declaratif_activites_12m.csv (fichier généré).

- Tables :
  - ops.run_metrique (métrique d'exécution SUCCESS/FAILURE).

Traitements & fonctionnalités
-----------------------------
- Chargement des référentiels RH et Sport pour définir la population.
- Attribution d'un "profil de volume" (faible/moyen/élevé) selon les déclaratifs.
- Génération temporelle sur une fenêtre glissante (paramètre config).
- Simulation des activités :
    - Tirage du sport (préférence déclarée ou aléatoire pondéré).
    - Placement temporel avec gestion des collisions (100 essais max par activité).
    - Rejet de l'activité si aucun créneau journalier n'est libre (saturation).
    - Calcul de la durée et de la distance selon des règles métiers.
- Application d'un facteur de variabilité aléatoire.
- Sur-génération (top-up) pour tenter d'atteindre le volume cible.
- Sous-échantillonnage final pour s'approcher au plus près de activites.nb_lignes.
- Pseudonymisation déterministe des matricules salariés.
- Écriture du CSV final respectant strictement le schéma défini dans activite_mapping.yml.

Contraintes
-----------
- Le fichier de sortie écrase toute version précédente.
- La graine aléatoire (seed) est fixée dans la config pour reproductibilité.
- L'échec de la génération lève une erreur bloquante.
- Les dates générées sont localisées (Europe/Paris).
- Les noms de colonnes (RH, Sport, Sortie) sont configurables via YAML.

Observations & remarques
------------------------
- La colonne 'source_donnee' est fixée à 'csv_simule'.
- Les sports d'endurance ont une distance en mètres, les autres ont distance_m = None.
- La métrique est écrite via un bloc finally pour garantir sa présence même en cas d'erreur.
"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import sys
import unicodedata
import calendar
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# -------------------------------------------------------------------
# Action 01 - Contexte d'exécution (imports projet + .env)
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env", override=False)

try:
    from src.utils.logger import get_logger, log_failure, log_success, write_run_metric
    from src.utils.normalisation import charger_yaml
except ImportError:
    pass

def _detect_origin() -> str:
    kestra_keys = (
        "KESTRA_FLOW_ID",
        "KESTRA_EXECUTION_ID",
        "KESTRA_NAMESPACE",
        "KESTRA_TASKRUN_ID",
    )
    if any(os.getenv(k) for k in kestra_keys):
        return "KESTRA"
    return "CLI"

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--origin", choices=["CLI", "KESTRA"], default=_detect_origin())
    parser.add_argument("--log-level", default=os.getenv("P12_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO")
    parser.add_argument("--mapping", default=None)
    args, _unknown = parser.parse_known_args(argv)
    return args

LOGGER = get_logger(
    script="mod50_simuler_activites",
    origin=_detect_origin(),
    level=os.getenv("P12_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO",
)

# -------------------------------------------------------------------
# Action 02 - Chargement Config & Modèles
# -------------------------------------------------------------------
@dataclass(frozen=True)
class Employe:
    id_salarie_brut: str
    mod_depl_decl: str
    sport_declare: str | None
    cle_salarie: str
    profil_volume: str

def _load_mapping_config(path_yaml: Path) -> Dict[str, Any]:
    if not path_yaml.exists():
        raise FileNotFoundError(f"Configuration mapping manquante : {path_yaml}")
    with open(path_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _get_required(cfg: dict[str, Any], key_path: str) -> Any:
    cur: Any = cfg
    for k in key_path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            raise KeyError(f"Clé manquante dans config : '{key_path}' (segment '{k}')")
        cur = cur[k]
    return cur

# -------------------------------------------------------------------
# Action 03 - Fonctions Métier
# -------------------------------------------------------------------
def pseudonymiser_cle_salarie(id_salarie_brut: str, salt: str) -> str:
    digest = hashlib.sha256((salt + "|" + id_salarie_brut).encode("utf-8")).hexdigest()
    return f"sal_{digest[:12]}"

def _normaliser_texte(valeur: str) -> str:
    texte = str(valeur).strip().lower()
    texte = unicodedata.normalize("NFKD", texte)
    return "".join(ch for ch in texte if not unicodedata.combining(ch))

def _trouver_fichier_excel(data_raw_dir: Path, mots_cles: list[str]) -> Path:
    candidats: list[Path] = []
    for p in data_raw_dir.glob("*.xlsx"):
        nom = _normaliser_texte(p.name)
        if all(_normaliser_texte(k) in nom for k in mots_cles):
            candidats.append(p)
    if not candidats:
        raise FileNotFoundError(f"Aucun fichier .xlsx trouvé dans {data_raw_dir} avec mots-clés={mots_cles}.")
    candidats.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidats[0]

def _mois_dernieres_n_fenetres(now: datetime, fenetre_mois: int, inclure_mois_courant: bool) -> list[tuple[int, int]]:
    if fenetre_mois <= 0:
        raise ValueError("fenetre_mois doit être > 0.")
    ancre = now
    if not inclure_mois_courant:
        premier_jour_mois = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=now.tzinfo)
        ancre = premier_jour_mois - timedelta(days=1)
    y, m = ancre.year, ancre.month
    mois: list[tuple[int, int]] = []
    for _ in range(fenetre_mois):
        mois.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    mois.reverse()
    return mois

def _random_date_dans_mois(rng: random.Random, tz: ZoneInfo, year: int, month: int, now: datetime) -> datetime:
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    is_current_month = (year == now.year and month == now.month)
    if is_current_month:
        max_day = min(last_day, now.day)
        day = rng.randint(1, max_day)
        hour = 12
        if day == now.day and now.hour < 12:
            hour = 0
        return datetime(year, month, day, hour, 0, 0, tzinfo=tz)
    day = rng.randint(1, last_day)
    return datetime(year, month, day, 12, 0, 0, tzinfo=tz)

# -------------------------------------------------------------------
# Action 04 - Simulation Activité
# -------------------------------------------------------------------
def _est_mode_actif(mod_depl_decl: str) -> bool:
    return mod_depl_decl in {"Marche/running", "Vélo/Trottinette/Autres"}

def _determiner_profil_volume(sport_declare: str | None, mod_depl_decl: str) -> str:
    has_sport = bool(sport_declare and sport_declare.strip())
    actif = _est_mode_actif(mod_depl_decl)
    if has_sport and actif: return "eleve"
    if (not has_sport) and (not actif): return "faible"
    return "moyen"

def _tirer_sport_principal(rng: random.Random, sport_declare: str | None, sports_endurance: list[str], sports_non_endurance: list[str]) -> str:
    if sport_declare:
        s = sport_declare.strip()
        if s in sports_endurance or s in sports_non_endurance:
            return s
    all_sports = sports_endurance + sports_non_endurance
    return all_sports[rng.randint(0, len(all_sports) - 1)]

def _generer_duree_distance(rng: random.Random, sport_est_endurance: bool, mod_depl_decl: str, regles_simulation: dict[str, Any]) -> tuple[int, int | None]:
    duree_bornes = _get_required(regles_simulation, "duree_bornes_sec")
    if not sport_est_endurance:
        b = _get_required(duree_bornes, "non_endurance")
        duree = rng.randint(int(b["min"]), int(b["max"]))
        return int(duree), None
    refs = _get_required(regles_simulation, "refs_temps_par_mode")
    facteurs = _get_required(regles_simulation, "facteurs_duree")
    ratio_cfg = _get_required(regles_simulation, "distance_ratio")
    bornes_end = _get_required(duree_bornes, "endurance")
    
    if _est_mode_actif(mod_depl_decl):
        mode_ref = "marche_running" if mod_depl_decl == "Marche/running" else "velo_trott_autres"
    else:
        mode_ref = "marche_running"
    
    ref = _get_required(refs, mode_ref)
    distance_ref_km = float(ref["distance_ref_km"])
    duree_ref_sec = float(ref["duree_ref_sec"])
    r_dist = rng.uniform(float(ratio_cfg["min"]), float(ratio_cfg["max"]))
    distance_m = max(1, int(round(distance_ref_km * 1000.0 * r_dist)))
    f_duree = rng.uniform(float(facteurs["min"]), float(facteurs["max"]))
    duree_calc = int(round(duree_ref_sec * r_dist * f_duree))
    duree_calc = max(int(bornes_end["min"]), min(int(bornes_end["max"]), duree_calc))
    return duree_calc, distance_m

def _tirer_nb_activites_par_mois(rng: random.Random, scenario_cfg: dict[str, Any], profil: str) -> int:
    borne = scenario_cfg[profil]
    return rng.randint(int(borne[0]), int(borne[1]))

def main() -> int:
    """
    Point d'entrée principal du script de simulation des activités.

    Orchestration :
    1. Initialisation de l'environnement (logs, chemins).
    2. Chargement de la configuration (YAML) et des référentiels (Excel).
    3. Préparation des structures de données (Employés, Profils).
    4. Génération itérative des activités (Tirage aléatoire sous contrainte).
    5. Sérialisation des résultats en CSV et écriture des métriques en base.

    Returns:
        int: Code de retour (0=Succès, 1=Échec).
    """
    # -----------------------------------
    # Action 05-a : Initialisation
    # -----------------------------------
    args = _parse_args(sys.argv[1:])
    global LOGGER
    LOGGER = get_logger(script="mod50_simuler_activites", origin=args.origin, level=args.log_level)

    repo_root = Path(__file__).resolve().parents[2]
    data_raw_dir = repo_root / "data" / "raw"
    config_path = repo_root / "src" / "utils" / "config_pipeline.yml"
    mapping_path = Path(args.mapping) if args.mapping else repo_root / "src" / "utils" / "activite_mapping.yml"
    out_path = data_raw_dir / "declaratif_activites_12m.csv"

    tz = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz)

    statut = "FAILED_EXCEPTION"
    lignes_simulees = 0

    try:
        # -----------------------------------
        # Action 05-b : Chargement Config
        # -----------------------------------
        cfg = charger_yaml(config_path)
        map_cfg = _load_mapping_config(mapping_path)

        simu_cfg = map_cfg["simulation"]
        cols_out = simu_cfg["cols_out"]
        rh_cols = simu_cfg["rh_cols_required"]
        col_id, col_mode = rh_cols[0], rh_cols[1]
        sport_cols = simu_cfg["sport_cols_required"]
        col_sport_id, col_sport = sport_cols[0], sport_cols[1]

        reference_date = str(_get_required(cfg, "regles_eligibilite.fenetre_activites.reference_date"))
        if reference_date != "DATE_EXECUTION_PARIS":
            raise ValueError(f"reference_date non supportée : {reference_date}")

        type_fenetre = str(_get_required(cfg, "regles_eligibilite.fenetre_activites.type_fenetre"))
        if type_fenetre != "GLISSANTE_MENSUELLE":
            raise ValueError(f"type_fenetre non supporté : {type_fenetre}")

        fenetre_mois = int(_get_required(cfg, "regles_eligibilite.fenetre_activites.nb_mois"))
        inclure_mois_courant = bool(_get_required(cfg, "regles_eligibilite.fenetre_activites.inclure_mois_courant"))
        nb_lignes = int(_get_required(cfg, "activites.nb_lignes"))
        seed = int(_get_required(cfg, "activites.random_seed"))
        scenario = str(_get_required(cfg, "activites.scenario"))
        facteur_surgeneration = float(_get_required(cfg, "activites.facteur_surgeneration"))

        scenarios_cfg = _get_required(cfg, "activites.scenarios")
        if scenario not in scenarios_cfg:
            raise ValueError(f"Scenario inconnu : {scenario}.")
        scenario_cfg = scenarios_cfg[scenario]

        regles_simulation = _get_required(cfg, "regles_simulation")
        sports_endurance = list(_get_required(regles_simulation, "sports_endurance"))
        sports_non_endurance = list(_get_required(regles_simulation, "sports_non_endurance"))

        salt = os.getenv("P12_PSEUDO_SALT")
        if not salt:
            raise RuntimeError("Variable d'environnement P12_PSEUDO_SALT manquante.")

        rng = random.Random(seed)
        # Référence temporelle unique du run (évite les incohérences si le script franchit minuit)
        now = date_debut_exe

        # -----------------------------------
        # Action 05-c : Chargement Données
        # -----------------------------------
        rh_path = _trouver_fichier_excel(data_raw_dir, ["rh"])
        sport_path = _trouver_fichier_excel(data_raw_dir, ["sport"])
        LOGGER.info("Fichier RH : %s", rh_path)
        LOGGER.info("Fichier Sport : %s", sport_path)
        LOGGER.info("Sortie CSV : %s", out_path)

        df_rh = pd.read_excel(rh_path)
        df_sport = pd.read_excel(sport_path)

        for col in rh_cols:
            if col not in df_rh.columns:
                raise KeyError(f"Colonne manquante dans RH : '{col}'")
        for col in sport_cols:
            if col not in df_sport.columns:
                raise KeyError(f"Colonne manquante dans Sport : '{col}'")

        df_rh = df_rh[rh_cols].copy()
        df_rh[col_id] = df_rh[col_id].astype(str).str.strip()
        df_rh[col_mode] = df_rh[col_mode].astype(str).str.strip()
        df_rh = df_rh.drop_duplicates(subset=[col_id]).reset_index(drop=True)

        df_sport = df_sport[sport_cols].copy()
        df_sport[col_sport_id] = df_sport[col_sport_id].astype(str).str.strip()
        df_sport[col_sport] = df_sport[col_sport].astype(str).str.strip().replace({"nan": ""})
        df_sport = df_sport.drop_duplicates(subset=[col_sport_id]).reset_index(drop=True)
        sport_map = dict(zip(df_sport[col_sport_id], df_sport[col_sport]))

        # -----------------------------------
        # Action 05-d : Préparation Salariés
        # -----------------------------------
        employes: list[Employe] = []
        stats = {"faible": 0, "moyen": 0, "eleve": 0}

        for _, row in df_rh.iterrows():
            id_brut = str(row[col_id]).strip()
            mode = str(row[col_mode]).strip()
            sport_decl = sport_map.get(id_brut)
            if sport_decl is not None and str(sport_decl).strip() == "":
                sport_decl = None

            cle = pseudonymiser_cle_salarie(id_salarie_brut=id_brut, salt=salt)
            profil = _determiner_profil_volume(sport_declare=sport_decl, mod_depl_decl=mode)
            employes.append(
                Employe(
                    id_salarie_brut=id_brut,
                    mod_depl_decl=mode,
                    sport_declare=sport_decl,
                    cle_salarie=cle,
                    profil_volume=profil,
                )
            )
            stats[profil] += 1

        LOGGER.info("Population : %s salariés %s", len(employes), stats)

        # -----------------------------------
        # Action 05-e : Planification
        # -----------------------------------
        mois_fenetre = _mois_dernieres_n_fenetres(now, fenetre_mois, inclure_mois_courant)
        cible_interne = max(nb_lignes, int(round(nb_lignes * facteur_surgeneration)))

        # Jours disponibles par mois : mois courant borné à now.day (pas de dates après aujourd'hui)
        jours_dispo_mois: dict[tuple[int, int], int] = {}
        for (yy, mm) in mois_fenetre:
            dmax = calendar.monthrange(yy, mm)[1]
            if yy == now.year and mm == now.month:
                dmax = now.day
            jours_dispo_mois[(yy, mm)] = int(dmax)

        # --- Fail-fast : incohérence entre mois_fenetre et jours_dispo_mois (évite KeyError)
        missing = [k for k in mois_fenetre if k not in jours_dispo_mois]
        if missing:
            raise RuntimeError(
                "Incohérence planification : certains mois de mois_fenetre ne sont pas présents dans jours_dispo_mois. "
                f"missing={missing} | mois_fenetre={mois_fenetre} | keys={sorted(jours_dispo_mois.keys())}"
            )

        # Log explicite des bornes de fenêtre (debug + traçabilité)
        y0, m0 = mois_fenetre[0]
        y1, m1 = mois_fenetre[-1]
        end_day = int(jours_dispo_mois[(y1, m1)])
        start_fenetre = datetime(y0, m0, 1, tzinfo=tz).date()
        end_fenetre = datetime(y1, m1, end_day, tzinfo=tz).date()
        LOGGER.info(
            "Fenêtre simulation (Europe/Paris) : %s -> %s | nb_mois=%s | inclure_mois_courant=%s",
            start_fenetre,
            end_fenetre,
            fenetre_mois,
            inclure_mois_courant,
        )

        couples_emp_mois: list[tuple[Employe, int, int]] = []
        compteur_emp_mois: dict[tuple[str, int, int], int] = {}

        # --- Planification initiale : capper par (MAX scénario) ET (jours dispo du mois)
        for emp in employes:
            max_scenario_emp = int(scenario_cfg[emp.profil_volume][1])
            for (y, m) in mois_fenetre:
                jours_m = int(jours_dispo_mois[(y, m)])
                cap_mois = min(max_scenario_emp, jours_m)

                if cap_mois <= 0:
                    compteur_emp_mois[(emp.cle_salarie, y, m)] = 0
                    continue

                n_brut = _tirer_nb_activites_par_mois(rng, scenario_cfg, emp.profil_volume)
                n = min(int(n_brut), cap_mois)

                compteur_emp_mois[(emp.cle_salarie, y, m)] = n
                for _ in range(n):
                    couples_emp_mois.append((emp, y, m))

        LOGGER.info("Lignes initiales : %s", len(couples_emp_mois))

        # --- Top-up : uniquement sur des mois où il reste de la capacité (cap_mois)
        if len(couples_emp_mois) < cible_interne:
            LOGGER.info("Top-up en cours...")
            prof_prio = {"eleve": 3, "moyen": 2, "faible": 1}
            employes_tries = sorted(employes, key=lambda e: prof_prio[e.profil_volume], reverse=True)

            tentatives = 0
            while len(couples_emp_mois) < cible_interne:
                tentatives += 1
                if tentatives > 200_000:
                    break

                emp = employes_tries[rng.randint(0, min(len(employes_tries) - 1, 30))]
                y, m = mois_fenetre[rng.randint(0, len(mois_fenetre) - 1)]

                jours_m = int(jours_dispo_mois[(y, m)])
                max_scenario_emp = int(scenario_cfg[emp.profil_volume][1])
                cap_mois = min(max_scenario_emp, jours_m)
                if cap_mois <= 0:
                    continue

                key = (emp.cle_salarie, y, m)
                if compteur_emp_mois.get(key, 0) >= cap_mois:
                    continue

                couples_emp_mois.append((emp, y, m))
                compteur_emp_mois[key] = compteur_emp_mois.get(key, 0) + 1

        # ---------------------------------------------------------------------
        # Micro-check : refuser une configuration impossible (sans déplacer de mois)
        # ---------------------------------------------------------------------
        jours_disponibles_fenetre = sum(jours_dispo_mois.values())
        capacite_globale = len(employes) * jours_disponibles_fenetre
        if nb_lignes > capacite_globale:
            raise RuntimeError(
                f"Configuration impossible : nb_lignes={nb_lignes} > capacité={capacite_globale} "
                f"({len(employes)} salariés × {jours_disponibles_fenetre} jours)."
            )

        # Cap par salarié (tous mois confondus)
        cnt_emp_total = Counter(emp.cle_salarie for (emp, _, _) in couples_emp_mois)
        max_emp_total = max(cnt_emp_total.values(), default=0)
        if max_emp_total > jours_disponibles_fenetre:
            raise RuntimeError(
                f"Configuration impossible : un salarié a {max_emp_total} activités demandées "
                f"sur {jours_disponibles_fenetre} jours disponibles."
            )

        # Cap par salarié et par mois (double sécurité)
        for (cle, y, m), n in compteur_emp_mois.items():
            jours_m = int(jours_dispo_mois[(y, m)])
            if n > jours_m:
                raise RuntimeError(
                    f"Configuration impossible : {cle} a {n} activités sur {y:04d}-{m:02d} "
                    f"mais seulement {jours_m} jours disponibles (mois courant borné à aujourd'hui)."
                )

        # ---------------------------------------------------------------------
        # Pools de jours (sans remplacement) par salarié et par mois
        # ---------------------------------------------------------------------
        jours_par_mois: dict[tuple[int, int], list[str]] = {}
        for (yy, mm) in mois_fenetre:
            dmax = int(jours_dispo_mois[(yy, mm)])
            jours_par_mois[(yy, mm)] = [f"{yy:04d}-{mm:02d}-{d:02d}" for d in range(1, dmax + 1)]

        jours_pool_emp: dict[str, dict[tuple[int, int], list[str]]] = {}
        for e in employes:
            per_month: dict[tuple[int, int], list[str]] = {}
            for k_month, days in jours_par_mois.items():
                tmp = days[:]
                rng.shuffle(tmp)
                per_month[k_month] = tmp
            jours_pool_emp[e.cle_salarie] = per_month

        # -----------------------------------
        # Action 05-f : Génération Activités
        # -----------------------------------
        rows: list[dict[str, Any]] = []
        jours_utilises: set[tuple[str, str]] = set()

        for emp, y, m in couples_emp_mois:
            sport = _tirer_sport_principal(rng, emp.sport_declare, sports_endurance, sports_non_endurance)

            pool_pref = jours_pool_emp[emp.cle_salarie][(y, m)]
            if not pool_pref:
                # Ne devrait pas arriver si caps OK : on refuse plutôt que de déplacer vers un autre mois.
                raise RuntimeError(
                    f"Pool jours épuisé pour {emp.cle_salarie} sur {y:04d}-{m:02d} "
                    f"(config/caps incohérents)."
                )

            jour_str = pool_pref.pop()

            # Unicité (sécurité) + construction à 12:00:00 Europe/Paris
            k = (emp.cle_salarie, jour_str)
            if k in jours_utilises:
                # Ne devrait jamais arriver (pool sans remplacement), mais on sécurise.
                continue
            jours_utilises.add(k)

            yy, mm, dd = map(int, jour_str.split("-"))

            # Construction à 12:00:00 Europe/Paris (unicité au jour).
            # Si exécution avant midi et jour == aujourd'hui, éviter un timestamp "dans le futur".
            hour = 12
            if inclure_mois_courant and (yy == now.year and mm == now.month and dd == now.day) and now.hour < 12:
                hour = 0
            final_dt = datetime(yy, mm, dd, hour, 0, 0, tzinfo=tz)

            duree_sec, distance_m = _generer_duree_distance(
                rng,
                sport in set(sports_endurance),
                emp.mod_depl_decl,
                regles_simulation,
            )

            rows.append(
                {
                    "cle_salarie": emp.cle_salarie,
                    "date_debut": final_dt.isoformat(),
                    "duree_sec": int(duree_sec),
                    "distance_m": distance_m,
                    "type_activite": sport,
                    "commentaire": "",
                    "source_donnee": "csv_simule",
                }
            )

        LOGGER.info("Lignes générées (total) : %s", len(rows))
        if len(rows) < nb_lignes:
            raise RuntimeError(f"Génération insuffisante : {len(rows)} < {nb_lignes}")

        # -----------------------------------
        # Action 05-g : Export CSV
        # -----------------------------------
        indices = list(range(len(rows)))
        rng.shuffle(indices)
        indices = indices[:nb_lignes]
        rows_final = [rows[i] for i in indices]
        lignes_simulees = len(rows_final)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=cols_out, extrasaction="ignore")
            writer.writeheader()
            for r in rows_final:
                writer.writerow(r)

        statut = "SUCCESS"
        log_success(LOGGER, message="CSV simulé écrit.", context={"rows": lignes_simulees})
        return 0

    except Exception as exc:
        statut = "FAILED"
        log_failure(LOGGER, exc=exc, message="Erreur simulation.")
        return 1

    finally:
        # -----------------------------------
        # Action 05-h : Métriques DB
        # -----------------------------------
        try:
            date_fin_exe = datetime.now(tz)
            with psycopg.connect(
                host=(os.getenv("PGHOST") or "localhost").strip(),
                port=int((os.getenv("PGPORT") or "5432").strip()),
                user=(os.getenv("PGUSER") or "postgres").strip(),
                password=(os.getenv("PGPASSWORD") or "postgres").strip(),
                dbname=(os.getenv("PGDATABASE") or "SportDataSolution").strip(),
            ) as conn:
                conn.execute("SET TIME ZONE 'Europe/Paris';")
                write_run_metric(
                    conn=conn,
                    nom_pipeline="mod50_simuler_activites",
                    date_debut_exe=date_debut_exe,
                    date_fin_exe=date_fin_exe,
                    statut=statut,
                    nb_lignes_lues=lignes_simulees,
                    nb_lignes_ecrites=lignes_simulees,
                    nb_anomalies=1 if statut != "SUCCESS" else 0,
                    logger=LOGGER,
                )
                conn.commit()
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(main())
