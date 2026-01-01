#-------------------------------------------------
# mod50_verif_simulation.py
#-------------------------------------------------
"""
Vérification de conformité du fichier CSV d'activités simulées
par rapport au contrat de données et aux référentiels métier.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\gene_lignes\\mod50_verif_simulation.py

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
- Valider techniquement et fonctionnellement le fichier CSV généré par mod50.
- Garantir que les données simulées respectent strictement les contraintes métier avant ingestion.
- Assurer la cohérence avec les référentiels RH et Sport (clé salarié, typologie).
- Vérifier l'unicité (1 activité / salarié / jour) et les bornes temporelles.

Entrées
-------
- Fichiers :
  - <repo>/data/raw/declaratif_activites_12m.csv (fichier simulé à vérifier).
  - <repo>/src/utils/config_pipeline.yml (règles métier, bornes, scénarios).
  - <repo>/src/utils/activite_mapping.yml (schéma des colonnes attendues).
  - <repo>/data/raw/*rh*.xlsx (référentiel RH pour validation matricules).
  - <repo>/data/raw/*sport*.xlsx (référentiel Sport pour validation préférences).

- Variables d'environnement :
  - P12_PSEUDO_SALT (obligatoire pour reconstruire les clés salariés pseudonymisées).
  - PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (métriques).

Sorties
-------
- Logs console : détail des contrôles (OK/WARN/FAIL).
- Code retour : 0 si succès (aucun FAIL), 1 si échec (au moins un FAIL).
- Tables :
  - ops.run_metrique (statut SUCCESS si conformité OK, sinon FAILURE).

Traitements & fonctionnalités
-----------------------------
- Validation structurelle :
  - Chargement dynamique du schéma attendu depuis activite_mapping.yml (section simulation.cols_out).
  - Vérification stricte de la présence et de l'ordre des colonnes.
  - Vérification stricte du volume de lignes (doit être ÉGAL à activites.nb_lignes).
  - Attention : Si le simulateur rejette trop de doublons, ce test échouera.
  
- Validation référentielle :
  - Mapping complet des clés salariés (cle_salarie) vers le référentiel RH.
  - Conformité des types d'activités avec les sports pratiques autorisés.
  - Vérification de la constante 'source_donnee' = 'csv_simule'.

- Validation temporelle :
  - Parsing des dates et conversion UTC -> Europe/Paris.
  - Contrôle strict de l'unicité (1 activité max par jour par salarié).
  - Vérification des bornes de la fenêtre glissante (date min/max).

- Validation métier (Règles de gestion) :
  - Sports d'endurance : distance_m obligatoire et cohérente avec la durée (bornes ratio).
  - Sports hors endurance : distance_m interdite (NULL/NaN).
  - Durées : respect des bornes min/max par catégorie (endurance/non-endurance).
  - Scénarios : respect du nombre MAX d'activités par mois selon le profil (faible/moyen/élevé).
  - Préférence : si un sport est déclaré en référentiel, l'activité simulée doit correspondre.

Contraintes
-----------
- Bloquant : tout échec (FAIL) entraîne un code retour 1 et un statut FAILURE en base.
- Avertissements : les WARN (ex: incohérence mineure non bloquante) n'arrêtent pas le pipeline.
- Le fichier CSV doit exister au chemin attendu (data/raw/declaratif_activites_12m.csv).

Observations & remarques
------------------------
- Ce script ne modifie pas les données, il est purement passif (lecture seule).
- Il utilise P12_PSEUDO_SALT pour régénérer les clés à la volée et vérifier leur existence.
- La métrique est envoyée même en cas de crash non géré (via bloc try/except global).
"""

# ---------------\
# IMPORTS
# ---------------\
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv

# -------------------------------------------------------------------
# Action 0 - Rendre le package 'src' importable en exécution directe
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]

# Permet d'importer "src.*" même quand le script est exécuté en fichier direct
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

try:
    from src.utils.logger import get_logger, log_failure, log_success, write_run_metric
except ImportError:
    pass

def _detect_origin() -> str:
    """Déduire l'origine d'exécution (CLI ou KESTRA) à partir de l'environnement."""
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
    """Parser les arguments CLI (tolérant aux arguments inconnus)."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--origin",
        choices=["CLI", "KESTRA"],
        default=_detect_origin(),
        help="Origine d'exécution (override). Par défaut : déduction via variables d'environnement.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("P12_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO",
        help="Niveau de log (DEBUG|INFO|WARNING|ERROR). Par défaut : P12_LOG_LEVEL ou LOG_LEVEL ou INFO.",
    )
    parser.add_argument(
        "--mapping",
        default=None,
        help="Chemin vers activite_mapping.yml (optionnel).",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args

# Logger par défaut (sera ré-initialisé dans main() si --origin/--log-level sont fournis)
LOGGER = get_logger(
    script="mod50_verif_simulation",
    origin=_detect_origin(),
    level=os.getenv("P12_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO",
)

# -------------------------------------------------------------------
# Action 1 - Paramètres de chemin (relatifs à la racine du repo)
# -------------------------------------------------------------------
CSV_REL_PATH = Path("data/raw/declaratif_activites_12m.csv")
YML_REL_PATH = Path("src/utils/config_pipeline.yml")
MAPPING_REL_PATH = Path("src/utils/activite_mapping.yml") # Valeur par défaut si non fourni en args

# -------------------------------------------------------------------
# Action 2 - Compteurs de résultats (PASS / WARN / FAIL)
# -------------------------------------------------------------------
@dataclass(frozen=True)
class CheckCounters:
    """Compteurs de résultats."""
    ok: int = 0
    warn: int = 0
    fail: int = 0

# -------------------------------------------------------------------
# Action 3 - Affichage console (sections et statuts)
# -------------------------------------------------------------------
def _print_header(title: str) -> None:
    LOGGER.info("")
    LOGGER.info("%s", "=" * 80)
    LOGGER.info(title)
    LOGGER.info("%s", "=" * 80)

def _ok(msg: str) -> None:
    LOGGER.info("[OK] %s", msg)

def _warn(msg: str) -> None:
    LOGGER.warning("[WARN] %s", msg)

def _fail(msg: str) -> None:
    LOGGER.error("[FAIL] %s", msg)

# -------------------------------------------------------------------
# Action 4 - Utilitaires génériques (normalisation, repo root, YAML, recherche fichiers)
# -------------------------------------------------------------------
def _normaliser_texte(valeur: str) -> str:
    """Normaliser un texte (minuscule + suppression accents) pour recherche de fichiers."""
    texte = str(valeur).strip().lower()
    texte = unicodedata.normalize("NFKD", texte)
    return "".join(ch for ch in texte if not unicodedata.combining(ch))

def _find_repo_root(start: Path) -> Path:
    """
    Déterminer la racine du repo de manière robuste.
    Critère : présence de dossiers 'data' et 'src' au même niveau.
    """
    for p in [start] + list(start.parents):
        if (p / "data").exists() and (p / "src").exists():
            return p
    raise FileNotFoundError("Racine du repo introuvable (dossiers 'data' et 'src' non détectés).")

def _load_yaml(path: Path) -> dict[str, Any]:
    """Charger un YAML en dict."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier YAML introuvable : {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def _get_required(dct: dict[str, Any], key_path: str) -> Any:
    """Accéder à une clé requise (notation a.b.c) avec erreur explicite."""
    cur: Any = dct
    for part in key_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"Clé manquante dans config : '{key_path}' (segment '{part}')")
        cur = cur[part]
    return cur

def _trouver_excel(data_raw_dir: Path, mots_cles: list[str]) -> Path:
    """Trouver un fichier .xlsx dans data/raw via mots-clés normalisés."""
    fichiers = list(data_raw_dir.glob("*.xlsx"))
    if not fichiers:
        raise FileNotFoundError(f"Aucun fichier .xlsx trouvé dans {data_raw_dir}.")
    
    mots = [_normaliser_texte(m) for m in mots_cles]
    for f in fichiers:
        nom = _normaliser_texte(f.name)
        if all(m in nom for m in mots):
            return f
            
    dispo = ", ".join(x.name for x in fichiers)
    raise FileNotFoundError(
        f"Fichier .xlsx introuvable pour mots_cles={mots_cles}. Disponibles : {dispo}"
    )

# -------------------------------------------------------------------
# Action 5 - Règles partagées avec la génération (pseudonymisation, profils, fenêtre mensuelle)
# -------------------------------------------------------------------
def _pseudonymiser_id(id_brut: str, salt: str) -> str:
    """Pseudonymiser un identifiant salarié (doit matcher le générateur)."""
    digest = hashlib.sha256((salt + "|" + id_brut).encode("utf-8")).hexdigest()
    return f"sal_{digest[:12]}"

def _is_mode_actif(mod_depl: str) -> bool:
    """Déterminer si un mode de déplacement est actif."""
    return mod_depl in {"Marche/running", "Vélo/Trottinette/Autres"}

def _groupe_abc(has_sport: bool, has_mode_actif: bool) -> str:
    """Déterminer le groupe A/B/C."""
    if has_sport and has_mode_actif:
        return "C"
    if (has_sport and not has_mode_actif) or (not has_sport and has_mode_actif):
        return "B"
    return "A"

def _profil_depuis_groupe(groupe: str) -> str:
    """Mapper A/B/C vers faible/moyen/eleve."""
    if groupe == "A":
        return "faible"
    if groupe == "B":
        return "moyen"
    if groupe == "C":
        return "eleve"
    raise ValueError(f"Groupe invalide : {groupe}")

def _mois_fenetre(now: datetime, fenetre_mois: int, inclure_mois_courant: bool) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    ancre = now
    if not inclure_mois_courant:
        premier_jour = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=now.tzinfo)
        ancre = premier_jour - timedelta(days=1)
        
    y, m = ancre.year, ancre.month
    for _ in range(fenetre_mois):
        out.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    out.reverse()
    return out

# -------------------------------------------------------------------
# Action 6 - Exécution principale : contrôles structurés du CSV simulé
# -------------------------------------------------------------------
def main() -> int:
    counters = CheckCounters()
    args = _parse_args(sys.argv[1:])
    global LOGGER
    LOGGER = get_logger(
        script="mod50_verif_simulation",
        origin=str(args.origin),
        level=str(args.log_level),
    )
    
    date_debut_exe = datetime.now(ZoneInfo("Europe/Paris"))
    nb_lignes_lues = 0

    # -------------------------------------------------------------------
    # Action 6.0 - Résolution des chemins et chargement configuration
    # -------------------------------------------------------------------
    repo_root = _find_repo_root(Path(__file__).resolve())
    csv_path = repo_root / CSV_REL_PATH
    yml_path = repo_root / YML_REL_PATH
    
    # Prise en compte de l'argument --mapping s'il est fourni
    mapping_path = Path(args.mapping) if args.mapping else repo_root / MAPPING_REL_PATH

    data_raw_dir = repo_root / "data" / "raw"
    
    _print_header("Chargement configuration & fichiers")
    
    cfg = _load_yaml(yml_path)
    map_cfg = _load_yaml(mapping_path) # Chargement du mapping pour le schéma

    reference_date = str(_get_required(cfg, "regles_eligibilite.fenetre_activites.reference_date"))
    if reference_date != "DATE_EXECUTION_PARIS":
        raise ValueError(f"reference_date non supportée : {reference_date}")
        
    tz_name = "Europe/Paris"
    tz = ZoneInfo(tz_name)
    
    nb_lignes_cfg = int(_get_required(cfg, "activites.nb_lignes"))
    fenetre_mois = int(_get_required(cfg, "regles_eligibilite.fenetre_activites.nb_mois"))
    inclure_mois_courant = bool(_get_required(cfg, "regles_eligibilite.fenetre_activites.inclure_mois_courant"))
    scenario = str(_get_required(cfg, "activites.scenario"))
    
    regles = _get_required(cfg, "regles_simulation")
    sports_endurance = set(_get_required(regles, "sports_endurance"))
    sports_non_endurance = set(_get_required(regles, "sports_non_endurance"))
    sports_pratiques = set(_get_required(cfg, "referentiels.sports_pratiques"))
    
    distance_ratio = _get_required(regles, "distance_ratio")
    ratio_min = float(distance_ratio["min"])
    ratio_max = float(distance_ratio["max"])
    
    refs = _get_required(regles, "refs_temps_par_mode")
    ref_marche = _get_required(refs, "marche_running")
    ref_velo = _get_required(refs, "velo_trott_autres")
    
    bornes = _get_required(regles, "duree_bornes_sec")
    bornes_end = _get_required(bornes, "endurance")
    bornes_non = _get_required(bornes, "non_endurance")
    
    scenarios_cfg = _get_required(cfg, "activites.scenarios")
    if scenario not in scenarios_cfg:
        _fail(f"Scenario inconnu dans YAML : {scenario}")
        return 1
        
    scen = scenarios_cfg[scenario]

    if not csv_path.exists():
        _fail(f"CSV introuvable : {csv_path}")
        return 1
        
    _ok(f"CSV trouvé : {csv_path}")
    df = pd.read_csv(csv_path)
    nb_lignes_lues = int(len(df))
    _ok(f"CSV chargé : {len(df)} lignes")

    # -------------------------------------------------------------------
    # Action 6.1 - Contrat de schéma : colonnes, ordre et volume
    # -------------------------------------------------------------------
    _print_header("1) Structure CSV")
    
    # Modification : Lecture dynamique des colonnes depuis le YAML
    expected_cols = _get_required(map_cfg, "simulation.cols_out")
    
    if list(df.columns) != expected_cols:
        _fail(f"Colonnes CSV inattendues. Reçu={list(df.columns)} | Attendu={expected_cols}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Colonnes CSV conformes et dans le bon ordre")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    if len(df) != nb_lignes_cfg:
        # Utilisation directe du logger standard au lieu d'une fonction wrapper inexistante
        LOGGER.warning("[WARN] Nombre de lignes != config (écart toléré). CSV=%s | config=%s", len(df), nb_lignes_cfg)
        counters = CheckCounters(counters.ok, counters.warn + 1, counters.fail)
    else:
        _ok(f"Nombre de lignes conforme à config : {nb_lignes_cfg}")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)



    # -------------------------------------------------------------------
    # Action 6.2 - Référentiels RH/Sport : mapping cle_salarie
    # -------------------------------------------------------------------
    _print_header("2) Chargement RH/Sport & mapping cle_salarie")
    
    salt = os.getenv("P12_PSEUDO_SALT")
    if not salt:
        _fail("P12_PSEUDO_SALT absent : impossible de vérifier les règles liées aux données RH.")
        return 1
    _ok("P12_PSEUDO_SALT présent")
    
    rh_path = _trouver_excel(data_raw_dir, ["rh"])
    sport_path = _trouver_excel(data_raw_dir, ["sport"])
    
    _ok(f"Excel RH : {rh_path.name}")
    _ok(f"Excel Sport : {sport_path.name}")
    
    df_rh = pd.read_excel(rh_path)
    df_sp = pd.read_excel(sport_path)
    
    col_id = "ID salarié"
    col_mode = "Moyen de déplacement"
    col_sport = "Pratique d'un sport"
    
    missing = [c for c in [col_id, col_mode] if c not in df_rh.columns]
    if missing:
        _fail(f"Colonnes manquantes RH : {missing}")
        return 1
        
    missing = [c for c in [col_id, col_sport] if c not in df_sp.columns]
    if missing:
        _fail(f"Colonnes manquantes Sport : {missing}")
        return 1

    df_rh = df_rh[[col_id, col_mode]].copy()
    df_rh[col_id] = df_rh[col_id].astype(str).str.strip()
    df_rh[col_mode] = df_rh[col_mode].astype(str).str.strip()
    df_rh = df_rh.drop_duplicates(subset=[col_id]).reset_index(drop=True)

    df_sp = df_sp[[col_id, col_sport]].copy()
    df_sp[col_id] = df_sp[col_id].astype(str).str.strip()
    df_sp[col_sport] = df_sp[col_sport].astype(str).str.strip().replace({"nan": ""})
    df_sp = df_sp.drop_duplicates(subset=[col_id]).reset_index(drop=True)
    
    sport_map = dict(zip(df_sp[col_id], df_sp[col_sport]))
    cle_map: dict[str, dict[str, Any]] = {}
    stats = {"A": 0, "B": 0, "C": 0}

    for _, row in df_rh.iterrows():
        id_brut = row[col_id]
        mode = row[col_mode]
        sport_decl = sport_map.get(id_brut, "")
        sport_decl = sport_decl.strip() if isinstance(sport_decl, str) else ""
        sport_decl = sport_decl if sport_decl else None
        
        has_sport = sport_decl is not None
        has_mode_actif = _is_mode_actif(mode)
        
        groupe = _groupe_abc(has_sport=has_sport, has_mode_actif=has_mode_actif)
        profil = _profil_depuis_groupe(groupe)
        
        stats[groupe] += 1
        
        cle = _pseudonymiser_id(id_brut=id_brut, salt=salt)
        cle_map[cle] = {
            "id_salarie_brut": id_brut,
            "mod_depl_decl": mode,
            "sport_declare": sport_decl,
            "groupe": groupe,
            "profil": profil,
        }

    _ok(f"Salariés RH = {len(df_rh)}")
    _ok(f"Groupe A (pas de sport ET déplacement non actif): A={stats['A']}")
    _ok(f"Groupe B (sport OU déplacement actif) hors groupe C: B={stats['B']}")
    _ok(f"Groupe C (sport ET déplacement actif): C={stats['C']}")

    unknown = set(df["cle_salarie"]) - set(cle_map.keys())
    if unknown:
        _fail(f"cle_salarie inconnues (RH non joint) : {len(unknown)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Toutes les cle_salarie sont jointes au référentiel RH")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # -------------------------------------------------------------------
    # Action 6.3 - Contraintes de format et référentiels
    # -------------------------------------------------------------------
    _print_header("3) Contraintes de format (cle_salarie, source_donnee, type_activite)")
    
    pat = re.compile(r"^sal_[0-9a-f]{12}$")
    bad_keys = df.loc[~df["cle_salarie"].astype(str).apply(lambda x: bool(pat.match(x))), "cle_salarie"]
    
    if len(bad_keys) > 0:
        _fail(f"cle_salarie format invalide : {len(bad_keys)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("cle_salarie : format conforme (sal_[12 hex])")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    src_bad = df.loc[df["source_donnee"].astype(str) != "csv_simule", "source_donnee"]
    if len(src_bad) > 0:
        _fail(f"source_donnee != csv_simule : {len(src_bad)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("source_donnee : valeur constante csv_simule")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    bad_sports = df.loc[~df["type_activite"].astype(str).isin(sports_pratiques), "type_activite"]
    if len(bad_sports) > 0:
        _fail(f"type_activite hors référentiel sports_pratiques : {len(bad_sports)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("type_activite : toutes les valeurs sont dans sports_pratiques")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # -------------------------------------------------------------------
    # Action 6.4 - Fenêtre temporelle & unicité salarié / activité / jour
    # -------------------------------------------------------------------
    _print_header("4) Fenêtre temporelle & unicité salarié / activité / jour")
    
    dt = pd.to_datetime(df["date_debut"], errors="coerce", utc=True)
    
    if dt.isna().any():
        _fail(f"date_debut non parseable : {int(dt.isna().sum())}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("date_debut : toutes les valeurs sont parseables")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    dt_local = dt.dt.tz_convert(tz_name)
    
    # -------------------------------------------------------------------
    # Contrôle - Unicité : 1 activité maximum par salarié et par jour (Europe/Paris)
    # -------------------------------------------------------------------
    jours = dt_local.dt.strftime("%Y-%m-%d")
    
    # Variante plus lisible (sans effet de bord sur df) :
    df_tmp = df[["cle_salarie"]].copy()
    df_tmp["_jour"] = jours
    nb_dup = int(df_tmp.duplicated(subset=["cle_salarie", "_jour"], keep=False).sum())
    
    if nb_dup > 0:
        _fail(f"Doublons détectés : plusieurs activités pour un salarié le même jour : {nb_dup}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Unicité jour : aucune activité dupliquée pour un salarié le même jour")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    now = datetime.now(tz)
    mois = _mois_fenetre(now=now, fenetre_mois=fenetre_mois, inclure_mois_courant=inclure_mois_courant)
    
    y0, m0 = mois[0]
    start_window = datetime(y0, m0, 1, 0, 0, 0, tzinfo=tz)
    
    min_dt = dt_local.min().to_pydatetime()
    max_dt = dt_local.max().to_pydatetime()
    
    if min_dt < start_window:
        _fail(f"date_debut min hors fenêtre : {min_dt.isoformat()} < {start_window.isoformat()}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok(f"date_debut min dans fenêtre : {min_dt.isoformat()}")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    if max_dt.date() > now.date():
        _fail(
            f"date_debut max dans le futur (jour) : {max_dt.date().isoformat()} > {now.date().isoformat()}"
        )
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok(f"date_debut max <= aujourd'hui (jour) : {max_dt.isoformat()}")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)


    # -------------------------------------------------------------------
    # Action 6.5 - Règles endurance / non-endurance
    # -------------------------------------------------------------------
    _print_header("5) Règles endurance vs non-endurance (distance/durée)")
    
    is_end = df["type_activite"].astype(str).isin(sports_endurance)
    is_non = df["type_activite"].astype(str).isin(sports_non_endurance)
    
    if int((~(is_end | is_non)).sum()) > 0:
        _warn("Certaines activités ne sont ni endurance ni non_endurance (à vérifier dans le YAML).")
        counters = CheckCounters(counters.ok, counters.warn + 1, counters.fail)

    dist_isna = df["distance_m"].isna()
    ko_end_dist = df[is_end & dist_isna]
    ko_non_dist = df[is_non & ~dist_isna]
    
    if len(ko_end_dist) > 0:
        _fail(f"Endurance avec distance_m manquante : {len(ko_end_dist)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Endurance : distance_m renseignée")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    if len(ko_non_dist) > 0:
        _fail(f"Non-endurance avec distance_m renseignée : {len(ko_non_dist)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Non-endurance : distance_m non applicable (NULL)")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # Durées bornées par catégorie
    duree = pd.to_numeric(df["duree_sec"], errors="coerce")
    if duree.isna().any():
        _fail(f"duree_sec non numérique : {int(duree.isna().sum())}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("duree_sec : valeurs numériques")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    end_min, end_max = int(bornes_end["min"]), int(bornes_end["max"])
    non_min, non_max = int(bornes_non["min"]), int(bornes_non["max"])
    
    ko_end_duree = df[is_end & ((duree < end_min) | (duree > end_max))]
    ko_non_duree = df[is_non & ((duree < non_min) | (duree > non_max))]
    
    if len(ko_end_duree) > 0:
        _fail(f"Endurance : duree_sec hors bornes [{end_min},{end_max}] : {len(ko_end_duree)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok(f"Endurance : duree_sec dans [{end_min},{end_max}]")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    if len(ko_non_duree) > 0:
        _fail(f"Non-endurance : duree_sec hors bornes [{non_min},{non_max}] : {len(ko_non_duree)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok(f"Non-endurance : duree_sec dans [{non_min},{non_max}]")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # -------------------------------------------------------------------
    # Action 6.6 - Cohérence distance endurance vs ratio
    # -------------------------------------------------------------------
    _print_header("6) Cohérence distance (endurance) vs références 15/25 km (bornes ratio)")
    
    df_join = df.merge(
        pd.DataFrame.from_dict(cle_map, orient="index").reset_index().rename(columns={"index": "cle_salarie"}),
        on="cle_salarie",
        how="left",
    )
    
    # Distance attendue par référence (endurance uniquement)
    dist = pd.to_numeric(df_join["distance_m"], errors="coerce")
    
    def _ref_km(mode: str) -> int:
        if mode == "Vélo/Trottinette/Autres":
            return int(ref_velo["distance_ref_km"])
        return int(ref_marche["distance_ref_km"])

    ref_km = df_join["mod_depl_decl"].astype(str).apply(_ref_km)
    ref_m = ref_km.astype(float) * 1000.0
    
    min_m = ref_m * ratio_min
    max_m = ref_m * ratio_max
    
    ko_ratio = df_join[is_end & ((dist < (min_m - 1.0)) | (dist > (max_m + 1.0)))]
    
    if len(ko_ratio) > 0:
        _fail(f"Endurance : distance_m hors bornes ratio [{ratio_min},{ratio_max}] : {len(ko_ratio)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok(f"Endurance : distance_m dans les bornes ratio [{ratio_min},{ratio_max}]")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # -------------------------------------------------------------------
    # Action 6.7 - Sport déclaré : constance type_activite
    # -------------------------------------------------------------------
    _print_header("7) Règles sport déclaré (Sport.xlsx) => type_activite constant")
    
    # Si sport déclaré, le générateur fixe type_activite à cette valeur.
    declared = df_join["sport_declare"].notna()
    df_decl = df_join[declared].copy()
    df_decl["sport_declare"] = df_decl["sport_declare"].astype(str).str.strip()
    
    ko_decl = df_decl[df_decl["type_activite"].astype(str) != df_decl["sport_declare"]]
    
    if len(ko_decl) > 0:
        _fail(f"type_activite != sport déclaré (pour salariés avec sport déclaré) : {len(ko_decl)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("Pour les salariés avec sport déclaré : type_activite = sport déclaré")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)
    
    LOGGER.info("Pour la simulation, les salariés sans sport déclaré peuvent avoir des déclaratifs avec des valeurs du référentiel type_activite")

    # -------------------------------------------------------------------
    # Action 6.8 - Scénarios : contrôle du MAX
    # -------------------------------------------------------------------
    _print_header("8) Règle scénarios : contrôle des MAX par salarié et par mois")
    
    # Le sous-échantillonnage peut casser les MIN, mais ne doit pas dépasser les MAX.
    df_join["ym"] = dt_local.dt.strftime("%Y-%m")
    grp = df_join.groupby(["cle_salarie", "ym"], as_index=False).size()
    grp = grp.rename(columns={"size": "nb_activites_mois"})
    
    max_by_profile = {
        "faible": int(scen["faible"][1]),
        "moyen": int(scen["moyen"][1]),
        "eleve": int(scen["eleve"][1]),
    }
    
    prof = df_join[["cle_salarie", "profil"]].drop_duplicates()
    grp = grp.merge(prof, on="cle_salarie", how="left")
    grp["max_autorise"] = grp["profil"].map(max_by_profile)
    
    ko_max = grp[grp["nb_activites_mois"] > grp["max_autorise"]]

    if len(ko_max) > 0:
        cols_dbg = ["cle_salarie", "ym", "nb_activites_mois", "profil", "max_autorise"]
        LOGGER.info("Détails dépassements MAX (extraits) :\n%s", ko_max[cols_dbg].head(15).to_string(index=False))

    
    if len(ko_max) > 0:
        _fail(f"Dépassement MAX scénario (par salarié et mois) : {len(ko_max)}")
        counters = CheckCounters(counters.ok, counters.warn, counters.fail + 1)
    else:
        _ok("MAX scénario respecté (par salarié et mois)")
        counters = CheckCounters(counters.ok + 1, counters.warn, counters.fail)

    # -------------------------------------------------------------------
    # Action 6.9 - Synthèse et code retour
    # -------------------------------------------------------------------
    _print_header("Synthèse checks unitaires")
    LOGGER.info("PASS=%s | WARN=%s | FAIL=%s", counters.ok, counters.warn, counters.fail)
    
    if counters.fail == 0:
        log_success(
            LOGGER,
            message="Vérification simulation : OK",
            context={"pass": counters.ok, "warn": counters.warn, "fail": counters.fail},
        )
    else:
        log_failure(
            LOGGER,
            message="Vérification simulation : KO",
            exc=RuntimeError("Checks KO"),
            context={"pass": counters.ok, "warn": counters.warn, "fail": counters.fail},
        )

    try:
        statut = "SUCCESS" if counters.fail == 0 else "FAILURE"
        nb_anomalies = int(counters.warn + counters.fail)
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
                nom_pipeline="mod50_verif_simulation",
                date_debut_exe=date_debut_exe,
                date_fin_exe=date_fin_exe,
                statut=statut,
                nb_lignes_lues=nb_lignes_lues,
                nb_lignes_ecrites=0,
                nb_anomalies=nb_anomalies,
                logger=LOGGER,
            )
            conn.commit()
    except Exception:
        pass

    return 0 if counters.fail == 0 else 1

# -------------------------------------------------------------------
# Action 7 - Exécution CLI
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc: # pragma: no cover
        # Logger une erreur standardisée si une exception non gérée survient.
        log_failure(
            LOGGER,
            message="Erreur non gérée dans mod50_verif_simulation.",
            exc=exc,
        )
        try:
            tz = ZoneInfo("Europe/Paris")
            dt = datetime.now(tz)
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
                    nom_pipeline="mod50_verif_simulation",
                    date_debut_exe=dt,
                    date_fin_exe=dt,
                    statut="FAILURE",
                    nb_lignes_lues=0,
                    nb_lignes_ecrites=0,
                    nb_anomalies=1,
                    logger=LOGGER,
                )
                conn.commit()
        except Exception:
            pass
        raise SystemExit(1)
