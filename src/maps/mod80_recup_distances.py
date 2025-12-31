#-------------------------------------------------
# mod80_recup_distances.py
#-------------------------------------------------

"""
Récupération des distances domicile -> entreprise via Google Maps (Directions API)
et chargement dans la table metier.ctrl_trajet.

Commande d'exécution
--------------------
python src/maps/mod80_recup_distances.py

Arguments
---------
--origin : origine d'exécution (CLI|KESTRA).
          Défaut : détection automatique via variables d'environnement.
--log-level : niveau de logs (DEBUG|INFO|WARNING|ERROR).
             Défaut : P12_LOG_LEVEL sinon INFO.

Objectifs
---------
- Calculer la distance réelle (en mètres) et la durée (en secondes) entre le domicile
  de chaque salarié et l'entreprise (1362 Av. des Platanes, 34970 Lattes).
- Détecter les incohérences de déclaration (ex: "Marche" pour > 15 km).
- Alimenter la table de contrôle `metier.ctrl_trajet`.

Entrées
-------
- Tables :
  - sec.lien_salarie (clé unique des salariés actifs).
  - sec.rh_salarie (adresses domiciles et modes de déplacement déclarés).
- Variables d'environnement :
  - GOOGLE_MAPS_API_KEY (obligatoire).
  - PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.
  - FORCE_GMAPS ("1" pour forcer le recalcul complet, "0" pour ignorer si déjà calculé).

Sorties
-------
- Tables :
  - metier.ctrl_trajet (TRUNCATE + INSERT des résultats).
  - ops.run_metrique (statut SUCCESS/SKIP/FAILED).
- Logs console : détail des appels API et des anomalies.

Traitements & fonctionnalités
-----------------------------
- Skip intelligent : si le nombre de lignes dans `metier.ctrl_trajet` correspond
  au nombre de salariés, le script s'arrête (statut SKIP) pour économiser l'API.
- Forçage : si FORCE_GMAPS=1, le recalcul est intégral (coût API facturé).
- Mapping API : conversion du mode de transport RH (ex: "Marche/running") vers
  le mode Google API ("walking", "bicycling", "driving", "transit").
- Contrôle de cohérence : flag `est_incoherent` = true si la distance dépasse les seuils
  métier (15km pour marche, 25km pour vélo).
- Résilience : en cas d'erreur API sur un salarié, l'anomalie est loggée et comptée,
  mais le traitement continue pour les autres.

Contraintes
-----------
- Coût API : chaque exécution complète (environ 160 appels) consomme du crédit Google Cloud.
- Géocodage : les adresses doivent être suffisamment précises pour Google Maps.
- Latence : traitement séquentiel synchrone (délai réseau API).

Observations & remarques
------------------------
- Le script utilise `requests` pour appeler l'API Google Maps Directions.
- La connexion DB est gérée via `psycopg` (v3).
- Les métriques sont enregistrées même en cas d'échec global (bloc finally/try-except).
"""

# ---------------\
# IMPORTS
# ---------------\
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg
import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    # Utilitaire projet (stdout + format stable)
    from src.utils.logger import get_logger, log_failure, log_success
    from src.utils.logger import write_run_metric as write_run_metric_project
except ImportError:
    get_logger = None # type: ignore[assignment]
    log_failure = None # type: ignore[assignment]
    log_success = None # type: ignore[assignment]
    write_run_metric_project = None # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SCRIPT_NAME = "mod80_recup_distances"
NOM_PIPELINE = "mod80_recup_distances"
ENTREPRISE_ADRESSE = "1362 Av. des Platanes, 34970 Lattes"
TZ_PARIS = ZoneInfo("Europe/Paris")

# ---------------------------------------------------------------------------
# Utilitaires généraux
# ---------------------------------------------------------------------------
def _add_project_root_to_syspath() -> None:
    """
    Ajouter la racine du projet au sys.path.
    But : permettre l'exécution en CLI (python -m ...) et en conteneur
    même si le CWD n'est pas la racine du repo.
    """
    current = Path(__file__).resolve()
    # __file__ = .../src/maps/mod80_recup_distances.py -> racine projet = parent de "src"
    project_root = current.parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

def _find_repo_root(start: Path) -> Path:
    """
    Déduire la racine du repo via une heuristique simple.
    """
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "src").is_dir() and (parent / "data").exists():
            return parent
    return start.resolve()

def _ensure_logger_imports() -> None:
    """
    Assurer l'import de src.utils.logger après ajout de la racine projet au sys.path.
    Ne modifie pas les traitements : uniquement la robustesse des imports de logging/metrics.
    """
    global get_logger, log_failure, log_success, write_run_metric_project
    if get_logger is not None and write_run_metric_project is not None:
        return

    try:
        from src.utils.logger import get_logger as _get_logger # noqa: WPS433
        from src.utils.logger import log_failure as _log_failure # noqa: WPS433
        from src.utils.logger import log_success as _log_success # noqa: WPS433
        from src.utils.logger import write_run_metric as _write_run_metric_project # noqa: WPS433
        get_logger = _get_logger
        log_failure = _log_failure
        log_success = _log_success
        write_run_metric_project = _write_run_metric_project
    except Exception:
        # Rester sur le fallback si l'import échoue
        pass

def _detect_origin() -> str:
    """
    Déduire une origine d'exécution.
    - "KESTRA" si des variables Kestra usuelles sont présentes
    - sinon "CLI"
    """
    for key in (
        "KESTRA_FLOW_ID",
        "KESTRA_EXECUTION_ID",
        "KESTRA_NAMESPACE",
        "KESTRA_TASKRUN_ID",
    ):
        if os.getenv(key):
            return "KESTRA"
    return "CLI"

def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parser les arguments CLI.
    - --origin : 'CLI' ou 'KESTRA'. Par défaut : détection automatique (_detect_origin()).
    - --log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Par défaut : P12_LOG_LEVEL sinon 'INFO'.
    Note : parse_known_args() ignore les arguments inconnus.
    """
    parser = argparse.ArgumentParser(prog="mod80_recup_distances")
    parser.add_argument(
        "--origin",
        default=_detect_origin(),
        help="Origine d'exécution (CLI ou KESTRA).",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("P12_LOG_LEVEL", "INFO"),
        help="Niveau de logs (DEBUG, INFO, WARNING, ERROR).",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args

def _parse_bool_env(name: str, default: str = "0") -> bool:
    """
    Parser une variable "0/1" en bool.
    Toute valeur différente de "1" est considérée comme False.
    """
    return os.getenv(name, default).strip() == "1"

# ---------------------------------------------------------------------------
# Configuration DB
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DbConfig:
    """Configuration de connexion PostgreSQL (variables d'environnement)."""
    host: str
    port: int
    user: str
    password: str
    database: str

    @staticmethod
    def from_env() -> "DbConfig":
        """Construire la configuration PostgreSQL à partir de l'environnement."""
        host = os.getenv("PGHOST", "localhost").strip()
        port_str = os.getenv("PGPORT", "5432").strip()
        user = os.getenv("PGUSER", "").strip()
        password = os.getenv("PGPASSWORD", "").strip()
        database = os.getenv("PGDATABASE", "").strip()

        if not user or not password or not database:
            raise ValueError(
                "Variables manquantes : PGUSER / PGPASSWORD / PGDATABASE doivent être définies."
            )

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"PGPORT invalide : {port_str}") from exc
        
        return DbConfig(host=host, port=port, user=user, password=password, database=database)

def connect_db(cfg: DbConfig) -> psycopg.Connection:
    """Ouvrir une connexion PostgreSQL."""
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        dbname=cfg.database,
        autocommit=False,
    )

# ---------------------------------------------------------------------------
# Monitoring - ops.run_metrique
# ---------------------------------------------------------------------------
def _write_run_metric_fallback(
    conn: psycopg.Connection,
    nom_pipeline: str,
    date_debut_exe: datetime,
    date_fin_exe: datetime,
    statut: str,
    nb_lignes_lues: int,
    nb_lignes_ecrites: int,
    nb_anomalies: int,
) -> None:
    """
    Insérer une métrique d'exécution dans ops.run_metrique.
    Note : fallback local si src.utils.logger.write_run_metric n'est pas importable.
    """
    query = """
    INSERT INTO ops.run_metrique (
        nom_pipeline, date_debut_exe, date_fin_exe, statut,
        nb_lignes_lues, nb_lignes_ecrites, nb_anomalies, origine
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'CLI');
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                nom_pipeline,
                date_debut_exe,
                date_fin_exe,
                statut,
                nb_lignes_lues,
                nb_lignes_ecrites,
                nb_anomalies,
            ),
        )

def write_run_metric(
    conn: psycopg.Connection,
    nom_pipeline: str,
    date_debut_exe: datetime,
    date_fin_exe: datetime,
    statut: str,
    nb_lignes_lues: int,
    nb_lignes_ecrites: int,
    nb_anomalies: int,
    logger: Optional[object] = None,
) -> None:
    """
    Écrire une métrique d'exécution dans ops.run_metrique.
    - Utilise src.utils.logger.write_run_metric si disponible.
    - Sinon, utilise le fallback local (insertion SQL identique).
    """
    if write_run_metric_project is not None:
        write_run_metric_project(
            conn=conn,
            nom_pipeline=nom_pipeline,
            date_debut_exe=date_debut_exe,
            date_fin_exe=date_fin_exe,
            statut=statut,
            nb_lignes_lues=nb_lignes_lues,
            nb_lignes_ecrites=nb_lignes_ecrites,
            nb_anomalies=nb_anomalies,
            logger=logger,
        )
        return

    _write_run_metric_fallback(
        conn=conn,
        nom_pipeline=nom_pipeline,
        date_debut_exe=date_debut_exe,
        date_fin_exe=date_fin_exe,
        statut=statut,
        nb_lignes_lues=nb_lignes_lues,
        nb_lignes_ecrites=nb_lignes_ecrites,
        nb_anomalies=nb_anomalies,
    )

# ---------------------------------------------------------------------------
# Règles métier (mapping mode + seuils)
# ---------------------------------------------------------------------------
def mode_to_gmaps(mode_decl: str) -> str:
    """
    Mapper le mode déclaré vers le paramètre 'mode' Google Directions API.
    """
    mapping = {
        "Marche/running": "walking",
        "Vélo/Trottinette/Autres": "bicycling",
        "Transports en commun": "transit",
        "véhicule thermique/électrique": "driving",
    }
    return mapping.get(mode_decl, "driving")

def seuil_km(mode_decl: str) -> Optional[float]:
    """
    Retourner le seuil (km) d'incohérence selon le mode déclaré.
    - Marche/running : 15 km
    - Vélo/Trottinette/Autres : 25 km
    - Autres modes : pas de seuil
    """
    if mode_decl == "Marche/running":
        return 15.0
    if mode_decl == "Vélo/Trottinette/Autres":
        return 25.0
    return None

# ---------------------------------------------------------------------------
# Client Google Maps (Directions API)
# ---------------------------------------------------------------------------
def call_google_directions(
    api_key: str,
    origin: str,
    destination: str,
    mode: str,
    timeout_s: int = 10,
) -> Tuple[int, int]:
    """
    Appeler Google Directions API et retourner (distance_m, duree_sec).
    Lève ValueError si aucun itinéraire exploitable n'est retourné.
    """
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "key": api_key,
    }
    
    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    
    payload = resp.json()
    status = payload.get("status")
    
    if status != "OK":
        raise ValueError(f"Google Maps status={status}")
    
    routes = payload.get("routes") or []
    legs = (routes[0].get("legs") or []) if routes else []
    
    if not legs:
        raise ValueError("Google Maps: aucune route exploitable")
    
    distance_val = (legs[0].get("distance") or {}).get("value")
    duration_val = (legs[0].get("duration") or {}).get("value")
    
    if distance_val is None or duration_val is None:
        raise ValueError("Google Maps: distance/durée manquantes")
    
    return int(distance_val), int(duration_val)

# ---------------------------------------------------------------------------
# Accès DB (source + SKIP + chargement cible)
# ---------------------------------------------------------------------------
def fetch_salaries_source(conn: psycopg.Connection) -> List[Tuple[str, str, str]]:
    """
    Lire la source de calcul via la table intermédiaire sec.lien_salarie :
    - cle_salarie : sec.lien_salarie.cle_salarie
    - adresse_dom : sec.rh_salarie.adresse_dom
    - mod_depl_decl : sec.rh_salarie.mod_depl_decl
    """
    query = """
        SELECT l.cle_salarie, r.adresse_dom, r.mod_depl_decl
        FROM sec.lien_salarie l
        JOIN sec.rh_salarie r ON r.id_salarie_brut = l.id_salarie_brut
        ORDER BY l.cle_salarie;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    return [
        (str(cle), (adresse or "").strip(), (mode or "").strip())
        for cle, adresse, mode in rows
    ]

def count_expected_salaries(conn: psycopg.Connection) -> int:
    """Compter le nombre de salariés attendus (sec.lien_salarie)."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sec.lien_salarie;")
        return int(cur.fetchone()[0])

def count_existing_ctrl(conn: psycopg.Connection) -> int:
    """Compter le nombre de lignes déjà présentes dans metier.ctrl_trajet."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM metier.ctrl_trajet;")
        return int(cur.fetchone()[0])

def reload_ctrl_trajet(conn: psycopg.Connection, rows: List[Tuple]) -> int:
    """
    Recharger complètement metier.ctrl_trajet.
    - TRUNCATE
    - INSERT batch
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE metier.ctrl_trajet;")
        
        query = """
            INSERT INTO metier.ctrl_trajet (
                cle_salarie, mode_trajet, distance_m, duree_sec,
                seuil_km, est_incoherent, date_ctrl
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cur.executemany(query, rows)
        
    return len(rows)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """
    Exécuter le contrôle trajets et charger metier.ctrl_trajet.
    Statuts (ops.run_metrique)
    - SKIP : table déjà complète et FORCE_GMAPS=0
    - SUCCESS : calcul/chargement effectué (y compris en mode "forced")
    - FAILED : erreur d'exécution
    """
    _add_project_root_to_syspath()
    _ensure_logger_imports()
    
    if load_dotenv is not None:
        repo_root = _find_repo_root(Path(__file__).resolve().parent)
        load_dotenv(dotenv_path=repo_root / ".env", override=False)
    
    args = _parse_args(sys.argv[1:])
    origin = str(args.origin)
    
    if get_logger is not None:
        log_level = str(args.log_level)
        logger = get_logger(SCRIPT_NAME, origin=origin, level=log_level)
    else:
        # Fallback minimal si l'utilitaire logger n'est pas importable
        import logging
        logging.basicConfig(
            level=str(args.log_level),
            format="%(asctime)s | %(levelname)s | mod80_recup_distances | origin=%(origin)s | %(message)s",
        )
        logger = logging.LoggerAdapter( # type: ignore[assignment]
            logging.getLogger("p12.mod80_recup_distances"),
            {"origin": origin},
        )
    
    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Variable manquante : GOOGLE_MAPS_API_KEY doit être définie.")
    
    force_gmaps = _parse_bool_env("FORCE_GMAPS", default="0")
    cfg = DbConfig.from_env()
    
    date_debut = datetime.now(tz=TZ_PARIS)
    statut = "SUCCESS"
    nb_lues = 0
    nb_ecrites = 0
    nb_anomalies = 0
    
    try:
        with connect_db(cfg) as conn:
            expected = count_expected_salaries(conn)
            existing = count_existing_ctrl(conn)
            
            if expected > 0 and existing == expected and not force_gmaps:
                statut = "SKIP"
                date_fin = datetime.now(tz=TZ_PARIS)
                write_run_metric(
                    conn=conn,
                    logger=logger,
                    nom_pipeline=NOM_PIPELINE,
                    date_debut_exe=date_debut,
                    date_fin_exe=date_fin,
                    statut=statut,
                    nb_lignes_lues=expected,
                    nb_lignes_ecrites=0,
                    nb_anomalies=0,
                )
                conn.commit()
                logger.info(
                    "SKIP metier.ctrl_trajet : table déjà complète (%s/%s).",
                    existing,
                    expected,
                )
                return 0
            
            if force_gmaps and expected > 0 and existing == expected:
                logger.info(
                    "FORCE_GMAPS=1 : bypass du SKIP (table complète %s/%s). Recalcul en cours.",
                    existing,
                    expected,
                )
            elif force_gmaps:
                logger.info("FORCE_GMAPS=1 : recalcul demandé. Traitement en cours.")
            
            salaries = fetch_salaries_source(conn)
            nb_lues = len(salaries)
            
            out_rows: List[Tuple] = []
            now_ctrl = datetime.now(tz=TZ_PARIS)
            
            for cle_salarie, adresse_dom, mode_decl in salaries:
                if not adresse_dom or not mode_decl:
                    nb_anomalies += 1
                    logger.warning(
                        "SKIP cle_salarie=%s | adresse_dom/manquant=%s | mod_depl_decl/manquant=%s",
                        cle_salarie,
                        0 if adresse_dom else 1,
                        0 if mode_decl else 1,
                    )
                    continue
                
                mode_api = mode_to_gmaps(mode_decl)
                seuil = seuil_km(mode_decl)
                
                try:
                    distance_m, duree_sec = call_google_directions(
                        api_key=api_key,
                        origin=adresse_dom,
                        destination=ENTREPRISE_ADRESSE,
                        mode=mode_api,
                    )
                except (requests.RequestException, ValueError) as exc:
                    nb_anomalies += 1
                    logger.warning(
                        "SKIP cle_salarie=%s | mode=%s | erreur=%s",
                        cle_salarie,
                        mode_decl,
                        str(exc),
                    )
                    continue
                
                incoherent = False
                if seuil is not None:
                    incoherent = distance_m > int(seuil * 1000)
                
                out_rows.append((
                    cle_salarie,
                    mode_decl,
                    distance_m,
                    duree_sec,
                    seuil,
                    incoherent,
                    now_ctrl,
                ))
            
            nb_ecrites = reload_ctrl_trajet(conn, out_rows)
            conn.commit()
            
            date_fin = datetime.now(tz=TZ_PARIS)
            write_run_metric(
                conn=conn,
                logger=logger,
                nom_pipeline=NOM_PIPELINE,
                date_debut_exe=date_debut,
                date_fin_exe=date_fin,
                statut="SUCCESS",
                nb_lignes_lues=nb_lues,
                nb_lignes_ecrites=nb_ecrites,
                nb_anomalies=nb_anomalies,
            )
            conn.commit()
            
            if log_success is not None:
                log_success(
                    logger,
                    message="Fin traitement : SUCCESS (forced)." if force_gmaps else "Fin traitement : SUCCESS.",
                    context={
                        "statut": "SUCCESS",
                        "nb_lignes_lues": nb_lues,
                        "nb_lignes_ecrites": nb_ecrites,
                        "nb_anomalies": nb_anomalies,
                        "force_gmaps": int(force_gmaps),
                    },
                )
            else:
                logger.info(
                    "Fin traitement : %s | nb_lignes_lues=%s | nb_lignes_ecrites=%s | nb_anomalies=%s | force_gmaps=%s",
                    "SUCCESS (forced)" if force_gmaps else "SUCCESS",
                    nb_lues,
                    nb_ecrites,
                    nb_anomalies,
                    int(force_gmaps),
                )
    
    except Exception as exc:
        statut = "FAILED"
        try:
            with connect_db(cfg) as conn2:
                date_fin = datetime.now(tz=TZ_PARIS)
                write_run_metric(
                    conn=conn2,
                    logger=logger,
                    nom_pipeline=NOM_PIPELINE,
                    date_debut_exe=date_debut,
                    date_fin_exe=date_fin,
                    statut=statut,
                    nb_lignes_lues=nb_lues,
                    nb_lignes_ecrites=nb_ecrites,
                    nb_anomalies=nb_anomalies,
                )
                conn2.commit()
        except Exception:
            # Ne pas masquer l'erreur principale si l'écriture de métrique échoue.
            pass
        
        if log_failure is not None:
            log_failure(
                logger,
                message="Erreur lors du calcul/chargement des trajets.",
                exc=exc,
                context={
                    "statut": statut,
                    "nb_lignes_lues": nb_lues,
                    "nb_lignes_ecrites": nb_ecrites,
                    "nb_anomalies": nb_anomalies,
                    "force_gmaps": int(force_gmaps),
                },
            )
        else:
            logger.exception(
                "Erreur lors du calcul/chargement des trajets. | statut=%s | nb_lignes_lues=%s | nb_lignes_ecrites=%s | nb_anomalies=%s | force_gmaps=%s | exception=%s",
                statut,
                nb_lues,
                nb_ecrites,
                nb_anomalies,
                int(force_gmaps),
                repr(exc),
            )
        return 1
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
