#-------------------------------------------------
# mod62_load_activite_table.py
#-------------------------------------------------

"""
Charger la table PostgreSQL metier.activite à partir des activités
déclaratives (Gsheet) et, si présent, d’un fichier de simulation.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\load\\mod62_load_activite_table.py

Arguments
---------
--origin : origine d'exécution (CLI|KESTRA).
          Défaut : P12_RUN_ORIGIN sinon détection automatique.
--log-level : niveau de logs (DEBUG|INFO|WARNING|ERROR).
             Défaut : P12_LOG_LEVEL sinon INFO.

Objectifs
---------
- Charger uniquement la table metier.activite. [file:75]
- Appliquer une priorité au déclaratif Gsheet en cas de collision
  sur (cle_salarie, date_debut). [file:75]
- Bloquer le chargement si doublons (1 activité / salarié / jour local
  Europe/Paris). [file:75]

Entrées
-------
- Fichiers :
  - <repo>/data/processed/*_declaratif_activites_gsheet.csv (obligatoire).
  - <repo>/data/raw/declaratif_activites_12m.csv (optionnel). [file:75]
  - <repo>/src/utils/activite_mapping.yml (paramétrage colonnes/sources). [file:69]
  - <repo>/sql/update_activite.sql (requêtes SQL externalisées). [file:69]

- Variables d'environnement :
  - PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE. [file:75]

Sorties
-------
- Tables :
  - metier.activite (chargement/actualisation). [file:75]
  - ops.run_metrique (métriques d'exécution). [file:75]

Traitements & fonctionnalités
-----------------------------
- Charger le fichier Gsheet normalisé depuis data/processed. [file:75]
- Si le fichier de simulation est présent :
  - Lire Gsheet + simulation.
  - Fusionner avec priorité Gsheet sur (cle_salarie, date_debut). [file:75]
- Contrôler l'unicité (cle_salarie, jour local Europe/Paris) et arrêter
  le traitement si doublons. [file:75]
- Dédupliquer via la fonction utilitaire dedupliquer_activites_par_jour. [file:75]
- Charger en base via INSERT ... ON CONFLICT (UPSERT) sur
  (cle_salarie, date_debut, source_donnee). [file:75]
- Si simulation présente :
  - Supprimer en base les lignes dont source_donnee appartient à la liste
    paramétrée (sources_to_replace), puis insérer la simulation. [file:75]
  - Après COMMIT, supprimer le fichier de simulation. [file:75]
- Écrire une métrique ops.run_metrique en SUCCESS/FAILED. [file:75]

Contraintes
-----------
- Le script s'appuie sur des CSV déjà normalisés (schéma attendu). [file:75]
- En cas d'erreur de lecture/parsing/schéma/doublons : arrêt avant écriture
  métier (base inchangée). [file:75]
- La connexion PostgreSQL repose uniquement sur les variables d'environnement. [file:75]

Observations & remarques
------------------------
- Le contrôle de doublons se fait au jour local Europe/Paris à partir d'un
  timestamptz (date_debut). [file:75]
- Le choix du fichier Gsheet se fait via le dernier fichier correspondant
  au pattern *_declaratif_activites_gsheet.csv dans data/processed. [file:75]
- La suppression du fichier de simulation est effectuée uniquement après
  un commit réussi. [file:75]
"""

# ---------------
# IMPORTS
# ---------------

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import yaml

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


# -------------------------------------------------------------------
# Action 01 - Imports projet (normalisation + logger)
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]  # ./src/etl/load/mod62_load_activite_table.py -> racine repo

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Import métier indispensable
try:
    from src.utils.normalisation import dedupliquer_activites_par_jour
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Import projet impossible : src.utils.normalisation.dedupliquer_activites_par_jour. "
        "Vérifier que la racine du repo est dans PYTHONPATH et que 'src' est un package importable."
    ) from exc

# Import logger non bloquant
try:
    from src.utils.logger import get_logger as get_logger_project
    from src.utils.logger import log_failure as log_failure_project
    from src.utils.logger import log_success as log_success_project
    from src.utils.logger import write_run_metric as write_run_metric_project
except ImportError:  # pragma: no cover
    get_logger_project = None  # type: ignore[assignment]
    log_failure_project = None  # type: ignore[assignment]
    log_success_project = None  # type: ignore[assignment]
    write_run_metric_project = None  # type: ignore[assignment]


def _detect_origin() -> str:
    """Déduire une origine d'exécution (CLI/KESTRA)."""
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
    """Parser les arguments CLI."""
    parser = argparse.ArgumentParser(prog="mod62_load_activite_table")
    parser.add_argument(
        "--origin",
        default=(os.getenv("P12_RUN_ORIGIN", "").strip() or _detect_origin()),
        help="Origine d'exécution (CLI ou KESTRA).",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("P12_LOG_LEVEL", "INFO"),
        help="Niveau de logs (DEBUG, INFO, WARNING, ERROR).",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args


def _build_fallback_logger(script: str, origin: str, level: str) -> logging.LoggerAdapter:
    """Construire un logger minimal si src.utils.logger n'est pas importable."""
    base_logger = logging.getLogger(script)
    base_logger.setLevel((level or "INFO").upper())

    if not base_logger.handlers:
        fmt = "%(asctime)s | %(levelname)s | %(name)s | origin=%(origin)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        base_logger.addHandler(handler)
        base_logger.propagate = False

    class _Adapter(logging.LoggerAdapter):
        def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
            extra = kwargs.get("extra", {})
            extra.update({"origin": origin})
            kwargs["extra"] = extra
            return msg, kwargs

    return _Adapter(base_logger, {})


def get_logger(script: str, origin: str = "CLI", level: Optional[str] = None) -> logging.LoggerAdapter:
    """Créer un logger standardisé."""
    if get_logger_project is not None:
        return get_logger_project(script=script, origin=origin, level=level)
    return _build_fallback_logger(script=script, origin=origin, level=level or "INFO")


def log_success(
    logger: logging.LoggerAdapter,
    message: str,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """Logger un succès."""
    if log_success_project is not None:
        log_success_project(logger, message=message, context=context)
        return

    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())
    logger.info("SUCCESS | %s%s", message, suffix)


def log_failure(
    logger: logging.LoggerAdapter,
    exc: Exception,
    message: str,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """Logger un échec."""
    if log_failure_project is not None:
        log_failure_project(logger, exc=exc, message=message, context=context)
        return

    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())
    logger.error("FAILURE | %s%s | exception=%s", message, suffix, repr(exc))


def _write_run_metric_fallback(
    conn: "psycopg.Connection",
    nom_pipeline: str,
    date_debut_exe: datetime,
    date_fin_exe: datetime,
    statut: str,
    nb_lignes_lues: int,
    nb_lignes_ecrites: int,
    nb_anomalies: int,
) -> None:
    """Fallback local : écrire une métrique dans ops.run_metrique."""
    sql = """
        INSERT INTO ops.run_metrique (
            nom_pipeline, date_debut_exe, date_fin_exe, statut,
            nb_lignes_lues, nb_lignes_ecrites, nb_anomalies, origine
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
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
    conn: "psycopg.Connection",
    nom_pipeline: str,
    date_debut_exe: datetime,
    date_fin_exe: datetime,
    statut: str,
    nb_lignes_lues: int,
    nb_lignes_ecrites: int,
    nb_anomalies: int,
    logger: Optional[logging.LoggerAdapter] = None,
    raise_on_error: bool = False,
) -> None:
    """Écrire une métrique."""
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
            raise_on_error=raise_on_error,
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


# -------------------------------------------------------------------
# Action 02 - Configuration globale
# -------------------------------------------------------------------
TZ_PARIS = ZoneInfo("Europe/Paris")
YAML_REL_PATH = Path("src/utils") / "activite_mapping.yml"
SQL_REL_PATH = Path("sql") / "update_activite.sql"


# -------------------------------------------------------------------
# Action 03 - Utilitaires
# -------------------------------------------------------------------
@dataclass(frozen=True)
class DbConfig:
    """Configuration de connexion PostgreSQL."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @staticmethod
    def from_env() -> "DbConfig":
        """Construire la configuration à partir des variables d'environnement."""
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


def find_repo_root(start_dir: Path) -> Path:
    """Trouver la racine du repo."""
    current = start_dir.resolve()
    for _ in range(10):
        if (current / "src").exists() and (
            (current / "data").exists() or (current / "pyproject.toml").exists()
        ):
            return current
        if current.parent == current:
            break
        current = current.parent

    raise FileNotFoundError("Racine du repo introuvable depuis le chemin courant.")


def load_yaml_config(path: Path) -> Dict[str, Any]:
    """Charger la configuration YAML."""
    if not path.exists():
        raise FileNotFoundError(f"Configuration manquante : {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sql_queries(path: Path) -> Dict[str, str]:
    """Charger et parser le fichier SQL."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier SQL manquant : {path}")

    content = path.read_text(encoding="utf-8")
    queries = {}
    current_name = None
    current_lines = []

    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = stripped.replace("-- name:", "", 1).strip()
            current_lines = []
        elif current_name:
            if not stripped.startswith("--"):  # Ignore comments
                current_lines.append(line)

    if current_name:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries


def find_latest_gsheet_activites(data_processed_dir: Path) -> Path:
    """Trouver le dernier fichier gsheet normalisé."""
    candidates = sorted(data_processed_dir.glob("*_declaratif_activites_gsheet.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"Aucun fichier gsheet trouvé dans {data_processed_dir} (pattern '*_declaratif_activites_gsheet.csv')."
        )
    return candidates[-1]


def read_activites_csv(path_csv: Path, expected_cols: List[str]) -> pd.DataFrame:
    """Lire et valider un fichier d'activités."""
    df = pd.read_csv(path_csv)

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans {path_csv.name} : {missing}")

    return df


def prepare_dataframe_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalisation des types pour manipulation Pandas interne.
    Note : Le typage strict pour Psycopg se fait dans insert_activites.
    """
    df = df.copy()

    # Strings
    df["cle_salarie"] = df["cle_salarie"].astype(str).str.strip()
    df["type_activite"] = df["type_activite"].astype(str)
    df["commentaire"] = df["commentaire"].astype(str)
    df["source_donnee"] = df["source_donnee"].astype(str)

    # Dates
    df["date_debut"] = pd.to_datetime(df["date_debut"], utc=True, errors="coerce")
    if df["date_debut"].isna().any():
        nb = int(df["date_debut"].isna().sum())
        raise ValueError(f"date_debut contient {nb} valeurs invalides.")

    # Numériques (Pandas side)
    # On s'assure juste que c'est manipulable, le cast final est fait plus tard
    df["duree_sec"] = pd.to_numeric(df["duree_sec"], errors="coerce").fillna(0)
    df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")

    return df


def stop_if_doublons_jour(df: pd.DataFrame, logger: logging.LoggerAdapter, label: str) -> None:
    """Vérifier unicité jour local."""
    if df.empty:
        return

    local_dates = df["date_debut"].dt.tz_convert(TZ_PARIS).dt.date
    key = df["cle_salarie"] + "|" + local_dates.astype(str)
    dup = key.duplicated(keep=False)

    if dup.any():
        nb = int(dup.sum())
        logger.error(
            "Doublons détectés (%s) : %s lignes en conflit (cle_salarie + jour local).", label, nb
        )
        raise ValueError(
            f"STOP : doublons détectés ({label}) sur (cle_salarie, jour local Europe/Paris)."
        )


def fuser_simu_et_gsheet(df_simu: pd.DataFrame, df_gsheet: pd.DataFrame) -> pd.DataFrame:
    """Fusionner simulation et gsheet (priorité gsheet)."""
    if df_simu.empty:
        return df_gsheet.copy()
    if df_gsheet.empty:
        return df_simu.copy()

    key_cols = ["cle_salarie", "date_debut"]
    df_simu = df_simu.copy()
    df_gsheet = df_gsheet.copy()

    df_simu["_k"] = df_simu[key_cols].astype(str).agg("|".join, axis=1)
    df_gsheet["_k"] = df_gsheet[key_cols].astype(str).agg("|".join, axis=1)

    gsheet_keys = set(df_gsheet["_k"].tolist())
    df_simu_filtered = df_simu[~df_simu["_k"].isin(gsheet_keys)].copy()

    df_out = pd.concat([df_simu_filtered, df_gsheet], ignore_index=True)
    df_out = df_out.drop(columns=["_k"], errors="ignore")
    return df_out


# -------------------------------------------------------------------
# Action 04 - SQL : chargement metier.activite
# -------------------------------------------------------------------
def insert_activites(conn: psycopg.Connection, df: pd.DataFrame, queries: Dict[str, str]) -> int:
    """
    Insérer/mettre à jour des activités.
    Optimisation : itertuples + executemany.
    CORRECTION CRITIQUE : Conversion forcée en types natifs Python (int/None) avant envoi.
    """
    if df.empty:
        return 0

    now_paris = datetime.now(tz=TZ_PARIS)
    batch_data = []

    # Iteration rapide
    for row in df.itertuples(index=False):
        
        # 1. Gestion DUREE (Obligatoire)
        # On force la conversion native Python int(). 
        # numpy.int64 ou float(2500.0) deviennent int(2500).
        try:
            duree_val = int(row.duree_sec)
        except (ValueError, TypeError):
             # Securité: si conversion impossible, on met 0 (mais ne devrait pas arriver vu prepare_dataframe)
            duree_val = 0

        # 2. Gestion DISTANCE (Nullable)
        # Si pandas a chargé un float/nan pour le null, pd.isna le détecte.
        # Sinon, on force int() natif pour supprimer le type float ou numpy.
        if pd.isna(row.distance_m):
            dist_val = None
        else:
            try:
                dist_val = int(row.distance_m)
            except (ValueError, TypeError):
                dist_val = None

        batch_data.append(
            (
                row.cle_salarie,
                row.date_debut.to_pydatetime(),
                duree_val,  # int natif
                dist_val,   # int natif ou None
                row.type_activite,
                row.commentaire,
                row.source_donnee,
                now_paris,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(queries["insert_activite"], batch_data)

    return len(batch_data)


def reload_metier_activite_simule_only(
    conn: psycopg.Connection,
    df_simu: pd.DataFrame,
    queries: Dict[str, str],
    sources_to_replace: List[str],
) -> int:
    """
    Reload partiel (simulation).
    """
    if df_simu.empty:
        return 0

    with conn.cursor() as cur:
        cur.execute(queries["delete_by_source"], (sources_to_replace,))

    return insert_activites(conn, df_simu, queries)


# -------------------------------------------------------------------
# Action 05 - Main
# -------------------------------------------------------------------
def main() -> int:
    """Point d'entrée script."""
    if load_dotenv is not None:
        load_dotenv()

    args = _parse_args(sys.argv[1:])
    origin = str(args.origin)
    log_level = str(args.log_level)

    logger = get_logger(script="mod62_load_activite_table", origin=origin, level=log_level)

    script_path = Path(__file__).resolve()
    repo_root = find_repo_root(script_path.parent)
    data_raw_dir = repo_root / "data" / "raw"
    data_processed_dir = repo_root / "data" / "processed"

    path_simu = data_raw_dir / "declaratif_activites_12m.csv"
    path_gsheet = find_latest_gsheet_activites(data_processed_dir)

    path_config = repo_root / YAML_REL_PATH
    path_sql = repo_root / SQL_REL_PATH

    db = DbConfig.from_env()

    nom_pipeline = "mod62_load_activite_table"
    start = datetime.now(tz=TZ_PARIS)
    nb_lues = 0
    nb_ecrites = 0
    nb_anomalies = 0
    mode_activite = "UNKNOWN"

    logger.info("Repo=%s", repo_root)
    logger.info("Config=%s", path_config)
    logger.info("SQL=%s", path_sql)
    logger.info("DB=%s@%s:%s/%s", db.user, db.host, db.port, db.database)

    # ---------------------------------------------------------------
    # Action 06-a - Lecture + contrôles (avant DB)
    # ---------------------------------------------------------------
    try:
        # Chargement Config
        config = load_yaml_config(path_config)
        expected_cols = config.get("expected_cols", [])
        sources_to_replace = config.get("sources_to_replace", [])
        queries = load_sql_queries(path_sql)

        # Lecture Données
        df_gsheet_raw = read_activites_csv(path_gsheet, expected_cols)
        df_gsheet = prepare_dataframe_for_db(df_gsheet_raw)
        stop_if_doublons_jour(df_gsheet, logger, label="gsheet")

        df_gsheet_effective = df_gsheet
        df_simu_effective = pd.DataFrame(columns=df_gsheet.columns)
        mode_activite = "GSHEET_UPSERT_ONLY"

        if path_simu.exists():
            df_simu_raw = read_activites_csv(path_simu, expected_cols)
            df_simu = prepare_dataframe_for_db(df_simu_raw)
            stop_if_doublons_jour(df_simu, logger, label="simulation")

            df_fusion = fuser_simu_et_gsheet(df_simu=df_simu, df_gsheet=df_gsheet)
            stop_if_doublons_jour(df_fusion, logger, label="fusion")

            df_gsheet_effective = df_fusion[df_fusion["source_donnee"] == "csv_gsheet"].copy()
            df_simu_effective = df_fusion[df_fusion["source_donnee"] == "csv_simule"].copy()
            mode_activite = "GSHEET_UPSERT + SIMU_RELOAD_ONLY"

        # Déduplication
        res_gsheet = dedupliquer_activites_par_jour(df_gsheet_effective)
        df_gsheet_effective = res_gsheet[0] if isinstance(res_gsheet, tuple) else res_gsheet

        res_simu = dedupliquer_activites_par_jour(df_simu_effective)
        df_simu_effective = res_simu[0] if isinstance(res_simu, tuple) else res_simu

        nb_lues = int(df_gsheet_effective.shape[0] + df_simu_effective.shape[0])

    except Exception as exc:
        end = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="STOP : données d'entrée invalides (base inchangée).",
            context={
                "gsheet_path": str(path_gsheet),
                "simu_present": path_simu.exists(),
            },
        )
        try:
            with psycopg.connect(
                host=db.host,
                port=db.port,
                user=db.user,
                password=db.password,
                dbname=db.database,
            ) as conn_fail:
                conn_fail.execute("SET TIME ZONE 'Europe/Paris';")
                write_run_metric(
                    conn=conn_fail,
                    logger=logger,
                    nom_pipeline=nom_pipeline,
                    date_debut_exe=start,
                    date_fin_exe=end,
                    statut="FAILED",
                    nb_lignes_lues=nb_lues,
                    nb_lignes_ecrites=0,
                    nb_anomalies=max(nb_anomalies, 1),
                )
                conn_fail.commit()
        except Exception:
            pass
        return 1

    # ---------------------------------------------------------------
    # Action 06-b - Chargement DB (transactionnel)
    # ---------------------------------------------------------------
    try:
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            conn.execute("SET TIME ZONE 'Europe/Paris';")

            nb_gsheet = insert_activites(conn, df_gsheet_effective, queries)
            logger.info("Activités : GSHEET_UPSERT | lignes_traitées=%s", nb_gsheet)

            nb_simu = 0
            if path_simu.exists():
                nb_simu = reload_metier_activite_simule_only(
                    conn, df_simu_effective, queries, sources_to_replace
                )
                logger.info("Activités : SIMU_RELOAD_ONLY | lignes_traitées=%s", nb_simu)

            nb_ecrites = int(nb_gsheet + nb_simu)
            logger.info("Activités : %s | lignes_traitées=%s", mode_activite, nb_ecrites)

            end = datetime.now(tz=TZ_PARIS)
            write_run_metric(
                conn=conn,
                logger=logger,
                nom_pipeline=nom_pipeline,
                date_debut_exe=start,
                date_fin_exe=end,
                statut="SUCCESS",
                nb_lignes_lues=nb_lues,
                nb_lignes_ecrites=nb_ecrites,
                nb_anomalies=nb_anomalies,
            )
            conn.commit()

        # -----------------------------------------------------------
        # Action 06-c - Post-traitement fichiers
        # -----------------------------------------------------------
        if path_simu.exists():
            try:
                path_simu.unlink()
                logger.info("Suppression fichier simulation : %s", path_simu)
            except Exception as exc:
                raise RuntimeError(
                    f"Suppression du fichier simulation impossible : {path_simu}"
                ) from exc

        log_success(
            logger,
            message="Terminé : SUCCESS",
            context={
                "mode": mode_activite,
                "lues": nb_lues,
                "ecrites": nb_ecrites,
                "anomalies": nb_anomalies,
            },
        )
        return 0

    except Exception as exc:
        end = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="Échec load PostgreSQL.",
            context={
                "mode": mode_activite,
                "lues": nb_lues,
                "ecrites": nb_ecrites,
                "anomalies": max(nb_anomalies, 1),
            },
        )
        try:
            with psycopg.connect(
                host=db.host,
                port=db.port,
                user=db.user,
                password=db.password,
                dbname=db.database,
            ) as conn_fail:
                conn_fail.execute("SET TIME ZONE 'Europe/Paris';")
                write_run_metric(
                    conn=conn_fail,
                    logger=logger,
                    nom_pipeline=nom_pipeline,
                    date_debut_exe=start,
                    date_fin_exe=end,
                    statut="FAILED",
                    nb_lignes_lues=nb_lues,
                    nb_lignes_ecrites=nb_ecrites,
                    nb_anomalies=max(nb_anomalies, 1),
                )
                conn_fail.commit()
        except Exception:
            pass

        return 1


if __name__ == "__main__":
    sys.exit(main())
