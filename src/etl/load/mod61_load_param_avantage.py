# -------------------------------------------------------------------
# Script de chargement / modification de la table metier.param
# -------------------------------------------------------------------
"""
src/etl/load/mod61_load_param_avantage.py

Chargement et mise à jour des paramètres "Avantage" dans PostgreSQL.

Table cible
-----------
metier.param
- code_param TEXT (PK/UNIQUE)
- valeur_param TEXT NOT NULL
- date_effet DATE (date d'alimentation initiale / mise à jour)

Source
------
src/utils/config_param_avantage.yml
Format attendu : mapping simple code_param: valeur_param (1 niveau).

SQL
---
sql/update_param.sql
Requêtes externalisées, découpées via commentaires:  -- name: <query_name>

Traitements
-----------
- Chargement du YAML en mémoire (mapping) via yaml.safe_load.
- Validation : mapping 1 niveau, clés non vides, valeurs scalaires (pas de dict/list), pas de NULL.
- Comparaison avec l'existant en base.
- Inserts : UPSERT des code_param absents en base.
- Mises à jour : UPDATE uniquement si valeur_param a changé (comparaison texte).
- date_effet : date du jour (Europe/Paris) uniquement pour les INSERT/UPDATE.
- Si un code_param existe en base mais est absent du YAML : inchangé.
- Si YAML manquant/invalide : STOP + base inchangée (log erreur).

Observabilité
-------------
- Logger projet : src/utils/logger.py (origin via --origin CLI|KESTRA ; défaut : détection automatique).
- Métrique : ops.run_metrique (nom_pipeline = "mod61_load_param_avantage")
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import psycopg
import yaml
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
TZ_PARIS = ZoneInfo("Europe/Paris")
LOG_LEVEL = os.getenv("P12_LOG_LEVEL", "INFO")
NOM_PIPELINE = "mod61_load_param_avantage"
YAML_REL_PATH = Path("src/utils") / "config_param_avantage.yml"
SQL_REL_PATH = Path("sql") / "update_param.sql"

# -------------------------------------------------------------------
# Imports projet (logger)
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]  # .../src/etl/load/mod61_load_param_avantage.py -> racine repo

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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
    """
    Parser les arguments CLI.
    - --origin : CLI|KESTRA (par défaut : détection automatique).
    - --log-level : DEBUG|INFO|WARNING|ERROR (par défaut : P12_LOG_LEVEL sinon INFO).
    Note : parse_known_args() ignore les arguments inconnus.
    """
    parser = argparse.ArgumentParser(prog="mod61_load_param_avantage")
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


def _ensure_logger_imports(repo_root: Path) -> None:
    """Assurer l'import de src.utils.logger après ajout de la racine projet au sys.path."""
    global get_logger_project, log_failure_project, log_success_project, write_run_metric_project

    if get_logger_project is not None and write_run_metric_project is not None:
        return

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from src.utils.logger import get_logger as _get_logger  # noqa: WPS433
        from src.utils.logger import log_failure as _log_failure  # noqa: WPS433
        from src.utils.logger import log_success as _log_success  # noqa: WPS433
        from src.utils.logger import write_run_metric as _write_run_metric  # noqa: WPS433

        get_logger_project = _get_logger
        log_failure_project = _log_failure
        log_success_project = _log_success
        write_run_metric_project = _write_run_metric
    except Exception:
        pass


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
    """Créer un logger standardisé (projet si disponible, sinon fallback)."""
    if get_logger_project is not None:
        return get_logger_project(script=script, origin=origin, level=level)
    return _build_fallback_logger(script=script, origin=origin, level=level or "INFO")


def log_success(
    logger: logging.LoggerAdapter,
    message: str,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """Logger un succès (projet si disponible, sinon fallback)."""
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
    """Logger un échec (projet si disponible, sinon fallback)."""
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
    """Écrire une métrique (projet si disponible, sinon fallback)."""
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
# Modèles
# -------------------------------------------------------------------
@dataclass(frozen=True)
class DbConfig:
    """Configuration de connexion PostgreSQL (env)."""

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
            raise ValueError("Variables manquantes : PGUSER / PGPASSWORD / PGDATABASE doivent être définies.")

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"PGPORT invalide : {port_str}") from exc

        return DbConfig(host=host, port=port, user=user, password=password, database=database)


# -------------------------------------------------------------------
# Utilitaires
# -------------------------------------------------------------------
def find_repo_root(start_dir: Path) -> Path:
    """
    Trouver la racine du repo (présence de 'src' + 'data' ou d'un 'pyproject.toml').

    Args:
        start_dir: Dossier de départ.

    Returns:
        Chemin de la racine repo.
    """
    current = start_dir.resolve()
    for _ in range(10):
        if (current / "src").exists() and ((current / "data").exists() or (current / "pyproject.toml").exists()):
            return current
        if current.parent == current:
            break
        current = current.parent
    raise FileNotFoundError("Racine du repo introuvable depuis le chemin courant.")


def parse_yaml_mapping(path_yaml: Path) -> Dict[str, str]:
    """
    Charger un YAML et valider un schéma minimal : mapping 1 niveau {str: scalar}.

    Règles:
    - Le fichier doit exister.
    - Le contenu doit être un dict (mapping).
    - Les clés doivent être des str non vides.
    - Les valeurs doivent être des scalaires (str/int/float/bool) et non nulles.
    - Les dict/list imbriqués sont refusés.

    Returns:
        Dict[str, str] : valeurs normalisées en str.
    """
    if not path_yaml.exists():
        raise FileNotFoundError(f"YAML manquant : {path_yaml}")

    raw = yaml.safe_load(path_yaml.read_text(encoding="utf-8"))

    if raw is None:
        return {}

    if not isinstance(raw, dict):
        raise ValueError("YAML invalide : attendu un mapping (1 niveau).")

    mapping: Dict[str, str] = {}
    for key, val in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"YAML invalide : clé non valide : {repr(key)}")

        if isinstance(val, (dict, list)):
            raise ValueError(f"YAML invalide : valeur non scalaire pour {key} (dict/list interdit).")

        if val is None:
            raise ValueError(f"YAML invalide : valeur NULL interdite pour {key} (valeur_param NOT NULL).")

        mapping[key.strip()] = str(val)

    return mapping


def load_sql_queries(path_sql: Path) -> Dict[str, str]:
    """
    Charger et parser un fichier SQL découpé en blocs par :
    -- name: <nom_requete>

    Retour:
        Dict[str, str] : {nom_requete: sql}
    """
    if not path_sql.exists():
        raise FileNotFoundError(f"Fichier SQL manquant : {path_sql}")

    content = path_sql.read_text(encoding="utf-8")

    queries: Dict[str, str] = {}
    current_name: Optional[str] = None
    current_lines: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name is not None:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = stripped.replace("-- name:", "", 1).strip()
            current_lines = []
            continue

        if current_name is None:
            continue

        current_lines.append(line)

    if current_name is not None:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries


# -------------------------------------------------------------------
# Fonctions SQL
# -------------------------------------------------------------------
def fetch_existing_params(conn: psycopg.Connection, queries: Dict[str, str]) -> Dict[str, str]:
    """Récupérer les paramètres existants en base (code_param -> valeur_param)."""
    query = queries["fetch_all_params"]
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {r[0]: r[1] for r in rows}


def diff_params(
    existing: Dict[str, str],
    params_yaml: Dict[str, str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Comparer les paramètres YAML aux existants.

    Returns:
    - to_insert : code_param absents de la base
    - to_update : code_param présents mais valeur différente
    """
    to_insert: Dict[str, str] = {}
    to_update: Dict[str, str] = {}

    for code, val in params_yaml.items():
        if code not in existing:
            to_insert[code] = val
        else:
            if str(existing[code]) != str(val):
                to_update[code] = val

    return to_insert, to_update


def apply_changes(
    conn: psycopg.Connection,
    queries: Dict[str, str],
    to_insert: Dict[str, str],
    to_update: Dict[str, str],
    date_effet: date,
) -> Tuple[int, int]:
    """
    Appliquer INSERT/UPDATE sur metier.param.

    Returns:
        (nb_inserts, nb_updates)
    """
    nb_inserts = 0
    nb_updates = 0

    keys_to_insert = sorted(to_insert.keys())
    if keys_to_insert:
        insert_rows = [(k, to_insert[k], date_effet) for k in keys_to_insert]
        with conn.cursor() as cur:
            cur.executemany(queries["insert_param"], insert_rows)
        nb_inserts = len(insert_rows)

    keys_to_update = sorted(to_update.keys())
    if keys_to_update:
        update_rows = [(to_update[k], date_effet, k) for k in keys_to_update]
        with conn.cursor() as cur:
            cur.executemany(queries["update_param"], update_rows)
        nb_updates = len(update_rows)

    return nb_inserts, nb_updates


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main() -> int:
    """Point d'entrée script."""
    if load_dotenv is not None:
        load_dotenv()

    _ensure_logger_imports(REPO_ROOT)

    args = _parse_args(sys.argv[1:])
    origin = str(args.origin)
    log_level = str(args.log_level)
    logger = get_logger(script="mod61_load_param_avantage", origin=origin, level=log_level)

    script_path = Path(__file__).resolve()
    repo_root = find_repo_root(script_path.parent)

    path_yaml = repo_root / YAML_REL_PATH
    path_sql = repo_root / SQL_REL_PATH

    db = DbConfig.from_env()

    start_dt = datetime.now(tz=TZ_PARIS)
    nb_lues = 0
    nb_ecrites = 0
    nb_anomalies = 0

    logger.info("Repo=%s", repo_root)
    logger.info("YAML paramètres=%s", path_yaml)
    logger.info("SQL=%s", path_sql)
    logger.info("DB=%s@%s:%s/%s", db.user, db.host, db.port, db.database)

    # Lecture/validation YAML + SQL (avant action DB)
    try:
        params_yaml = parse_yaml_mapping(path_yaml)
        queries = load_sql_queries(path_sql)
        nb_lues = len(params_yaml)

    except Exception as exc:
        end_dt = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="STOP : YAML/SQL invalide/manquant (base inchangée).",
            context={"yaml_path": str(path_yaml), "sql_path": str(path_sql)},
        )

        # Métrique d'échec (best effort)
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
                    nom_pipeline=NOM_PIPELINE,
                    date_debut_exe=start_dt,
                    date_fin_exe=end_dt,
                    statut="FAILED",
                    nb_lignes_lues=nb_lues,
                    nb_lignes_ecrites=nb_ecrites,
                    nb_anomalies=max(nb_anomalies, 1),
                )
                conn_fail.commit()
        except Exception:
            pass

        return 1

    # Synchronisation transactionnelle
    try:
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            conn.execute("SET TIME ZONE 'Europe/Paris';")

            existing = fetch_existing_params(conn, queries=queries)
            to_insert, to_update = diff_params(existing=existing, params_yaml=params_yaml)

            date_effet = datetime.now(tz=TZ_PARIS).date()

            nb_inserts, nb_updates = apply_changes(
                conn=conn,
                queries=queries,
                to_insert=to_insert,
                to_update=to_update,
                date_effet=date_effet,
            )

            nb_ecrites = nb_inserts + nb_updates
            end_dt = datetime.now(tz=TZ_PARIS)

            write_run_metric(
                conn=conn,
                logger=logger,
                nom_pipeline=NOM_PIPELINE,
                date_debut_exe=start_dt,
                date_fin_exe=end_dt,
                statut="SUCCESS",
                nb_lignes_lues=nb_lues,
                nb_lignes_ecrites=nb_ecrites,
                nb_anomalies=nb_anomalies,
            )

            conn.commit()

        log_success(
            logger,
            message="Terminé : SUCCESS",
            context={"lues": nb_lues, "ecrites": nb_ecrites, "anomalies": nb_anomalies},
        )
        return 0

    except Exception as exc:
        end_dt = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="Échec synchronisation PostgreSQL.",
            context={"lues": nb_lues, "ecrites": nb_ecrites, "anomalies": max(nb_anomalies, 1)},
        )

        # Métrique d'échec (best effort)
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
                    nom_pipeline=NOM_PIPELINE,
                    date_debut_exe=start_dt,
                    date_fin_exe=end_dt,
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
    raise SystemExit(main())
