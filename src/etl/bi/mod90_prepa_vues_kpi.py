# -------------------------------------------------
# mod90_prepa_vues_kpi.py
# -------------------------------------------------

"""
Préparer les vues KPI (PostgreSQL) à partir d'un fichier SQL.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\bi\\mod90_prepa_vues_kpi.py

Arguments
---------
--origin : origine d'exécution (CLI ou KESTRA). Défaut : variable d'env ORIGIN ou CLI.
--log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Défaut : variable d'env LOG_LEVEL ou INFO.
--sql-file : chemin du fichier SQL à exécuter (optionnel). Défaut : sql/views/vues_kpi.sql

Objectifs
---------
- Exécuter un fichier SQL contenant des instructions CREATE VIEW (et éventuellement DROP VIEW).
- Garantir l'intégrité des vues métiers KPI sans altération par le code Python (logique SQL pure).
- Produire des logs standardisés incluant la traçabilité du fichier source (Hash MD5).
- Écrire une métrique d'exécution dans ops.run_metrique à chaque run.

Entrées
-------
- Variables d'environnement :
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

- Fichiers :
    sql/views/vues_kpi.sql (par défaut)

Sorties
-------
- Vues KPI (re)créées dans le schéma metier.
- Métriques :
    Table ops.run_metrique (statut exécution, nombre de vues créées).

Traitements & fonctionnalités
-----------------------------
- Chargement de la configuration DB depuis les variables d'environnement.
- Lecture du fichier SQL source.
- Calcul de l'empreinte MD5 du fichier SQL pour traçabilité dans les logs.
- Exécution transactionnelle du script SQL complet (support multi-commandes natif).
- Comptage indicatif des vues créées via analyse textuelle simple (Regex).

Contraintes
-----------
- Les dépendances Python (psycopg) doivent être installées.
- Les identifiants de connexion PostgreSQL doivent être définis en variables d'environnement.
- Le fichier SQL doit être valide et exécutable en un seul bloc.

Observations & remarques
------------------------
- La logique métier de correction des NULL (salaires manquants) a été déplacée directement dans le fichier SQL.
- Le script est agnostique au contenu SQL : il exécute ce qu'on lui donne.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import hashlib
import inspect
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple
from zoneinfo import ZoneInfo

import psycopg

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    from src.utils.logger import get_logger, log_failure, log_success, write_run_metric
except ModuleNotFoundError:  # pragma: no cover
    get_logger = None  # type: ignore[assignment]
    log_failure = None  # type: ignore[assignment]
    log_success = None  # type: ignore[assignment]
    write_run_metric = None  # type: ignore[assignment]


# -------------------------------------------------------------------
# Action 01 - Configuration & Logging
# -------------------------------------------------------------------

DEFAULT_SQL_FILE = "sql/views/vues_kpi.sql"


def find_repo_root(start: Path) -> Path:
    """
    Trouver la racine du dépôt en remontant jusqu'à des dossiers repères.

    Args:
        start: Dossier de départ.
    """
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "src").is_dir() and (parent / "sql").is_dir():
            return parent
    return start.resolve()


def _ensure_logger_imports(repo_root: Path) -> None:
    """
    Rendre l'import de src.utils.logger disponible quel que soit le mode d’exécution.

    Args:
        repo_root: Racine du dépôt (doit contenir le dossier 'src').
    """
    global get_logger, log_failure, log_success, write_run_metric

    if get_logger is not None:
        return

    sys.path.insert(0, str(repo_root))
    
    from src.utils.logger import get_logger as _get_logger  # noqa: WPS433
    from src.utils.logger import log_failure as _log_failure  # noqa: WPS433
    from src.utils.logger import log_success as _log_success  # noqa: WPS433
    from src.utils.logger import write_run_metric as _write_run_metric  # noqa: WPS433

    get_logger = _get_logger
    log_failure = _log_failure
    log_success = _log_success
    write_run_metric = _write_run_metric


def _safe_log_failure(logger: Any, exc: BaseException, message: str, **context: Any) -> None:
    """Émettre un log d'échec en restant compatible avec la signature de log_failure()."""
    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())

    try:
        sig = inspect.signature(log_failure)
        params = sig.parameters
        has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

        if has_varkw:
            try:
                log_failure(logger, message=message, exc=exc, **context)
                return
            except TypeError:
                log_failure(logger, message, exc, **context)
                return

        if "context" in params:
            try:
                log_failure(logger, message=message, exc=exc, context=context or None)
                return
            except TypeError:
                log_failure(logger, message, exc, context or None)
                return

        try:
            log_failure(logger, message=f"{message}{suffix}", exc=exc)
            return
        except TypeError:
            log_failure(logger, f"{message}{suffix}", exc)
            return

    except Exception:  # noqa: BLE001
        try:
            logger.exception("FAILURE | %s%s | exception=%s", message, suffix, repr(exc))
        except Exception:  # noqa: BLE001
            pass


def _safe_log_success(logger: Any, message: str, **context: Any) -> None:
    """Émettre un log de succès en restant compatible avec la signature de log_success()."""
    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())

    try:
        sig = inspect.signature(log_success)
        params = sig.parameters
        has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

        if has_varkw:
            try:
                log_success(logger, message=message, **context)
                return
            except TypeError:
                log_success(logger, message, **context)
                return

        if "context" in params:
            try:
                log_success(logger, message=message, context=context or None)
                return
            except TypeError:
                log_success(logger, message, context or None)
                return

        try:
            log_success(logger, message=f"{message}{suffix}")
            return
        except TypeError:
            log_success(logger, f"{message}{suffix}")
            return

    except Exception:  # noqa: BLE001
        try:
            logger.info("SUCCESS | %s%s", message, suffix)
        except Exception:  # noqa: BLE001
            pass


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
        """Construire la configuration à partir des variables d'environnement PG*."""
        host = os.getenv("PGHOST", "localhost").strip()
        port_str = os.getenv("PGPORT", "5432").strip()
        user = os.getenv("PGUSER", "").strip().strip('"')
        password = os.getenv("PGPASSWORD", "").strip().strip('"')
        database = os.getenv("PGDATABASE", "").strip().strip('"')

        if not user or not password or not database:
            raise ValueError("Variables manquantes : PGUSER / PGPASSWORD / PGDATABASE")

        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"PGPORT invalide : {port_str}") from exc

        return DbConfig(host=host, port=port, user=user, password=password, database=database)


# -------------------------------------------------------------------
# Action 02 - Helpers SQL
# -------------------------------------------------------------------
def _read_sql_file_with_hash(sql_path: Path) -> Tuple[str, str]:
    """
    Lire un fichier SQL en UTF-8 et calculer son hash MD5.

    Returns:
        Tuple (contenu_sql, hash_md5)
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")
    
    content = sql_path.read_text(encoding="utf-8")
    md5_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
    return content, md5_hash


def count_create_views_regex(sql_text: str) -> int:
    """
    Compter approximativement les statements de création de vues via Regex.
    Utilisé uniquement pour la métrique informative.
    """
    # Recherche case-insensitive de "CREATE [OR REPLACE] VIEW"
    pattern = r"(?is)CREATE\s+(OR\s+REPLACE\s+)?VIEW\b"
    return len(re.findall(pattern, sql_text))


# -------------------------------------------------------------------
# Action 03 - Exécution Transactionnelle
# -------------------------------------------------------------------
def run_sql_script(conn: psycopg.Connection, sql_text: str, logger: Any) -> None:
    """
    Exécuter le bloc SQL complet dans une transaction.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            # Exécution en bloc : PostgreSQL gère les multiples instructions séparées par ';'
            cur.execute(sql_text)
            logger.debug("Script SQL exécuté avec succès.")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parser les arguments CLI.

    - --origin : 'CLI' ou 'KESTRA'. Par défaut : variable d'environnement ORIGIN sinon 'CLI'.
    - --log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Par défaut : 'INFO'.
    - --sql-file : chemin du fichier SQL à exécuter (optionnel).
    """
    parser = argparse.ArgumentParser(prog="mod90_prepa_vues_kpi")
    parser.add_argument(
        "--origin",
        default=os.getenv("ORIGIN", "CLI"),
        help="Origine d'exécution (CLI ou KESTRA).",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Niveau de logs (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--sql-file",
        default=None,
        help="Chemin du fichier SQL à exécuter (optionnel).",
    )
    args, _unknown = parser.parse_known_args(argv)
    
    allowed_origins = {"CLI", "KESTRA"}
    origin = str(args.origin).strip().upper()
    if origin not in allowed_origins:
        raise ValueError(f"--origin invalide : {args.origin}. Valeurs attendues : {sorted(allowed_origins)}")
    args.origin = origin
    return args


# -------------------------------------------------------------------
# Action 04 - Main
# -------------------------------------------------------------------
def main() -> int:
    """Point d'entrée CLI."""
    if load_dotenv is not None:
        load_dotenv()

    repo_root = find_repo_root(Path(__file__).parent)
    _ensure_logger_imports(repo_root)

    args = _parse_args(sys.argv[1:])
    logger = get_logger("mod90_prepa_vues_kpi", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz_paris)

    statut = "FAILED_EXCEPTION"
    nb_lignes_lues = 0
    nb_lignes_ecrites = 0
    nb_anomalies = 0

    try:
        db = DbConfig.from_env()

        # Détermination du fichier SQL
        if args.sql_file:
            sql_path = Path(str(args.sql_file)).expanduser()
            if not sql_path.is_absolute():
                sql_path = (repo_root / sql_path).resolve()
        else:
            sql_path = (repo_root / DEFAULT_SQL_FILE).resolve()

        # Lecture et Hashage
        sql_content, sql_hash = _read_sql_file_with_hash(sql_path)
        nb_vues_detectees = count_create_views_regex(sql_content)

        logger.info(
            "START | Préparation des vues KPI | origin=%s | sql_file=%s (md5=%s) | db=%s",
            args.origin,
            str(sql_path),
            sql_hash,
            f"{db.user}@{db.host}:{db.port}/{db.database}",
        )

        # Connexion et exécution
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            run_sql_script(conn, sql_content, logger)

        statut = "SUCCESS"
        nb_lignes_ecrites = int(nb_vues_detectees)
        _safe_log_success(logger, "Terminé : vues KPI (re)créées.", nb_vues_recreees=nb_vues_detectees)
        return 0

    except Exception as exc:  # noqa: BLE001
        _safe_log_failure(logger, exc, "Erreur lors de la préparation des vues KPI.")
        return 1

    finally:
        date_fin_exe = datetime.now(tz_paris)
        try:
            db = DbConfig.from_env()
            # Écrire la métrique sur une connexion dédiée.
            with psycopg.connect(
                host=db.host,
                port=db.port,
                user=db.user,
                password=db.password,
                dbname=db.database,
            ) as conn_metric:
                write_run_metric(
                    conn=conn_metric,
                    nom_pipeline="mod90_prepa_vues_kpi",
                    date_debut_exe=date_debut_exe,
                    date_fin_exe=date_fin_exe,
                    statut=statut,
                    nb_lignes_lues=int(nb_lignes_lues),
                    nb_lignes_ecrites=int(nb_lignes_ecrites),
                    nb_anomalies=int(nb_anomalies),
                    logger=logger,
                    raise_on_error=True,
                )
                conn_metric.commit()
        except Exception as exc:  # noqa: BLE001
            _safe_log_failure(logger, exc, "Impossible d'écrire la métrique ops.run_metrique.")


if __name__ == "__main__":
    raise SystemExit(main())
