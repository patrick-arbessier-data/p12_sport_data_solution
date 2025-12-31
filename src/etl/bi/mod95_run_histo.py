# -------------------------------------------------
# mod95_run_histo.py
# -------------------------------------------------

"""
Historiser les KPI (POC) en snapshotant les vues KPI dans des tables d'historique.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\bi\\mod95_run_histo.py

Arguments
---------
--origin : origine d'exécution (CLI ou KESTRA). Défaut : variable d'env ORIGIN ou CLI.
--log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Défaut : variable d'env LOG_LEVEL ou INFO.

Objectifs
---------
- Créer un run d'historisation dans ops.histo_run (status=RUNNING) avec les paramètres effectifs utilisés.
- Snapshotter les vues KPI (metier.*) dans les tables ops.histo_kpi_*.
- Garantir une écriture "tout ou rien" sur les snapshots KPI via une transaction unique.
- Vérifier l'existence des tables cibles avant traitement.
- Mettre à jour ops.histo_run en SUCCESS ou FAILURE (et conserver la trace en cas d'échec).
- Produire des logs standardisés via src.utils.logger (logger.py).
- Écrire une métrique d'exécution dans ops.run_metrique à chaque run.

Entrées
-------
- Variables d'environnement :
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

- Vues KPI (schéma metier) :
    metier.vue_kpi_cout_prime
    metier.vue_kpi_cout_prime_fixe
    metier.vue_kpi_incoherences
    metier.vue_kpi_jours_supplementaires
    metier.vue_kpi_pratique_sportive
    metier.vw_kpi_age

- Tables d'historique (schéma ops) :
    ops.histo_run
    ops.histo_kpi_*

Sorties
-------
- Tables d'historique (INSERT) :
    ops.histo_run (1 ligne)
    ops.histo_kpi_* (N lignes snapshotées)

- Métriques :
    ops.run_metrique (1 ligne)

Traitements & fonctionnalités
-----------------------------
- Validation technique : vérification de l'existence des tables d'historique cibles.
- Récupération des paramètres métiers actifs.
- Insertion initiale du run 'RUNNING'.
- Copie massive (INSERT SELECT) des données avec logging détaillé par table.
- Gestion transactionnelle optimisée (une seule connexion principale).
- Mise à jour finale du statut.

Contraintes
-----------
- Les dépendances Python (psycopg) doivent être installées.
- Les identifiants de connexion PostgreSQL doivent être définis en variables d'environnement.
- Les tables cibles (ops.histo_*) doivent exister.

Observations & remarques
------------------------
- Le script utilise désormais une seule connexion pour le flux principal, réduisant la charge sur la base.
- Une connexion distincte reste utilisée pour l'écriture finale de la métrique (indépendance transactionnelle).

"""

# ---------------
# IMPORTS
# ---------------

from __future__ import annotations

import argparse
import inspect
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

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

# --- CONSTANTES SQL ---
SQL_READ_PARAMS = """
    SELECT code_param, valeur_param, date_effet
    FROM metier.vw_bi_param_effectif
    ORDER BY code_param
"""

SQL_INSERT_RUN = """
    INSERT INTO ops.histo_run (status, error_message, param_effectif)
    VALUES ('RUNNING', NULL, %s)
    RETURNING id_histo_run
"""

SQL_UPDATE_RUN_STATUS = """
    UPDATE ops.histo_run
    SET status = %s,
        error_message = %s
    WHERE id_histo_run = %s
"""

SQL_CHECK_TABLE_EXISTS = """
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_schema = %s AND table_name = %s
"""

# Mapping Vue -> Table Histo
SNAPSHOT_SPECS: List[Tuple[str, str]] = [
    ("metier.vue_kpi_cout_prime", "ops.histo_kpi_cout_prime"),
    ("metier.vue_kpi_cout_prime_fixe", "ops.histo_kpi_cout_prime_fixe"),
    ("metier.vue_kpi_incoherences", "ops.histo_kpi_incoherences"),
    ("metier.vue_kpi_jours_supplementaires", "ops.histo_kpi_jours_sup"),
    ("metier.vue_kpi_pratique_sportive", "ops.histo_kpi_pratique_sport"),
    ("metier.vw_kpi_age", "ops.histo_kpi_age"),
]


def find_repo_root(start: Path) -> Path:
    """Trouver la racine du dépôt."""
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "src").is_dir() and (parent / "sql").is_dir():
            return parent
    return start.resolve()


def _ensure_logger_imports(repo_root: Path) -> None:
    """Rendre l'import de src.utils.logger disponible."""
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
    """Émettre un log d'échec sécurisé."""
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
    """Émettre un log de succès sécurisé."""
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
        """Construire la configuration depuis l'environnement."""
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
# Action 02 - Helpers Métiers
# -------------------------------------------------------------------
def validate_target_tables(conn: psycopg.Connection) -> None:
    """
    Vérifier que toutes les tables d'historique cibles existent.
    Lève une erreur si une table est manquante.
    """
    with conn.cursor() as cur:
        for _, full_table_name in SNAPSHOT_SPECS:
            schema, table = full_table_name.split(".")
            cur.execute(SQL_CHECK_TABLE_EXISTS, (schema, table))
            if cur.fetchone() is None:
                raise RuntimeError(f"Table cible manquante : {full_table_name}")


def read_param_effectif(conn: psycopg.Connection) -> Dict[str, Dict[str, Any]]:
    """Lire les paramètres effectifs pour archivage JSON."""
    params: Dict[str, Dict[str, Any]] = {}
    
    with conn.cursor() as cur:
        cur.execute(SQL_READ_PARAMS)
        rows = cur.fetchall()
        
        for code_param, valeur_param, date_effet in rows:
            params[str(code_param)] = {
                "valeur_param": valeur_param,
                "date_effet": date_effet.isoformat() if date_effet is not None else None,
            }
            
    return params


def insert_histo_run(conn: psycopg.Connection, param_effectif: Dict[str, Any]) -> int:
    """Créer le run (RUNNING)."""
    with conn.cursor() as cur:
        cur.execute(SQL_INSERT_RUN, (Jsonb(param_effectif),))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Échec insertion run histo")
        (id_histo_run,) = row
    return int(id_histo_run)


def update_histo_run_status(
    conn: psycopg.Connection,
    id_histo_run: int,
    status: str,
    error_message: str | None
) -> None:
    """Mettre à jour le statut du run."""
    with conn.cursor() as cur:
        cur.execute(SQL_UPDATE_RUN_STATUS, (status, error_message, int(id_histo_run)))


def snapshot_all_kpi(
    conn: psycopg.Connection,
    id_histo_run: int,
    ts_snapshot: datetime,
    logger: Any
) -> int:
    """
    Snapshotter toutes les vues KPI.
    Loggue le détail par table.
    """
    nb_total = 0
    insert_tpl = "INSERT INTO {table} SELECT %s::bigint, %s::timestamptz, v.* FROM {view} v"

    with conn.cursor() as cur:
        for view_name, table_name in SNAPSHOT_SPECS:
            sql = insert_tpl.format(table=table_name, view=view_name)
            cur.execute(sql, (int(id_histo_run), ts_snapshot))
            
            row_count = cur.rowcount or 0
            logger.debug("Snapshot %s -> %s : %d lignes", view_name, table_name, row_count)
            nb_total += int(row_count)
            
    return nb_total


# -------------------------------------------------------------------
# Action 03 - CLI
# -------------------------------------------------------------------
def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parser les arguments CLI."""
    parser = argparse.ArgumentParser(prog="mod95_run_histo")
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
    logger = get_logger("mod95_run_histo", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz_paris)
    ts_snapshot = datetime.now(tz_paris)

    statut = "FAILED_EXCEPTION"
    nb_lignes_lues = 0
    nb_lignes_ecrites = 0
    nb_anomalies = 0
    id_histo_run: int | None = None

    try:
        db = DbConfig.from_env()
        logger.info(
            "START | Historisation KPI | origin=%s | db=%s",
            args.origin,
            f"{db.user}@{db.host}:{db.port}/{db.database}",
        )

        # Utilisation d'une connexion unique pour tout le flux principal
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
            autocommit=False  # Gestion manuelle des transactions
        ) as conn:
            
            # 1. Validation technique (Fail Fast)
            logger.info("Validation des tables cibles...")
            validate_target_tables(conn)

            # 2. Lecture params + Création Run
            param_effectif = read_param_effectif(conn)
            nb_lignes_lues = len(param_effectif)
            
            # Commit intermédiaire pour créer le run (il doit exister même si le snapshot plante après)
            id_histo_run = insert_histo_run(conn, param_effectif)
            conn.commit() 
            logger.info("Run créé : id=%s (%d params)", id_histo_run, nb_lignes_lues)

            # 3. Snapshot (Bloc Transactionnel dédié)
            try:
                with conn.transaction():
                    nb_lignes_ecrites = snapshot_all_kpi(conn, id_histo_run, ts_snapshot, logger)
                
                # 4. Succès
                update_histo_run_status(conn, id_histo_run, "SUCCESS", None)
                conn.commit()
                
                statut = "SUCCESS"
                _safe_log_success(
                    logger,
                    "Historisation terminée.",
                    context={"id_histo_run": id_histo_run, "total_rows": nb_lignes_ecrites},
                )

            except Exception as snap_exc:
                # Si le snapshot échoue, on rollback le snapshot (auto via .transaction)
                # Mais on met à jour le run en FAILURE
                conn.rollback() # Par sécurité
                err_msg = str(snap_exc)[:4000]
                update_histo_run_status(conn, id_histo_run, "FAILURE", err_msg)
                conn.commit()
                raise snap_exc

        return 0

    except Exception as exc:  # noqa: BLE001
        _safe_log_failure(
            logger,
            exc,
            message="Erreur lors de l'historisation KPI.",
            context={"id_histo_run": id_histo_run},
        )
        # Note : Le statut FAILURE est déjà géré dans le bloc try principal si id_histo_run existait
        return 1

    finally:
        date_fin_exe = datetime.now(tz_paris)
        try:
            # Connexion dédiée pour métrique (indépendante du sort de la connexion principale)
            db = DbConfig.from_env()
            with psycopg.connect(
                host=db.host,
                port=db.port,
                user=db.user,
                password=db.password,
                dbname=db.database,
            ) as conn_metric:
                write_run_metric(
                    conn=conn_metric,
                    nom_pipeline="mod95_run_histo",
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
            logger.error("Impossible d'écrire la métrique ops.run_metrique : %s", exc)


if __name__ == "__main__":
    raise SystemExit(main())
