# -------------------------------------------------
# mod90_prepa_eligibilite.py
# -------------------------------------------------

"""
Création des vues d'éligibilité dans PostgreSQL.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\transform\\mod90_prepa_eligibilite.py --origin CLI

Arguments
---------
--origin : origine d'exécution (CLI ou KESTRA). Défaut : variable d'env ORIGIN ou CLI.
--log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Défaut : variable d'env LOG_LEVEL ou INFO.

Objectifs
---------
- (Re)créer les vues métiers permettant de déterminer l'éligibilité aux primes et avantages bien-être.
- Centraliser les règles de gestion métier directement dans la couche de données (Vues).
- Exposer une vue synthétique des avantages par salarié.

Entrées
-------
- Variables d'environnement :
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (Connexion DB)

- Fichiers SQL (définitions des vues) :
    src/sql/views/vue_elig_prime.sql
    src/sql/views/vue_elig_bien_etre.sql
    src/sql/views/vue_avantages_salarie.sql

- Tables / Vues existantes (PostgreSQL) :
    sec.lien_salarie
    sec.rh_salarie
    metier.vw_bi_salarie
    metier.vw_bi_ctrl_trajet
    metier.vw_bi_activite
    metier.vw_bi_param_effectif

Sorties
-------
- Vues PostgreSQL (schéma metier) :
    metier.vue_elig_prime : Calcul éligibilité prime vélo (trajets, salaires).
    metier.vue_elig_bien_etre : Calcul éligibilité bien-être (nombre d'activités).
    metier.vue_avantages_salarie : Synthèse des droits ouverts (PRIME_SEULE, BIEN_ETRE_SEUL, etc.).

- Métriques :
    Table ops.run_metrique (statut exécution, nombre de vues créées).

Traitements & fonctionnalités
-----------------------------
- Connexion à la base de données PostgreSQL via psycopg.
- Lecture des fichiers SQL externes depuis le dossier src/sql/views/.
- Exécution transactionnelle stricte (conn.transaction) des scripts SQL.
- Logging DEBUG des requêtes exécutées pour faciliter le diagnostic.
- Gestion défensive de l'écriture des métriques (bloc finally indépendant).

Contraintes
-----------
- Les vues BI sources (vw_bi_*) doivent exister au préalable.
- Les identifiants de connexion (secrets) doivent être fournis par l'environnement.
- Les fichiers SQL doivent être présents dans l'arborescence du projet.

Observations & remarques
------------------------
- Le script utilise une transaction explicite : tout ou rien.
- Le nombre de lignes écrites dans les métriques correspond au nombre de fichiers SQL exécutés.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import inspect
import logging
import os
import sys
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
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
def _ensure_logger_imports(repo_root: Path) -> None:
    """
    Rendre l'import de src.utils.logger disponible quel que soit le mode d'exécution.

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


def _safe_log_failure(logger, exc: BaseException, message: str, **context) -> None:
    """Logger un échec en restant compatible avec la signature réelle de log_failure()."""
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


def _safe_log_success(logger, message: str, **context) -> None:
    """Logger un succès en restant compatible avec la signature réelle de log_success()."""
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


def find_repo_root(start: Path) -> Path:
    """
    Trouver la racine du repo en remontant jusqu'à trouver un couple de dossiers
    caractéristiques (data/ et sql/).
    """
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "data").is_dir() and (parent / "sql").is_dir():
            return parent
    return start.resolve()


# -------------------------------------------------------------------
# Action 02 - Chargement SQL
# -------------------------------------------------------------------
def load_sql_file(repo_root: Path, filename: str) -> str:
    """
    Lire le contenu d'un fichier SQL dans sql/views/.
    """
    sql_path = repo_root / "sql" / "views" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")
    
    return sql_path.read_text(encoding="utf-8")


# -------------------------------------------------------------------
# Action 03 - Fonctions d'exécution
# -------------------------------------------------------------------
def run(conn: psycopg.Connection, repo_root: Path, logger: logging.Logger) -> int:
    """
    Exécuter les scripts SQL de création de vues dans une transaction.
    Retourne le nombre de fichiers exécutés.
    """
    files_to_run = [
        "vue_elig_prime.sql",
        "vue_elig_bien_etre.sql",
        "vue_avantages_salarie.sql",
    ]
    
    count = 0
    # Utilisation explicite d'une transaction pour garantir l'atomicité
    with conn.transaction():
        with conn.cursor() as cur:
            for fname in files_to_run:
                logger.info("Exécution du fichier SQL : %s", fname)
                sql_content = load_sql_file(repo_root, fname)
                
                # Log Debug du début de la requête
                logger.debug("Début requête SQL (%s) : %.100s ...", fname, sql_content)
                
                cur.execute(sql_content)
                count += 1
                
    return count


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parser les arguments CLI.

    - --origin : 'CLI' ou 'KESTRA'. Default = variable d'environnement ORIGIN sinon 'CLI'.
    - --log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Default = 'INFO'.
    """
    parser = argparse.ArgumentParser(prog="prepare_eligibilite")
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
    return args


# -------------------------------------------------------------------
# Action 04 - Main
# -------------------------------------------------------------------
def main() -> int:
    """Point d'entrée script."""
    if load_dotenv is not None:
        load_dotenv()

    script_path = Path(__file__).resolve()
    repo_root = find_repo_root(script_path.parent)
    _ensure_logger_imports(repo_root)

    args = _parse_args(sys.argv[1:])
    logger = get_logger("mod90_prepa_eligibilite", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz_paris)
    db = DbConfig.from_env()

    logger.info("Repo : %s", repo_root)
    logger.info("DB : %s@%s:%s / %s", db.user, db.host, db.port, db.database)

    statut = "FAILED_EXCEPTION"
    nb_lignes_lues = 0
    nb_lignes_ecrites = 0
    nb_anomalies = 0

    try:
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
            autocommit=False # Important pour gérer manuellement les transactions
        ) as conn:
            conn.execute("SET TIME ZONE 'Europe/Paris';")
            
            # Exécution transactionnelle
            nb_vues_recreees = run(conn, repo_root, logger)
            
            # Commit explicite final (bien que le block transaction() fasse le job, ceinture et bretelles)
            conn.commit()

        statut = "SUCCESS"
        nb_lignes_ecrites = int(nb_vues_recreees)
        _safe_log_success(logger, "vues éligibilité (re)créées.", nb_vues_recreees=nb_vues_recreees)
        return 0

    except Exception as exc:  # noqa: BLE001
        _safe_log_failure(
            logger,
            exc,
            message="Échec création des vues éligibilité.",
            repo=str(repo_root),
            database=db.database,
        )
        raise

    finally:
        date_fin_exe = datetime.now(tz_paris)
        # Écriture métrique défensive : ne doit jamais faire planter le script si l'action principale a réussi
        try:
            with psycopg.connect(
                host=db.host,
                port=db.port,
                user=db.user,
                password=db.password,
                dbname=db.database,
            ) as conn_metric:
                write_run_metric(
                    conn=conn_metric,
                    nom_pipeline="mod90_prepa_eligibilite",
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
        except Exception as metric_exc: # noqa: BLE001
            # On loggue l'erreur de métrique mais on ne propage pas l'exception
            logger.error("Erreur écriture métriques (non-bloquant) : %s", repr(metric_exc))


if __name__ == "__main__":
    sys.exit(main())
