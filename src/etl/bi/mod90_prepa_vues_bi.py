# -------------------------------------------------
# mod90_prepa_vues_bi.py
# -------------------------------------------------

"""
Préparer les vues BI (PostgreSQL) à partir des tables métiers.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\transform\\mod90_prepa_vues_bi.py

Arguments
---------
--origin : origine d'exécution (CLI ou KESTRA). Défaut : variable d'env ORIGIN ou CLI.
--log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Défaut : variable d'env LOG_LEVEL ou INFO.

Objectifs
---------
- Utiliser cle_salarie comme clé de jointure BI.
- Dériver la date locale (Europe/Paris) à partir des timestamptz.
- Exposer des champs BI dérivés (km, minutes) sans ajouter de colonnes physiques.
- Normaliser les libellés via le référentiel unique config_pipeline.yml.
- Gérer les valeurs non reconnues via *_normalise="INCONNU" et *_inconnu=true.
- Dédupliquer les activités (source de vérité pour Power BI).

Entrées
-------
- Fichiers :
    src/utils/config_pipeline.yml
    sql/views/vue_bi_activite.sql
    sql/views/vue_bi_salarie.sql
    sql/views/vue_bi_ctrl_trajet.sql
    sql/views/vue_bi_param_effectif.sql

- Tables :
    metier.activite
    sec.lien_salarie
    sec.rh_salarie
    metier.ctrl_trajet
    metier.param

Sorties
-------
- Vues (re)créées :
    metier.vw_bi_activite
    metier.vw_bi_salarie
    metier.vw_bi_ctrl_trajet
    metier.vw_bi_param_effectif

- Métriques :
    Table ops.run_metrique (statut exécution, nombre de vues créées).

Traitements & fonctionnalités
-----------------------------
- Chargement de la configuration YAML.
- Lecture des templates SQL natifs externalisés.
- Construction dynamique et sécurisée des clauses CASE WHEN via psycopg.sql (prévention injections).
- Exécution transactionnelle stricte (DROP/CREATE atomique).
- Logging niveau DEBUG des requêtes SQL finales avant exécution.

Contraintes
-----------
- Les dépendances Python (psycopg, pyyaml) doivent être installées.
- Les identifiants de connexion PostgreSQL doivent être définis en variables d'environnement.
- Les fichiers SQL doivent contenir les placeholders attendus (ex: {type_norm_sql}).

Observations & remarques
------------------------
- Le script recrée systématiquement les vues pour garantir l'alignement avec la configuration.
- Les noms de schémas sont centralisés en constantes dans le script.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import inspect
import sys
from datetime import datetime
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError("Dépendance manquante : pyyaml") from exc

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

# Constantes Schémas
SCHEMA_METIER = "metier"
SCHEMA_SEC = "sec"


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
    """Trouver la racine du repo en remontant jusqu'à data/ et sql/."""
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "data").is_dir() and (parent / "sql").is_dir():
            return parent
    return start.resolve()


def _read_yaml(path: Path) -> Dict[str, Any]:
    """Lire un YAML (dict) depuis un fichier."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier config introuvable : {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"YAML invalide (attendu dict) : {path}")

    return data


def _get_required(cfg: Dict[str, Any], dotted_path: str) -> Any:
    """Lire une clé obligatoire dans un dict via une clé 'a.b.c'."""
    node: Any = cfg
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"Clé obligatoire manquante : {dotted_path}")
        node = node[part]
    return node


def load_sql_template(repo_root: Path, filename: str) -> str:
    """Lire le contenu d'un fichier SQL template dans sql/views/."""
    sql_path = repo_root / "sql" / "views" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")
    return sql_path.read_text(encoding="utf-8")


# -------------------------------------------------------------------
# Action 02 - Construction SQL Dynamique (Sécurisée)
# -------------------------------------------------------------------

def _build_case_normalise_secure(
    col_expr_str: str,  # Expression SQL brute (ex: "type_activite") - attention ici on suppose la colonne sûre
    referentiel: List[str],
    alias_normalise: str,
    alias_inconnu: str,
    conn: psycopg.Connection,
) -> str:
    """
    Construire le bloc SQL CASE WHEN de manière sécurisée avec psycopg.sql.
    Retourne une chaîne SQL formattée prête à être injectée.
    """
    
    # Construction de l'expression normalisée SQL : lower(regexp_replace(btrim({col}), '\s+', ' ', 'g'))
    # On utilise sql.SQL pour assembler, mais col_expr_str est ici considéré comme une expression de colonne existante
    # On va le traiter comme un Identifier si c'est un nom de colonne simple, ou SQL brut si expression.
    # Pour simplifier et sécuriser, on assume que col_expr_str est un nom de colonne ou expression valide du template.
    
    col = sql.SQL(col_expr_str) 
    
    # Expression de normalisation
    norm_expr = sql.SQL("lower(regexp_replace(btrim({}), '\\s+', ' ', 'g'))").format(col)
    
    cases = []
    known_norms = []

    for canon in referentiel:
        canon_clean = str(canon)
        canon_norm = re.sub(r"\s+", " ", canon_clean.strip()).lower()
        
        # known_norms pour le NOT IN (...)
        known_norms.append(sql.Literal(canon_norm))
        
        # WHEN norm_col = 'canon_norm' THEN 'canon_clean'
        case_when = sql.SQL("WHEN {} = {} THEN {}").format(
            norm_expr,
            sql.Literal(canon_norm),
            sql.Literal(canon_clean)
        )
        cases.append(case_when)

    # Clause IN (...)
    if known_norms:
        in_list = sql.SQL(", ").join(known_norms)
    else:
        in_list = sql.SQL("''")

    # Assemblage final du bloc
    query_obj = sql.SQL("""
    CASE
        WHEN {col} IS NULL OR btrim({col}) = '' THEN 'INCONNU'
        {cases_block}
        ELSE 'INCONNU'
    END AS {alias_norm},

    (
        {col} IS NOT NULL
        AND btrim({col}) <> ''
        AND {norm_expr} NOT IN ({in_list})
    ) AS {alias_inc}
    """).format(
        col=col,
        cases_block=sql.SQL("\n        ").join(cases),
        alias_norm=sql.Identifier(alias_normalise),
        norm_expr=norm_expr,
        in_list=in_list,
        alias_inc=sql.Identifier(alias_inconnu)
    )
    
    # On retourne la string SQL rendue (as_string(conn) permet le quoting correct)
    return query_obj.as_string(conn)


def _build_list_literals(values: List[str], conn: psycopg.Connection) -> str:
    """Construire une liste SQL de littéraux sécurisés ('val1', 'val2')."""
    if not values:
        return "''"
    
    norm_values = [re.sub(r"\s+", " ", str(v).strip()).lower() for v in values]
    literals = [sql.Literal(v) for v in norm_values]
    query_obj = sql.SQL(", ").join(literals)
    return query_obj.as_string(conn)


# -------------------------------------------------------------------
# Action 03 - Préparation des Vues
# -------------------------------------------------------------------

def get_sql_view_bi_activite(conn: psycopg.Connection, repo_root: Path, cfg: Dict[str, Any]) -> str:
    template = load_sql_template(repo_root, "vue_bi_activite.sql")
    
    sports_pratiques = list(_get_required(cfg, "referentiels.sports_pratiques"))
    sports_endurance = list(_get_required(cfg, "regles_simulation.sports_endurance"))
    sports_non_endurance = list(_get_required(cfg, "regles_simulation.sports_non_endurance"))

    # Génération du bloc CASE
    type_norm_sql = _build_case_normalise_secure(
        "type_activite", 
        sports_pratiques, 
        "type_activite_normalise", 
        "type_activite_inconnu",
        conn
    )
    
    # Génération des listes IN (...)
    types_endurance_in = _build_list_literals(sports_endurance, conn)
    types_non_endurance_in = _build_list_literals(sports_non_endurance, conn)
    
    # Expression de normalisation à réinjecter pour les comparaisons dans le template
    # Attention: dans le template, l'alias utilisé est "src0.type_activite_normalise" pour la 2eme partie CTE
    # Mais le bloc généré par build_case contient "lower(...)".
    # Le template attend une *expression* pour {type_norm_expr}. 
    # Dans la CTE 'src', type_activite_normalise est déjà calculé.
    # Donc l'expression est simplement : lower(regexp_replace(btrim(src0.type_activite_normalise), '\s+', ' ', 'g'))
    # Ou plus simplement : src0.type_activite_normalise (car le CASE renvoie déjà du propre ?)
    # Le CASE renvoie le CANONICAL CLEAN (ex: "Running").
    # La comparaison doit se faire sur la version normalisée minuscule ("running").
    type_norm_expr = "lower(regexp_replace(btrim(src0.type_activite_normalise), '\\s+', ' ', 'g'))"

    # Injection native
    return template.format(
        type_norm_sql=type_norm_sql,
        type_norm_expr=type_norm_expr,
        types_endurance_in=types_endurance_in,
        types_non_endurance_in=types_non_endurance_in
    )


def get_sql_view_bi_salarie(conn: psycopg.Connection, repo_root: Path, cfg: Dict[str, Any]) -> str:
    template = load_sql_template(repo_root, "vue_bi_salarie.sql")
    modes = list(_get_required(cfg, "referentiels.modes_deplacement"))
    
    # Note: dans la vue salarie, la colonne source est "rs.mod_depl_decl"
    mode_norm_sql = _build_case_normalise_secure(
        "rs.mod_depl_decl",
        modes,
        "mod_depl_decl_normalise",
        "mod_depl_decl_inconnu",
        conn
    )
    
    return template.format(mode_norm_sql=mode_norm_sql)


def get_sql_view_bi_ctrl_trajet(conn: psycopg.Connection, repo_root: Path, cfg: Dict[str, Any]) -> str:
    template = load_sql_template(repo_root, "vue_bi_ctrl_trajet.sql")
    modes = list(_get_required(cfg, "referentiels.modes_deplacement"))
    
    # Note: dans la vue ctrl, la colonne source est "c.mode_trajet"
    mode_norm_sql = _build_case_normalise_secure(
        "c.mode_trajet",
        modes,
        "mode_trajet_normalise",
        "mode_trajet_inconnu",
        conn
    )
    
    return template.format(mode_norm_sql=mode_norm_sql)


def get_sql_view_bi_param_effectif(repo_root: Path) -> str:
    # Fichier statique, pas de formatage
    return load_sql_template(repo_root, "vue_bi_param_effectif.sql")


# -------------------------------------------------------------------
# Action 04 - Exécution Transactionnelle
# -------------------------------------------------------------------
def run(conn: psycopg.Connection, repo_root: Path, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    """
    Exécuter la création des vues de manière transactionnelle.
    """
    # 1. Préparation des requêtes finales
    sql_activite = get_sql_view_bi_activite(conn, repo_root, cfg)
    sql_salarie = get_sql_view_bi_salarie(conn, repo_root, cfg)
    sql_ctrl = get_sql_view_bi_ctrl_trajet(conn, repo_root, cfg)
    sql_param = get_sql_view_bi_param_effectif(repo_root)
    
    # Liste ordonnée (attention aux dépendances si elles existent entre vues)
    # Ici param_effectif est indépendant.
    # activite est indépendante.
    # salarie est indépendante.
    # ctrl est indépendante.
    create_statements = [
        ("vw_bi_param_effectif", sql_param),
        ("vw_bi_activite", sql_activite),
        ("vw_bi_salarie", sql_salarie),
        ("vw_bi_ctrl_trajet", sql_ctrl),
    ]

    drop_statements = [
        f"DROP VIEW IF EXISTS {SCHEMA_METIER}.vw_bi_ctrl_trajet CASCADE;",
        f"DROP VIEW IF EXISTS {SCHEMA_METIER}.vw_bi_activite CASCADE;",
        f"DROP VIEW IF EXISTS {SCHEMA_METIER}.vw_bi_salarie CASCADE;",
        f"DROP VIEW IF EXISTS {SCHEMA_METIER}.vw_bi_param_effectif CASCADE;",
    ]

    # 2. Exécution Atomique
    with conn.transaction():
        with conn.cursor() as cur:
            # Drops
            for stmt in drop_statements:
                logger.info("Exécution : %s", stmt)
                cur.execute(stmt)
            
            # Creates
            for view_name, stmt in create_statements:
                logger.info("Création de la vue : %s", view_name)
                logger.debug("SQL Complet (%s) :\n%s", view_name, stmt)
                cur.execute(stmt)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parser les arguments CLI.

    - --origin : 'CLI' ou 'KESTRA'. Par défaut : variable d'environnement ORIGIN sinon 'CLI'.
    - --log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Par défaut : 'INFO'.
    """
    parser = argparse.ArgumentParser(prog="mod90_prepa_vues_bi")
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
# Action 05 - Main
# -------------------------------------------------------------------
def main() -> int:
    """Point d'entrée CLI."""
    if load_dotenv is not None:
        load_dotenv()

    repo_root = find_repo_root(Path(__file__).parent)
    _ensure_logger_imports(repo_root)

    args = _parse_args(sys.argv[1:])
    logger = get_logger("mod90_prepa_vues_bi", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz_paris)

    config_path = repo_root / "src" / "utils" / "config_pipeline.yml"
    cfg = _read_yaml(config_path)
    db = DbConfig.from_env()

    logger.info("Repo : %s", repo_root)
    logger.info("Config : %s", config_path)
    logger.info("DB : %s@%s:%s / %s", db.user, db.host, db.port, db.database)

    nb_vues_recreees = 4
    statut = "FAILED_EXCEPTION"
    nb_lignes_lues = 0
    nb_lignes_ecrites = 0
    nb_anomalies = 0

    try:
        # Connexion principale (autocommit=False par défaut, mais géré par .transaction())
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            run(conn, repo_root, cfg, logger)

        statut = "SUCCESS"
        nb_lignes_ecrites = int(nb_vues_recreees)
        _safe_log_success(logger, "Terminé : vues BI (re)créées.", nb_vues_recreees=nb_vues_recreees)
        return 0

    except Exception as exc:  # noqa: BLE001
        _safe_log_failure(
            logger,
            exc,
            message="Échec création des vues BI.",
            repo=str(repo_root),
            config=str(config_path),
            database=db.database,
        )
        raise

    finally:
        date_fin_exe = datetime.now(tz_paris)
        # Écrire la métrique sur une connexion dédiée.
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
                    nom_pipeline="mod90_prepa_vues_bi",
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
            logger.error("Erreur écriture métriques : %s", metric_exc)


if __name__ == "__main__":
    raise SystemExit(main())
