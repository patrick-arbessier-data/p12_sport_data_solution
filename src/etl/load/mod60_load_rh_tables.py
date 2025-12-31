# -------------------------------------------------
# mod60_load_rh_tables.py
# -------------------------------------------------
"""
Ingestion des référentiels RH du POC "Avantages Sportifs" vers PostgreSQL.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\load\\mod60_load_rh_tables.py

Arguments
---------
--origin : origine d'exécution (CLI ou KESTRA). Défaut : détection automatique.
--log-level : niveau de logs (DEBUG/INFO/WARNING/ERROR). Défaut : P12_LOG_LEVEL sinon INFO.

Objectifs
---------
- Synchroniser sec.rh_salarie : miroir du fichier RH avec désactivation logique (actif = TRUE/FALSE).
- Mettre à jour sec.lien_salarie : 1 ligne par salarié actif, lien stable vers la clé métier.
- Alimenter metier.salarie : table métier alimentée à partir des liens + attributs RH.
- Mettre à jour sec.rh_salarie.sport_declare : sport déclaré issu du fichier "Données Sportive".

Entrées
-------
- Fichiers :
    data/raw/Données+RH.xlsx
    data/raw/Données+Sportive.xlsx
    src/utils/rh_mapping.yml (Configuration mapping)
    sql/upsert_salarie.sql (Requêtes SQL)

- Variables d'environnement :
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Sorties
-------
- Tables PostgreSQL (Mise à jour) :
    sec.rh_salarie
    sec.lien_salarie
    metier.salarie

- Mise à jour du sport déclaré :
    sec.rh_salarie.sport_declare

- Métrique d'exécution :
    ops.run_metrique

Traitements & fonctionnalités
-----------------------------
- Chargement de la configuration YAML et des requêtes SQL externes.
- Lecture et normalisation des fichiers Excel selon le mapping YAML.
- Synchronisation RH (sec.rh_salarie) : Upsert par lots (batch), désactivation des absents.
- Alimentation des liens stables (sec.lien_salarie) par lots.
- Alimentation de la table métier (metier.salarie) par lots.
- Mise à jour différentielle du sport déclaré.

Contraintes
-----------
- Les tables RH ne sont pas rechargées "en brut" mais synchronisées (UPSERT / mise à jour).
- Les écritures sont conditionnées (IS DISTINCT FROM) pour éviter les mises à jour inutiles.
- Les salariés absents du fichier RH sont désactivés (actif = FALSE) mais conservés.
- La métrique d'exécution est bloquante : échec => exécution en échec.

Observations & remarques
------------------------
- Le script gère un fallback de logging si le module projet n'est pas disponible.
- Les doublons dans les fichiers Excel sont dédublonnés sur l'id salarié (dernier conservé).

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
from typing import Any, Optional, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import yaml

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


# -------------------------------------------------------------------
# Action 01 - Imports projet (logger) + robustesse origin/log-level
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]  # ./src/etl/load/mod60_load_rh_tables.py -> racine repo
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Tentative d'import (mode normal). Si échec : fallback sans bloquer l'exécution.
try:
    from src.utils.logger import get_logger as get_logger_project
    from src.utils.logger import log_failure as log_failure_project
    from src.utils.logger import log_success as log_success_project
    from src.utils.logger import write_run_metric as write_run_metric_project
except ModuleNotFoundError:  # pragma: no cover
    get_logger_project = None  # type: ignore[assignment]
    log_failure_project = None  # type: ignore[assignment]
    log_success_project = None  # type: ignore[assignment]
    write_run_metric_project = None  # type: ignore[assignment]

TZ_PARIS = ZoneInfo("Europe/Paris")


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

    - --origin : CLI|KESTRA (par défaut : _detect_origin()).
    - --log-level : DEBUG|INFO|WARNING|ERROR (par défaut : P12_LOG_LEVEL sinon INFO).

    Note : parse_known_args() ignore les arguments inconnus (ex: '.' ajouté par erreur).
    """
    parser = argparse.ArgumentParser(prog="mod60_load_rh_tables")
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
    """
    Rendre l'import de src.utils.logger disponible quel que soit le mode d'exécution.

    Ne modifie pas les traitements : uniquement la robustesse des imports pour le logging/métriques.
    """
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
        # Conserver le fallback si l'import échoue
        pass


def _build_fallback_logger(script: str, origin: str, level: str) -> logging.LoggerAdapter:
    """
    Construire un logger minimal (mode dégradé) si src.utils.logger n'est pas importable.
    """
    root = logging.getLogger(script)
    root.setLevel(level.upper() if level else "INFO")

    if not root.handlers:
        fmt = "%(asctime)s | %(levelname)s | %(script)s | origin=%(origin)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
        root.propagate = False

    class _Adapter(logging.LoggerAdapter):
        def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
            extra = kwargs.get("extra", {})
            extra.update({"script": script, "origin": origin})
            kwargs["extra"] = extra
            return msg, kwargs

    return _Adapter(root, {})


def log_success(logger: logging.LoggerAdapter, message: str, context: Optional[dict[str, Any]] = None) -> None:
    """
    Logger un succès de manière standardisée.
    - Utilise src.utils.logger.log_success si disponible.
    - Sinon, fallback local.
    """
    if log_success_project is not None:
        log_success_project(logger, message=message, context=context)
        return

    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())
    logger.info("SUCCESS | %s%s", message, suffix)


def log_failure(
    logger: logging.LoggerAdapter,
    message: str,
    exc: Exception,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """
    Logger un échec de manière standardisée.
    - Utilise src.utils.logger.log_failure si disponible.
    - Sinon, fallback local.
    """
    if log_failure_project is not None:
        log_failure_project(logger, message=message, exc=exc, context=context)
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
    """
    Fallback local d'écriture de métrique dans ops.run_metrique.

    Objectif : garder une écriture métrique même si src.utils.logger.write_run_metric
    n'est pas importable.
    """
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
    **_kwargs: Any,
) -> None:
    """
    Écrire une métrique d'exécution simple dans ops.run_metrique.

    - Utilise src.utils.logger.write_run_metric si disponible.
    - Sinon, fallback local (insertion SQL).
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
            logger=_kwargs.get("logger"),
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
# Action 02 - Configuration DB
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


# -------------------------------------------------------------------
# Action 03 - Chargement Config & SQL (Amélioration Maintenance)
# -------------------------------------------------------------------
def load_mapping_config(path: Path) -> dict[str, Any]:
    """Charger le fichier de mapping YAML."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

def load_sql_queries(path: Path) -> dict[str, str]:
    """
    Charger et parser le fichier SQL.
    Les requêtes sont séparées par des commentaires '-- name: <nom_requete>'.
    Retourne un dictionnaire {nom_requete: code_sql}.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {path}")

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    queries = {}
    current_name = None
    current_sql = []

    for line in content.splitlines():
        line_stripped = line.strip()
        if line_stripped.startswith("-- name:"):
            if current_name:
                queries[current_name] = "\n".join(current_sql).strip()
            current_name = line_stripped.replace("-- name:", "").strip()
            current_sql = []
        elif current_name and not line_stripped.startswith("--"): # Ignore comments
             current_sql.append(line)
        elif current_name and line_stripped.startswith("--") and not line_stripped.startswith("-- name:"):
             continue # Ignore comments inside query block but keep structure
        elif current_name:
            current_sql.append(line)

    if current_name and current_sql:
        queries[current_name] = "\n".join(current_sql).strip()

    return queries


# -------------------------------------------------------------------
# Action 04 - Lecture Excel (Dynamique via Config)
# -------------------------------------------------------------------
def _find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Trouver une colonne existante parmi plusieurs libellés possibles (insensible à la casse)."""
    lower_map = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in lower_map:
            return lower_map[key]
    return None


def read_rh_excel(path: Path, config: dict[str, Any]) -> pd.DataFrame:
    """Lire le fichier RH et normaliser les colonnes selon la config."""
    df = pd.read_excel(path)

    rename_map_config = config.get("rename_map", {})
    rename_cols: dict[str, str] = {}
    
    for canonical, cands in rename_map_config.items():
        col = _find_column(df, cands)
        if col is None:
            raise ValueError(f"Colonne RH manquante : {canonical}")
        rename_cols[col] = canonical

    df = df.rename(columns=rename_cols)

    for col in config.get("str_cols", []):
        if col not in df.columns:
            raise ValueError(f"Colonne RH absente après mapping : {col}")
        df[col] = df[col].astype(str).fillna("").map(lambda x: x.strip())

    for col in config.get("date_cols", []):
        if col not in df.columns:
            raise ValueError(f"Colonne RH absente après mapping : {col}")
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df = df.drop_duplicates(subset=["id_salarie_brut"], keep="last").reset_index(drop=True)
    return df


def read_sportive_excel(path: Path) -> pd.DataFrame:
    """Lire le fichier Sportive et conserver les colonnes utiles pour sport_declare."""
    df = pd.read_excel(path)

    # Colonnes attendues (noms "souples" pour limiter les erreurs de libellé)
    # Note: On garde une logique simple ici car ce fichier est spécifique
    col_id = _find_column(df, ("id_salarie", "id salarié", "id salarié"))
    col_sport = _find_column(
        df, ("sport", "sport_declare", "sport déclaré", "sport declare", "Pratique d'un sport")
    )

    if col_id is None or col_sport is None:
        raise ValueError("Colonnes manquantes dans Données Sportive (id salarié / sport).")

    df = df.rename(columns={col_id: "id_salarie_brut", col_sport: "sport_declare"})
    df["id_salarie_brut"] = df["id_salarie_brut"].astype(str).fillna("").map(lambda x: x.strip())
    df["sport_declare"] = df["sport_declare"].astype(str).fillna("").map(lambda x: x.strip())

    df = df.drop_duplicates(subset=["id_salarie_brut"], keep="last").reset_index(drop=True)
    return df


# -------------------------------------------------------------------
# Action 05 - SQL helpers
# -------------------------------------------------------------------
def _execute_rowcount(conn: psycopg.Connection, sql: str, params: Optional[dict[str, Any] | tuple[Any, ...]] = None) -> int:
    """Exécuter une requête SQL et retourner le nombre de lignes réellement modifiées."""
    if params is None:
        cur = conn.execute(sql)
    else:
        cur = conn.execute(sql, params)

    try:
        rc = int(cur.rowcount)
    except Exception:
        rc = 0

    return rc if rc > 0 else 0

def _execute_batch(conn: psycopg.Connection, sql: str, params_list: list[tuple[Any, ...]]) -> int:
    """Exécuter une requête SQL en batch (executemany)."""
    if not params_list:
        return 0
    
    with conn.cursor() as cur:
        cur.executemany(sql, params_list)
        try:
            rc = int(cur.rowcount)
        except Exception:
            rc = 0
    return rc if rc > 0 else 0


def _fetch_all(
    conn: psycopg.Connection, sql: str, params: Optional[dict[str, Any]] = None
) -> list[tuple[Any, ...]]:
    """Exécuter une requête SQL et retourner toutes les lignes."""
    if params is None:
        cur = conn.execute(sql)
    else:
        cur = conn.execute(sql, params)
    return list(cur.fetchall())


# -------------------------------------------------------------------
# Action 06 - Synchronisation sec.rh_salarie (Batch)
# -------------------------------------------------------------------
def sync_sec_rh_salarie(conn: psycopg.Connection, df_rh: pd.DataFrame, queries: dict[str, str]) -> dict[str, int]:
    """
    Synchroniser sec.rh_salarie via Batch Processing.
    """
    stats = {"upserted": 0, "deactivated": 0}

    # 1. Identification des absents pour désactivation
    existing = _fetch_all(conn, "SELECT id_salarie_brut FROM sec.rh_salarie WHERE actif = TRUE;")
    ids_in_db_active = {str(r[0]) for r in existing}
    ids_in_file = set(df_rh["id_salarie_brut"].tolist())
    
    to_deactivate = list(ids_in_db_active - ids_in_file)
    
    if to_deactivate:
        # Utilisation de ANY(%s) -> on passe une liste comme un seul paramètre
        rc = _execute_rowcount(conn, queries["deactivate_absent"], (to_deactivate,))
        stats["deactivated"] = rc

    # 2. Préparation du batch pour UPSERT
    # Ordre params SQL : id, nom, prenom, bu, type, date_n, date_e, adr, mod
    batch_data = []
    for _, row in df_rh.iterrows():
        batch_data.append((
            str(row["id_salarie_brut"]),
            row["nom"],
            row["prenom"],
            row["bu"],
            row["type_contrat"],
            row["date_naissance"],
            row["date_embauche"],
            row["adresse_dom"],
            row["mod_depl_decl"]
        ))
    
    if batch_data:
        rc = _execute_batch(conn, queries["upsert_rh_salarie"], batch_data)
        stats["upserted"] = rc

    return stats


# -------------------------------------------------------------------
# Action 07 - Synchronisation sec.lien_salarie (Batch)
# -------------------------------------------------------------------
def sync_sec_lien_salarie(conn: psycopg.Connection, df_rh: pd.DataFrame, queries: dict[str, str]) -> dict[str, int]:
    """
    Synchroniser sec.lien_salarie via Batch Processing.
    """
    stats = {"inserted": 0}

    existing = _fetch_all(conn, "SELECT id_salarie_brut FROM sec.lien_salarie;")
    existing_ids = {str(r[0]) for r in existing}

    batch_data = []
    for sid in df_rh["id_salarie_brut"].tolist():
        sid = str(sid)
        if sid not in existing_ids:
            # id_salarie_brut, cle_salarie (identique ici)
            batch_data.append((sid, sid))
    
    if batch_data:
        rc = _execute_batch(conn, queries["insert_lien_salarie"], batch_data)
        stats["inserted"] = rc

    return stats


# -------------------------------------------------------------------
# Action 08 - Synchronisation metier.salarie (Batch)
# -------------------------------------------------------------------
def sync_metier_salarie(conn: psycopg.Connection, df_rh: pd.DataFrame, queries: dict[str, str]) -> dict[str, int]:
    """
    Synchroniser metier.salarie via Batch Processing.
    """
    stats = {"upserted": 0}

    # Récupération du mapping id_brut -> cle_salarie
    lien_rows = _fetch_all(conn, "SELECT id_salarie_brut, cle_salarie FROM sec.lien_salarie;")
    lien_map = {str(idb).strip(): str(cle).strip() for (idb, cle) in lien_rows}

    batch_data = []
    
    for _, row in df_rh.iterrows():
        id_brut = str(row["id_salarie_brut"]).strip()
        cle_salarie = lien_map.get(id_brut)
        
        if not cle_salarie:
            # En batch, difficile d'arrêter tout de suite, on log ou on raise.
            # Ici on raise pour garder la sécurité de l'ancien script.
            raise ValueError(f"STOP : lien manquant dans sec.lien_salarie pour id_salarie_brut={id_brut}")

        # Ordre params SQL: cle_salarie, nom, prenom, bu, mod_depl_decl
        batch_data.append((
            cle_salarie,
            row["nom"],
            row["prenom"],
            row["bu"],
            row["mod_depl_decl"]
        ))

    if batch_data:
        rc = _execute_batch(conn, queries["upsert_metier_salarie"], batch_data)
        stats["upserted"] = rc

    return stats


# -------------------------------------------------------------------
# Action 09 - Mise à jour sport_declare (Batch)
# -------------------------------------------------------------------
def sync_rh_sport_declare(conn: psycopg.Connection, df_sports: pd.DataFrame, queries: dict[str, str]) -> dict[str, int]:
    """
    Mettre à jour sec.rh_salarie.sport_declare via Batch Processing.
    """
    stats = {"updated": 0}

    batch_data = []
    for _, row in df_sports.iterrows():
        sid = str(row["id_salarie_brut"])
        sport = str(row["sport_declare"]).strip()
        
        # Gestion NULL via None pour le paramètre SQL
        val_sport = sport if sport else None
        
        # Ordre params SQL: sport_valeur, id_salarie, sport_valeur (pour le distinct check)
        batch_data.append((val_sport, sid, val_sport))

    if batch_data:
        rc = _execute_batch(conn, queries["update_sport_declare"], batch_data)
        stats["updated"] = rc

    return stats


# -------------------------------------------------------------------
# Action 10 - Main
# -------------------------------------------------------------------
def _load_env(repo_root: Path) -> None:
    """Charger le fichier .env en exécution locale si disponible."""
    if load_dotenv is None:
        return
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def main() -> int:
    """
    Point d'entrée.
    """
    _load_env(REPO_ROOT)
    _ensure_logger_imports(REPO_ROOT)

    args = _parse_args(sys.argv[1:])
    origin = str(args.origin)
    log_level = str(args.log_level)

    if get_logger_project is not None:
        logger = get_logger_project("mod60_load_rh_tables", origin=origin, level=log_level)
    else:
        logger = _build_fallback_logger("mod60_load_rh_tables", origin=origin, level=log_level)

    # Définition des chemins
    path_rh = REPO_ROOT / "data" / "raw" / "Données+RH.xlsx"
    path_sports = REPO_ROOT / "data" / "raw" / "Données+Sportive.xlsx"
    path_config = REPO_ROOT / "src" / "utils" / "rh_mapping.yml"
    path_sql = REPO_ROOT / "sql" / "upsert_salarie.sql"

    logger.info("Repo=%s", REPO_ROOT)
    logger.info("Config Mapping=%s", path_config)
    logger.info("SQL Queries=%s", path_sql)

    db = DbConfig.from_env()
    logger.info("DB=%s@%s:%s/%s", db.user, db.host, db.port, db.database)

    start = datetime.now(tz=TZ_PARIS)
    nb_lues_rh = 0
    nb_lues_sports = 0
    nb_lues = 0
    nb_ecrites = 0
    nb_anomalies = 0

    # ---------------------------------------------------------------
    # Action 11-a - Lecture / validation des inputs
    # ---------------------------------------------------------------
    try:
        # Chargement configuration & SQL
        mapping_config = load_mapping_config(path_config)
        sql_queries = load_sql_queries(path_sql)

        # Lecture Données
        df_rh = read_rh_excel(path_rh, mapping_config)
        df_sports = read_sportive_excel(path_sports)

        nb_lues_rh = int(df_rh.shape[0])
        nb_lues_sports = int(df_sports.shape[0])
        nb_lues = nb_lues_rh + nb_lues_sports

    except Exception as exc:
        end = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="STOP : configuration ou données invalides.",
            context={
                "rh_path": path_rh,
                "config_path": path_config,
            },
        )
        # Fallback métrique échec (similaire à l'ancien script)
        try:
            with psycopg.connect(
                host=db.host, port=db.port, user=db.user, password=db.password, dbname=db.database
            ) as conn_fail:
                conn_fail.execute("SET TIME ZONE 'Europe/Paris';")
                write_run_metric(
                    conn=conn_fail, logger=logger, nom_pipeline="mod60_load_rh_tables",
                    date_debut_exe=start, date_fin_exe=end, statut="FAILURE",
                    nb_lignes_lues=0, nb_lignes_ecrites=0, nb_anomalies=1,
                )
                conn_fail.commit()
        except Exception:
            pass
        return 1

    # ---------------------------------------------------------------
    # Action 11-b - Synchronisation PostgreSQL (Batch)
    # ---------------------------------------------------------------
    nom_pipeline = "mod60_load_rh_tables"

    try:
        with psycopg.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            dbname=db.database,
        ) as conn:
            conn.execute("SET TIME ZONE 'Europe/Paris';")

            stats_rh = sync_sec_rh_salarie(conn, df_rh, sql_queries)
            logger.info(
                "SYNC sec.rh_salarie : upserted=%s | deactivated=%s",
                stats_rh["upserted"],
                stats_rh["deactivated"],
            )

            stats_lien = sync_sec_lien_salarie(conn, df_rh, sql_queries)
            logger.info(
                "SYNC sec.lien_salarie : inserted=%s",
                stats_lien["inserted"],
            )

            stats_metier = sync_metier_salarie(conn, df_rh, sql_queries)
            logger.info(
                "SYNC metier.salarie : upserted=%s",
                stats_metier["upserted"],
            )

            stats_sport = sync_rh_sport_declare(conn, df_sports, sql_queries)
            logger.info(
                "SYNC sec.rh_salarie.sport_declare : updated=%s",
                stats_sport["updated"],
            )

            nb_ecrites = int(
                stats_rh["upserted"]
                + stats_rh["deactivated"]
                + stats_lien["inserted"]
                + stats_metier["upserted"]
                + stats_sport["updated"]
            )

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

        log_success(
            logger,
            message=f"Terminé : SUCCESS | lues_total={nb_lues} | ecrites_total={nb_ecrites}",
            context={
                "ecrites_total": nb_ecrites,
                "upserted_rh": stats_rh["upserted"],
                "deactivated_rh": stats_rh["deactivated"],
                "upserted_metier": stats_metier["upserted"],
                "updated_sport": stats_sport["updated"],
                "anomalies": nb_anomalies,
            },
        )
        return 0

    except Exception as exc:
        end = datetime.now(tz=TZ_PARIS)
        log_failure(
            logger,
            exc=exc,
            message="Échec synchronisation PostgreSQL.",
            context={
                "lues": nb_lues,
                "ecrites": nb_ecrites,
                "anomalies": max(nb_anomalies, 1),
            },
        )
        try:
            with psycopg.connect(
                host=db.host, port=db.port, user=db.user, password=db.password, dbname=db.database
            ) as conn_fail:
                conn_fail.execute("SET TIME ZONE 'Europe/Paris';")
                write_run_metric(
                    conn=conn_fail, logger=logger, nom_pipeline=nom_pipeline,
                    date_debut_exe=start, date_fin_exe=end, statut="FAILURE",
                    nb_lignes_lues=nb_lues, nb_lignes_ecrites=0, nb_anomalies=max(nb_anomalies, 1),
                )
                conn_fail.commit()
        except Exception:
            pass

        return 1


if __name__ == "__main__":
    sys.exit(main())
