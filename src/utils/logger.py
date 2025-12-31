#-------------------------------------------------
# logger.py
#-------------------------------------------------
"""
Module logger (réutilisable) pour standardiser les logs.

Commande d'exécution
--------------------
Ce module est destiné à être importé, pas exécuté directement.
from src.utils.logger import get_logger, log_success, log_failure, write_run_metric

Arguments
---------
Aucun (module utilitaire).

Objectifs
---------
- Standardiser les logs des scripts du projet sur stdout/stderr.
- Offrir un format stable : horodatage, niveau, script, origine, message.
- Fournir des helpers pour tracer succès/erreur avec contexte structuré.
- Centraliser l'écriture des métriques d'exécution dans PostgreSQL (ops.run_metrique).

Entrées
-------
- Variables d'environnement :
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (si écriture métrique)

Sorties
-------
- Flux standard :
    stdout (logs < ERROR)
    stderr (logs >= ERROR)

- Tables :
    ops.run_metrique (INSERT)

Traitements & fonctionnalités
-----------------------------
- Configuration de logging.LoggerAdapter avec injection de contexte (script, origin).
- Filtrage des niveaux de logs pour séparer stdout et stderr.
- Helpers log_success/log_failure pour uniformiser les messages de fin de traitement.
- Abstraction de l'écriture en base via write_run_metric (gestion transactionnelle).

Contraintes
-----------
- Dépend de psycopg pour l'écriture en base (import conditionnel via TYPE_CHECKING).
- Ne propage pas les logs aux root loggers pour éviter la duplication.

Observations & remarques
------------------------
- L'origine (CLI/KESTRA) est capturée et persistée dans la table de métriques.
- En cas d'échec d'écriture de métrique, l'erreur est loggée mais non bloquante par défaut.
"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

# -------------------------------------------------------------------
# Action 01 - Configuration du Logger Standard
# -------------------------------------------------------------------
DEFAULT_LOG_LEVEL = "INFO"


class _MaxLevelFilter(logging.Filter):
    """Filtre : accepter uniquement les niveaux < max_level."""

    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        """Retourner True si le record est strictement inférieur au max."""
        return record.levelno < self.max_level


def _parse_level(value: Optional[str]) -> int:
    """
    Convertir une valeur texte en niveau logging.

    Args:
        value: Niveau texte ("INFO", "DEBUG", ...)

    Returns:
        Niveau logging.
    """
    if not value:
        return logging.INFO
    upper = value.strip().upper()
    return getattr(logging, upper, logging.INFO)


class _ContextAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter qui injecte des champs de contexte.

    Champs supportés :
    - script
    - origin
    """

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        """Injecter extra dans les LogRecord."""
        extra = kwargs.get("extra", {})
        merged = {**self.extra, **extra}
        kwargs["extra"] = merged
        return msg, kwargs


def get_logger(
    script: str,
    origin: str = "CLI",
    level: Optional[str] = None,
) -> logging.LoggerAdapter:
    """
    Créer un logger standardisé pour un script.

    Args:
        script: Nom logique du script (ex: "normalisation_gsheet").
        origin: Origine d'exécution (ex: "CLI", "KESTRA").
        level: Niveau de log (si None, utilise LOG_LEVEL ou DEFAULT_LOG_LEVEL).

    Returns:
        LoggerAdapter configuré.
    """
    resolved_level = _parse_level(level or DEFAULT_LOG_LEVEL)
    base_logger = logging.getLogger(script)
    base_logger.setLevel(resolved_level)

    if not base_logger.handlers:
        fmt = "%(asctime)s | %(levelname)s | %(script)s | origin=%(origin)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

        # stdout : tout ce qui est strictement inférieur à ERROR
        handler_out = logging.StreamHandler(sys.stdout)
        handler_out.setFormatter(formatter)
        handler_out.addFilter(_MaxLevelFilter(logging.ERROR))

        # stderr : ERROR et au-dessus
        handler_err = logging.StreamHandler(sys.stderr)
        handler_err.setLevel(logging.ERROR)
        handler_err.setFormatter(formatter)

        base_logger.addHandler(handler_out)
        base_logger.addHandler(handler_err)
        base_logger.propagate = False

    return _ContextAdapter(base_logger, {"script": script, "origin": origin})


# -------------------------------------------------------------------
# Action 02 - Helpers de formatage et logging
# -------------------------------------------------------------------
def _format_context(context: Optional[dict[str, Any]]) -> str:
    """
    Formater un dict de contexte en suffixe lisible.

    Args:
        context: Contexte additionnel (ex: output_path=..., rows=...).

    Returns:
        Chaîne formatée.
    """
    if not context:
        return ""
    items = [f"{k}={v}" for k, v in context.items()]
    return " | " + " | ".join(items)


def log_success(
    logger: logging.LoggerAdapter,
    message: str,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """
    Logger un succès de manière standardisée.

    Args:
        logger: LoggerAdapter (get_logger()).
        message: Message principal.
        context: Contexte additionnel.
    """
    suffix = _format_context(context)
    logger.info("SUCCESS | %s%s", message, suffix)


def log_failure(
    logger: logging.LoggerAdapter,
    message: str,
    exc: Exception,
    context: Optional[dict[str, Any]] = None,
) -> None:
    """
    Logger un échec de manière standardisée.

    Args:
        logger: LoggerAdapter (get_logger()).
        message: Message principal.
        exc: Exception levée.
        context: Contexte additionnel.
    """
    suffix = _format_context(context)
    logger.error("FAILURE | %s%s | exception=%s", message, suffix, repr(exc))


# -------------------------------------------------------------------
# Action 03 - Métriques d'exécution (PostgreSQL)
# -------------------------------------------------------------------
@dataclass(frozen=True)
class RunMetric:
    """Structure de métrique d'exécution."""
    nom_pipeline: str
    date_debut_exe: datetime
    date_fin_exe: datetime
    statut: str
    nb_lignes_lues: int
    nb_lignes_ecrites: int
    nb_anomalies: int


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
    """
    Écrire une métrique d'exécution simple dans ops.run_metrique.

    Objectif :
    - Mutualiser l'insertion d'une métrique d'exécution (pipeline, dates, volumes, anomalies, statut).

    Args:
        conn: Connexion psycopg active.
        nom_pipeline: Nom logique (ex: "extract_gsheet").
        date_debut_exe: Date/heure début.
        date_fin_exe: Date/heure fin.
        statut: Statut (ex: "SUCCESS", "NO_CHANGE", ...).
        nb_lignes_lues: Nombre de lignes lues (sans entête).
        nb_lignes_ecrites: Nombre de lignes écrites (sans entête).
        nb_anomalies: Nombre d'anomalies.
        logger: Logger optionnel.
        raise_on_error: Lever l'exception si l'écriture échoue.

    Raises:
        Exception: si raise_on_error=True et que l'insertion échoue.
    """
    metric = RunMetric(
        nom_pipeline=nom_pipeline,
        date_debut_exe=date_debut_exe,
        date_fin_exe=date_fin_exe,
        statut=statut,
        nb_lignes_lues=nb_lignes_lues,
        nb_lignes_ecrites=nb_lignes_ecrites,
        nb_anomalies=nb_anomalies,
    )

    origin_value = "NO PARAM"
    if logger and hasattr(logger, "extra") and isinstance(getattr(logger, "extra"), dict):
        origin_value = str(logger.extra.get("origin") or "NO PARAM")

    sql = """
        INSERT INTO ops.run_metrique (
            nom_pipeline,
            date_debut_exe,
            date_fin_exe,
            statut,
            nb_lignes_lues,
            nb_lignes_ecrites,
            nb_anomalies,
            "Origine"
        )
        VALUES (
            %(nom_pipeline)s,
            %(date_debut_exe)s,
            %(date_fin_exe)s,
            %(statut)s,
            %(nb_lignes_lues)s,
            %(nb_lignes_ecrites)s,
            %(nb_anomalies)s,
            %(origine)s
        );
    """
    params = {
        "nom_pipeline": metric.nom_pipeline,
        "date_debut_exe": metric.date_debut_exe,
        "date_fin_exe": metric.date_fin_exe,
        "statut": metric.statut,
        "nb_lignes_lues": metric.nb_lignes_lues,
        "nb_lignes_ecrites": metric.nb_lignes_ecrites,
        "nb_anomalies": metric.nb_anomalies,
        "origine": origin_value,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

        if logger:
            logger.info(
                "METRIC | écrit en base | nom_pipeline=%s | origine=%s | statut=%s "
                "| lues=%s | ecrites=%s | anomalies=%s",
                metric.nom_pipeline,
                origin_value,
                metric.statut,
                metric.nb_lignes_lues,
                metric.nb_lignes_ecrites,
                metric.nb_anomalies,
            )
    except Exception as exc:
        if logger:
            logger.error(
                "METRIC | échec écriture en base | nom_pipeline=%s | origine=%s | exception=%s",
                metric.nom_pipeline,
                origin_value,
                repr(exc),
            )
        if raise_on_error:
            raise
