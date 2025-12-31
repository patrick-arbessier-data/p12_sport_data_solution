#-------------------------------------------------
# alerting.py
#-------------------------------------------------
"""
# Publier un message Slack en cas d'erreurs/anomalies et logger la métrique.

Commande d'exécution
--------------------
python src/slack/alerting.py --pipeline norm_gsheet --origin CLI
.\\.venv\\Scripts\\python.exe .\\src\\slack\\alerting.py --pipeline norm_gsheet

Arguments
---------
--pipeline : Nom du pipeline pour le message (défaut: norm_gsheet).
--origin : Origine de l'exécution (CLI ou KESTRA). Auto-détecté si omis.
--log-level : Niveau de log (DEBUG, INFO, WARNING, ERROR).

Objectifs
---------
- Publier un message Slack dans le channel d’alerting (p12-alerting) en cas d’erreurs
  bloquantes et/ou d’anomalies détectées par la normalisation Google Sheet.
- Enregistrer une métrique d’exécution dans PostgreSQL (table ops.run_metrique).

Entrées
-------
- Fichiers :
	/logs/YYYYMMDD_HHMMSS_gsheet_erreur.csv
	/logs/YYYYMMDD_HHMMSS_gsheet_anomalie.csv
	/data/raw/*_decla_sheet.csv (informationnel)
	.env (optionnel)

- Variables d'environnement :
	SLACK_ALERTING_URL
	PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
	P12_LOG_LEVEL ou LOG_LEVEL

Sorties
-------
- Slack :
	Message multi-lignes sur le webhook configuré.

- Tables :
	ops.run_metrique

Traitements & fonctionnalités
-----------------------------
- Détection du dernier run via timestamp des logs.
- Lecture des fichiers CSV d'erreurs et d'anomalies.
- Construction d'un message Slack formaté (erreurs exhaustives, anomalies limitées).
- Envoi du message via webhook HTTP.
- Enregistrement du statut d'exécution en base de données.

Contraintes
-----------
- L’envoi Slack est bloquant en cas d’échec : si l’envoi échoue, le script retourne un code non-zéro.
- L’écriture de métrique en base est bloquante.
- Un seul message Slack multi-lignes par exécution.
- Les secrets Postgres et Slack viennent de l'environnement.

Observations & remarques
------------------------
- Si aucun fichier de log n'est trouvé, le script termine en succès sans alerte.
- Message d’échec Slack à journaliser : "L'envoi d'alerte à Slack a échoué".
- Si anomalies > 10, affichage limité à 3 exemples.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.request
import psycopg

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


# -------------------------------------------------------------------
# Action 01 - Fonctions utilitaires (chemins et imports dynamiques)
# -------------------------------------------------------------------

def _find_repo_root() -> Path:
    """
    Déterminer la racine du dépôt à partir de l’emplacement de ce fichier.

    Returns:
        Path: Chemin absolu vers la racine du projet.
    """
    return Path(__file__).resolve().parents[2]


def _ensure_project_imports(repo_root: Path) -> None:
    """
    Assurer que l’import 'src.*' est possible quel que soit le mode d’exécution.

    Args:
        repo_root: Chemin racine du dépôt.
    """
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _get_logger():
    """Importer get_logger depuis le module logger du projet."""
    from src.utils.logger import get_logger  # noqa: WPS433
    return get_logger


def _log_success():
    """Importer log_success depuis le module logger du projet."""
    from src.utils.logger import log_success  # noqa: WPS433
    return log_success


def _log_failure():
    """Importer log_failure depuis le module logger du projet."""
    from src.utils.logger import log_failure  # noqa: WPS433
    return log_failure


# -------------------------------------------------------------------
# Action 02 - Définition des structures de données
# -------------------------------------------------------------------

@dataclass(frozen=True)
class RunFiles:
    """
    Structure représentant les fichiers de logs d'un run spécifique.

    Attributes:
        run_tag: Identifiant du run (timestamp).
        errors_path: Chemin vers le fichier d'erreurs (ou None).
        anomalies_path: Chemin vers le fichier d'anomalies (ou None).
    """
    run_tag: str
    errors_path: Path | None
    anomalies_path: Path | None


# -------------------------------------------------------------------
# Action 03 - Gestion des fichiers de logs (sélection et lecture)
# -------------------------------------------------------------------

def _list_run_tags(logs_dir: Path) -> list[str]:
    """
    Lister les timestamps disponibles d’après les fichiers logs présents.

    Args:
        logs_dir: Répertoire contenant les logs.

    Returns:
        list[str]: Liste triée des tags (timestamps) trouvés.
    """
    tags: set[str] = set()
    for p in logs_dir.glob("*_gsheet_erreur.csv"):
        tags.add(p.name.split("_gsheet_erreur.csv")[0])
    for p in logs_dir.glob("*_gsheet_anomalie.csv"):
        tags.add(p.name.split("_gsheet_anomalie.csv")[0])
    return sorted(tags)


def _pick_latest_run(logs_dir: Path) -> RunFiles | None:
    """
    Sélectionner le run le plus récent à partir des fichiers présents dans logs.

    Prend le timestamp le plus récent parmi les fichiers erreurs/anomalies présents.
    Utilise les fichiers portant ce timestamp (si un type manque, il est considéré vide).

    Args:
        logs_dir: Répertoire des logs.

    Returns:
        RunFiles | None: Objet RunFiles du dernier run ou None si aucun log.
    """
    run_tags = _list_run_tags(logs_dir)
    if not run_tags:
        return None

    run_tag = run_tags[-1]
    err_path = logs_dir / f"{run_tag}_gsheet_erreur.csv"
    anom_path = logs_dir / f"{run_tag}_gsheet_anomalie.csv"

    return RunFiles(
        run_tag=run_tag,
        errors_path=err_path if err_path.exists() else None,
        anomalies_path=anom_path if anom_path.exists() else None,
    )


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """
    Lire un CSV UTF-8 avec en-tête.

    Args:
        path: Chemin du fichier CSV.

    Returns:
        tuple: (header, rows) où rows est une liste de dictionnaires.
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        header = list(reader.fieldnames or [])
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append({k: (v or "").strip() for k, v in row.items()})
    return header, rows


def _pick_latest_raw_filename(repo_root: Path) -> str:
    """
    Retrouver le dernier fichier raw '*_decla_sheet.csv' (informationnel).

    Args:
        repo_root: Racine du projet.

    Returns:
        str: Nom du fichier ou 'inconnu'.
    """
    raw_dir = repo_root / "data" / "raw"
    candidates = sorted(raw_dir.glob("*_decla_sheet.csv"), key=lambda p: p.name, reverse=True)
    return candidates[0].name if candidates else "inconnu"


# -------------------------------------------------------------------
# Action 04 - Construction du message Slack
# -------------------------------------------------------------------

def _row_to_message_line(row: dict[str, str], header: list[str]) -> str:
    """
    Construire une ligne de détail pour Slack.

    Format : • *raison* : val1,val2...

    Args:
        row: Dictionnaire des valeurs de la ligne.
        header: Liste des colonnes.

    Returns:
        str: Ligne formatée.
    """
    raison = (row.get("raison") or "").strip()
    raison_prefix = f"*{raison}* " if raison else "*raison manquante*"

    values: list[str] = []
    for col in header:
        if col == "raison":
            continue
        val = (row.get(col) or "").strip()
        if val:
            values.append(val)

    suffix = ",".join(values)
    if suffix:
        return f"• {raison_prefix} : {suffix}"
    return f"• {raison_prefix}"


def _build_message(
    pipeline: str,
    run_tag: str,
    source_file: str,
    errors_header: list[str],
    errors_rows: list[dict[str, str]],
    anomalies_header: list[str],
    anomalies_rows: list[dict[str, str]],
) -> str:
    """
    Construire le message Slack multi-lignes selon les règles validées.

    Args:
        pipeline: Nom du pipeline.
        run_tag: Identifiant du run.
        source_file: Nom du fichier source.
        errors_header: En-têtes des erreurs.
        errors_rows: Données des erreurs.
        anomalies_header: En-têtes des anomalies.
        anomalies_rows: Données des anomalies.

    Returns:
        str: Le message complet prêt à l'envoi.
    """
    nb_err = len(errors_rows)
    nb_ano = len(anomalies_rows)
    statut = "FAILURE" if nb_err > 0 else "ANOMALIES"

    lines: list[str] = []
    lines.append(f"P12 | {pipeline} | {statut} | run={run_tag}")
    lines.append(f"Erreurs bloquantes: {nb_err} | Anomalies: {nb_ano}")
    lines.append(f"Fichier source: {source_file}")

    if nb_err > 0:
        lines.append("--- ERREURS BLOQUANTES ---")
        for r in errors_rows:
            lines.append(_row_to_message_line(r, errors_header))

    if nb_ano > 0:
        lines.append("--- ANOMALIES ---")
        if nb_ano < 10:
            for r in anomalies_rows:
                lines.append(_row_to_message_line(r, anomalies_header))
        else:
            lines.append(f"Anomalies: {nb_ano} (affichage limité à 3 exemples)")
            for r in anomalies_rows[:3]:
                lines.append(_row_to_message_line(r, anomalies_header))

    return "\n".join(lines)


# -------------------------------------------------------------------
# Action 05 - Interaction avec l'API Slack
# -------------------------------------------------------------------

def _post_slack(webhook_url: str, text: str, timeout_s: int = 30) -> None:
    """
    Envoyer un message Slack via webhook.

    Args:
        webhook_url: URL du webhook Slack.
        text: Corps du message.
        timeout_s: Timeout en secondes (défaut 30).

    Raises:
        RuntimeError: Si l'appel réseau échoue.
    """
    payload = {"text": text}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=webhook_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            _ = resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError) as exc:
        raise RuntimeError("L'envoi d'alerte à Slack a échoué") from exc


# -------------------------------------------------------------------
# Action 06 - Interaction Base de Données (PostgreSQL)
# -------------------------------------------------------------------

def _open_pg_connection() -> "psycopg.Connection":
    """
    Ouvrir une connexion PostgreSQL à partir des variables d'environnement.

    Returns:
        psycopg.Connection: Objet connexion actif.

    Raises:
        ValueError: Si des variables PG* sont manquantes ou invalides.
    """
    host = (os.getenv("PGHOST") or "localhost").strip()
    port_str = (os.getenv("PGPORT") or "5432").strip()
    user = (os.getenv("PGUSER") or "").strip()
    password = (os.getenv("PGPASSWORD") or "").strip()
    database = (os.getenv("PGDATABASE") or "").strip()

    if not user or not password or not database:
        raise ValueError("Variables manquantes : PGUSER / PGPASSWORD / PGDATABASE doivent être définies.")

    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"PGPORT invalide : {port_str}") from exc

    import psycopg  # import local
    return psycopg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
    )


def _write_metric(
    logger,
    pipeline_metric: str,
    date_debut_exe: datetime,
    statut: str,
    nb_err: int,
    nb_ano: int,
    tz_paris: ZoneInfo,
) -> None:
    """
    Écrire une métrique dans la table ops.run_metrique (opération bloquante).

    Args:
        logger: Instance du logger.
        pipeline_metric: Nom du pipeline pour la métrique.
        date_debut_exe: Timestamp de début.
        statut: Statut final (SUCCESS/FAILURE).
        nb_err: Nombre d'erreurs.
        nb_ano: Nombre d'anomalies.
        tz_paris: Timezone pour la date de fin.
    """
    from src.utils.logger import write_run_metric  # noqa: WPS433

    date_fin_exe = datetime.now(tz_paris)
    nb_lignes_lues = nb_err + nb_ano

    with _open_pg_connection() as conn:
        write_run_metric(
            conn=conn,
            nom_pipeline=pipeline_metric,
            date_debut_exe=date_debut_exe,
            date_fin_exe=date_fin_exe,
            statut=statut,
            nb_lignes_lues=nb_lignes_lues,
            nb_lignes_ecrites=0,
            nb_anomalies=nb_ano,
            logger=logger,
            raise_on_error=True,
        )


# -------------------------------------------------------------------
# Action 07 - Gestion de la ligne de commande (CLI)
# -------------------------------------------------------------------

def _detect_origin() -> str:
    """
    Détecter l'origine d'exécution (KESTRA ou CLI).

    Returns:
        str: "KESTRA" si variables Kestra présentes, sinon "CLI".
    """
    for key in ("KESTRA_FLOW_ID", "KESTRA_EXECUTION_ID", "KESTRA_NAMESPACE", "KESTRA_TASKRUN_ID"):
        if os.getenv(key):
            return "KESTRA"
    return "CLI"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Parser les arguments de la ligne de commande.

    Args:
        argv: Liste des arguments.

    Returns:
        argparse.Namespace: Arguments parsés.
    """
    parser = argparse.ArgumentParser(description="Publier un message Slack d’alerting (p12-alerting).")

    parser.add_argument(
        "--pipeline",
        default="norm_gsheet",
        help="Nom du pipeline/étape à afficher dans le message Slack.",
    )

    parser.add_argument(
        "--origin",
        choices=["CLI", "KESTRA"],
        default=_detect_origin(),
        help="Origine d’exécution (CLI / KESTRA). Par défaut : détection via l'environnement.",
    )

    parser.add_argument(
        "--log-level",
        default=os.getenv("P12_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO",
        help="Niveau de logs (DEBUG|INFO|WARNING|ERROR). Par défaut : P12_LOG_LEVEL ou LOG_LEVEL ou INFO.",
    )

    return parser.parse_args(argv)


# -------------------------------------------------------------------
# Action 08 - Fonction Principale
# -------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """
    Point d'entrée principal du script.

    Orchestre la détection de logs, la construction du message Slack,
    l'envoi et l'enregistrement de la métrique.

    Args:
        argv: Arguments optionnels (pour test).

    Returns:
        int: 0 en cas de succès, 1 en cas d'erreur.
    """
    repo_root = _find_repo_root()
    _ensure_project_imports(repo_root)

    # Chargement .env si local
    if load_dotenv is not None:
        dotenv_path = repo_root / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path=dotenv_path, override=False)

    # Initialisation logger
    get_logger = _get_logger()
    log_success = _log_success()
    log_failure = _log_failure()

    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = get_logger("p12_alerting", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo("Europe/Paris")
    date_debut_exe = datetime.now(tz_paris)
    pipeline_metric = f"{str(args.pipeline)}_alerting Slack"

    # Vérification Webhook
    webhook_url = (os.getenv("SLACK_ALERTING_URL") or "").strip()
    if not webhook_url:
        try:
            _write_metric(
                logger=logger,
                pipeline_metric=pipeline_metric,
                date_debut_exe=date_debut_exe,
                statut="FAILURE",
                nb_err=0,
                nb_ano=0,
                tz_paris=tz_paris,
            )
        except Exception as exc:  # noqa: BLE001
            log_failure(logger, message="Échec écriture métrique en base.", exc=exc, context={"pipeline": pipeline_metric})
            return 1

        log_failure(
            logger,
            message="Webhook Slack manquant.",
            exc=ValueError("SLACK_ALERTING_URL doit être défini."),
            context={"env_var": "SLACK_ALERTING_URL"},
        )
        return 1

    # Sélection des logs
    logs_dir = repo_root / "logs"
    run_files = _pick_latest_run(logs_dir)

    # Cas : Aucun run trouvé
    if run_files is None:
        try:
            _write_metric(
                logger=logger,
                pipeline_metric=pipeline_metric,
                date_debut_exe=date_debut_exe,
                statut="SUCCESS",
                nb_err=0,
                nb_ano=0,
                tz_paris=tz_paris,
            )
        except Exception as exc:  # noqa: BLE001
            log_failure(logger, message="Échec écriture métrique en base.", exc=exc, context={"pipeline": pipeline_metric})
            return 1

        log_success(
            logger,
            message="Aucun fichier erreurs/anomalies trouvé : aucune alerte à envoyer.",
            context={"logs_dir": str(logs_dir)},
        )
        return 0

    # Lecture des données
    errors_header: list[str] = []
    errors_rows: list[dict[str, str]] = []
    anomalies_header: list[str] = []
    anomalies_rows: list[dict[str, str]] = []

    if run_files.errors_path is not None:
        errors_header, errors_rows = _read_csv_rows(run_files.errors_path)

    if run_files.anomalies_path is not None:
        anomalies_header, anomalies_rows = _read_csv_rows(run_files.anomalies_path)

    nb_err = len(errors_rows)
    nb_ano = len(anomalies_rows)

    # Cas : Fichiers présents mais vides
    if nb_err == 0 and nb_ano == 0:
        try:
            _write_metric(
                logger=logger,
                pipeline_metric=pipeline_metric,
                date_debut_exe=date_debut_exe,
                statut="SUCCESS",
                nb_err=0,
                nb_ano=0,
                tz_paris=tz_paris,
            )
        except Exception as exc:  # noqa: BLE001
            log_failure(logger, message="Échec écriture métrique en base.", exc=exc, context={"pipeline": pipeline_metric})
            return 1

        log_success(
            logger,
            message="Fichiers présents mais vides : aucune alerte à envoyer.",
            context={
                "run_tag": run_files.run_tag,
                "errors_path": str(run_files.errors_path) if run_files.errors_path else None,
                "anomalies_path": str(run_files.anomalies_path) if run_files.anomalies_path else None,
            },
        )
        return 0

    # Construction et envoi du message
    source_file = _pick_latest_raw_filename(repo_root)

    text = _build_message(
        pipeline=str(args.pipeline),
        run_tag=run_files.run_tag,
        source_file=source_file,
        errors_header=errors_header,
        errors_rows=errors_rows,
        anomalies_header=anomalies_header,
        anomalies_rows=anomalies_rows,
    )

    try:
        _post_slack(webhook_url=webhook_url, text=text, timeout_s=30)
        
        try:
            _write_metric(
                logger=logger,
                pipeline_metric=pipeline_metric,
                date_debut_exe=date_debut_exe,
                statut="SUCCESS",
                nb_err=nb_err,
                nb_ano=nb_ano,
                tz_paris=tz_paris,
            )
        except Exception as exc_metric:  # noqa: BLE001
            log_failure(logger, message="Échec écriture métrique en base.", exc=exc_metric, context={"pipeline": pipeline_metric, "run_tag": run_files.run_tag})
            return 1

        log_success(
            logger,
            message="Alerte Slack envoyée.",
            context={"run_tag": run_files.run_tag, "errors": nb_err, "anomalies": nb_ano},
        )
        return 0

    except Exception as exc:  # noqa: BLE001
        # Echec envoi Slack => On loggue quand même une tentative en échec dans la base
        try:
            _write_metric(
                logger=logger,
                pipeline_metric=pipeline_metric,
                date_debut_exe=date_debut_exe,
                statut="FAILURE",
                nb_err=nb_err,
                nb_ano=nb_ano,
                tz_paris=tz_paris,
            )
        except Exception as exc_metric2:  # noqa: BLE001
            log_failure(logger, message="Échec écriture métrique en base.", exc=exc_metric2, context={"pipeline": pipeline_metric, "run_tag": run_files.run_tag})
            return 1

        log_failure(
            logger,
            message="L'envoi d'alerte à Slack a échoué",
            exc=exc,
            context={"run_tag": run_files.run_tag},
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
