# -------------------------------------------------
# mod20_recup_gsheet.py
# -------------------------------------------------

"""
Télécharger un Google Sheet public via une URL d'export CSV (variable d'environnement)
et déposer le fichier dans data/raw/ avec un nom timé : YYYYMMDD_HHMMSS_decla_sheet.csv

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\extract\\mod20_recup_gsheet.py

Arguments
---------
--url : URL d'export CSV du Google Sheet (Défaut : variable d'env URL_GSHEET).
--out-dir : répertoire de sortie (Défaut : <repo>/data/raw).
--timeout : Timeout HTTP en secondes (Défaut : 30).
--origin : origine d'exécution (Défaut : CLI).
--log-level : niveau de logs (Défaut : INFO).

Objectifs
---------
- Télécharger et stocker le CSV source des déclaratifs.
- Éviter la redondance : ne créer un nouveau fichier que si le contenu a changé par rapport au dernier téléchargement.
- Signaler l'état "inchangé" via un fichier marqueur (_decla_sheet_unchanged.flag).
- Assurer la robustesse du téléchargement (retries) et la validité du fichier (Content-Type).

Entrées
-------
- Variables d'environnement :
    URL_GSHEET (URL cible)
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (Connexion métriques)

- Fichiers (comparaison) :
    data/raw/*_decla_sheet.csv (dernier fichier existant)

Sorties
-------
- Fichiers :
    data/raw/YYYYMMDD_HHMMSS_decla_sheet.csv (Nouveau fichier si changement)
    data/raw/_decla_sheet_unchanged.flag (Marqueur si aucun changement)

- Métriques :
    ops.run_metrique (1 ligne)

Traitements & fonctionnalités
-----------------------------
- Nettoyage préventif du marqueur d'état précédent.
- Téléchargement HTTP avec **Retry automatique** (3 tentatives) en cas d'échec réseau.
- Validation stricte de la réponse :
    - Code HTTP 200.
    - Content-Type compatible CSV/Text (rejet si HTML).
    - Contenu non vide.
- Normalisation du contenu et calcul SHA256.
- Comparaison avec l'existant (Hash) pour décision (NO_CHANGE vs SUCCESS).

Contraintes
-----------
- L'URL doit pointer vers un export CSV public valide.
- Le système de fichiers doit être accessible en écriture.

Observations & remarques
------------------------
- Le script utilise `urllib` standard mais implémente une logique de résilience avancée.
- Les erreurs de type "Quota Exceeded" (HTML) sont désormais détectées et rejetées proprement.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import hashlib
import inspect
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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
TZ_NAME = "Europe/Paris"
UNCHANGED_FLAG_FILENAME = "_decla_sheet_unchanged.flag"
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # Secondes (exponential backoff)


@dataclass(frozen=True)
class DownloadResult:
    """Résultat d'un téléchargement / comparaison."""
    output_path: Path
    bytes_written: int
    nb_lignes_lues: int
    nb_lignes_ecrites: int
    is_unchanged: bool


def _find_repo_root(start_path: Path) -> Path:
    """Remonter jusqu'à trouver un répertoire contenant 'src' et 'data'."""
    current = start_path
    for _ in range(10):
        if (current / "src").exists() and (current / "data").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Racine du dépôt introuvable (attendu : dossiers 'src' et 'data').")


def _ensure_project_imports(repo_root: Path) -> None:
    """S'assurer que l'import 'src.*' est possible."""
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


# -------------------------------------------------------------------
# Action 02 - Helpers Date & Hash
# -------------------------------------------------------------------
def _now_paris() -> datetime:
    """Retourner un datetime timezone-aware en Europe/Paris."""
    return datetime.now(ZoneInfo(TZ_NAME))


def _timestamp_paris() -> str:
    """Retourner un timestamp au format YYYYMMDD_HHMMSS en timezone Europe/Paris."""
    return _now_paris().strftime("%Y%m%d_%H%M%S")


def _strip_header_and_normalize_newlines(raw: bytes) -> bytes:
    """
    Retirer la première ligne (entête) et normaliser les sauts de ligne.
    Objectif : comparer le contenu "métier" sans être perturbé par des variations d'entête ou CRLF.
    """
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) <= 1:
        return b""
    payload = "\n".join(lines[1:]) + "\n"
    return payload.encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    """Calculer le SHA256 hex d'un payload binaire."""
    return hashlib.sha256(payload).hexdigest()  # noqa: S324


# -------------------------------------------------------------------
# Action 03 - Gestion Fichiers
# -------------------------------------------------------------------
def _find_latest_decla_csv(output_dir: Path) -> Path | None:
    """Retourner le dernier fichier '*_decla_sheet.csv' du répertoire, ou None si absent."""
    candidates = sorted(output_dir.glob("*_decla_sheet.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _remove_unchanged_flag(output_dir: Path) -> None:
    """Supprimer le fichier marqueur d'inchangé si présent."""
    flag_path = output_dir / UNCHANGED_FLAG_FILENAME
    if flag_path.exists():
        flag_path.unlink()


def _create_unchanged_flag(output_dir: Path) -> None:
    """Créer le fichier marqueur d'inchangé."""
    flag_path = output_dir / UNCHANGED_FLAG_FILENAME
    flag_path.write_text("NO_CHANGE\n", encoding="utf-8")


# -------------------------------------------------------------------
# Action 04 - Téléchargement Robuste & Validé
# -------------------------------------------------------------------
def _download_with_retry(url: str, timeout_s: int, logger: Any) -> bytes:
    """
    Télécharger le contenu avec retry et validation de contenu.
    """
    last_exc = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310
                # 1. Validation Content-Type
                ctype = resp.headers.get("Content-Type", "").lower()
                # On accepte text/csv, text/plain, ou application/octet-stream, mais on rejette explicitement html
                if "html" in ctype:
                    raise ValueError(f"Type de contenu invalide détecté (HTML). Attendait CSV. Content-Type: {ctype}")
                
                raw = resp.read()
                
                # 2. Validation Contenu vide
                if not raw:
                    raise ValueError("Contenu téléchargé vide.")

                # 3. Validation Structure (Anti-HTML d'erreur masqué)
                text_sample = raw[:2048].decode("utf-8", errors="replace").lower()
                if "<html" in text_sample or "<body" in text_sample:
                    raise ValueError("Le contenu ressemble à du HTML (balises détectées) malgré le Content-Type.")
                
                if ("," not in text_sample) and (";" not in text_sample):
                    raise ValueError("Le contenu ne ressemble pas à un CSV (aucun séparateur ',' ou ';' détecté).")
                
                return raw

        except (urllib.error.URLError, ValueError) as exc:
            last_exc = exc
            wait_time = RETRY_DELAY_BASE ** attempt
            if attempt < MAX_RETRIES:
                logger.warning("Échec tentative %d/%d : %s. Nouvelle tentative dans %ds...", attempt, MAX_RETRIES, exc, wait_time)
                time.sleep(wait_time)
            else:
                logger.error("Échec définitif après %d tentatives.", MAX_RETRIES)

    raise last_exc if last_exc else RuntimeError("Échec téléchargement inconnu")


def _process_download_logic(url: str, output_dir: Path, timeout_s: int, logger: Any) -> DownloadResult:
    """
    Orchestrer le téléchargement, la comparaison et l'écriture.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    _remove_unchanged_flag(output_dir)

    # Téléchargement robuste
    raw = _download_with_retry(url, timeout_s, logger)

    # Traitement
    payload = _strip_header_and_normalize_newlines(raw)
    new_hash = _sha256_bytes(payload)
    latest_path = _find_latest_decla_csv(output_dir)

    # Comparaison
    if latest_path is not None:
        latest_raw = latest_path.read_bytes()
        latest_payload = _strip_header_and_normalize_newlines(latest_raw)
        old_hash = _sha256_bytes(latest_payload)

        if new_hash == old_hash:
            _create_unchanged_flag(output_dir)
            nb_lignes = 0 if not payload else payload.decode("utf-8", errors="replace").count("\n")
            return DownloadResult(
                output_path=latest_path,
                bytes_written=0,
                nb_lignes_lues=nb_lignes,
                nb_lignes_ecrites=nb_lignes,
                is_unchanged=True,
            )

    # Écriture nouveau fichier
    out_path = output_dir / f"{_timestamp_paris()}_decla_sheet.csv"
    out_path.write_bytes(raw)
    
    nb_lignes = 0 if not payload else payload.decode("utf-8", errors="replace").count("\n")
    
    return DownloadResult(
        output_path=out_path,
        bytes_written=len(raw),
        nb_lignes_lues=nb_lignes,
        nb_lignes_ecrites=nb_lignes,
        is_unchanged=False,
    )


# -------------------------------------------------------------------
# Action 05 - Connexion DB
# -------------------------------------------------------------------
def _open_pg_connection():
    """Ouvrir une connexion PostgreSQL via les variables d'environnement."""
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

    import psycopg  # noqa: WPS433
    return psycopg.connect(host=host, port=port, user=user, password=password, dbname=database)


# -------------------------------------------------------------------
# Action 06 - CLI
# -------------------------------------------------------------------
def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mod20_recup_gsheet",
        description="Téléchargement Google Sheet (CSV) -> data/raw/ et métrique ops.run_metrique.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("URL_GSHEET"),
        help="URL d'export CSV du Google Sheet.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Répertoire de sortie. Par défaut : /data/raw/",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP (secondes). Par défaut : 30.",
    )
    parser.add_argument(
        "--origin",
        default="CLI",
        help="Origine d'exécution (ex: CLI).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Niveau de logs (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args(argv)


# -------------------------------------------------------------------
# Action 07 - Main
# -------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    """Point d'entrée principal."""
    date_debut_exe = _now_paris()
    
    repo_root = _find_repo_root(Path(__file__).resolve())
    _ensure_project_imports(repo_root)

    if load_dotenv is None:
        raise RuntimeError(
            "python-dotenv n'est pas installé : impossible de charger le fichier .env en exécution CLI."
        )

    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)

    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = get_logger("mod20_recup_gsheet", origin=str(args.origin), level=str(args.log_level))

    out_dir = Path(args.out_dir) if args.out_dir else (repo_root / "data" / "raw")

    logger.info("Démarrage extraction Google Sheet")
    logger.info("Paramètres | url=%s | out_dir=%s | timeout=%ss", args.url, out_dir, args.timeout)

    date_fin_exe: datetime | None = None
    statut = "FAILED_EXCEPTION"
    nb_lignes_lues = 0
    nb_lignes_ecrites = 0

    try:
        # Exécution avec logique de retry intégrée
        result = _process_download_logic(url=str(args.url), output_dir=out_dir, timeout_s=int(args.timeout), logger=logger)
        
        date_fin_exe = _now_paris()
        statut = "NO_CHANGE" if result.is_unchanged else "SUCCESS"
        nb_lignes_lues = int(result.nb_lignes_lues)
        nb_lignes_ecrites = int(result.nb_lignes_ecrites)

        if result.is_unchanged:
            _safe_log_success(
                logger,
                message="déclaratifs inchangés par rapport au dernier fichier téléchargé",
                output_path=str(result.output_path),
            )
            return 0

        _safe_log_success(
            logger,
            message="Google Sheet récupéré et écrit.",
            output_path=str(result.output_path),
            bytes_written=result.bytes_written,
        )
        return 0

    except Exception as exc:  # noqa: BLE001
        statut = "FAILED_EXCEPTION"
        nb_lignes_lues = 0
        nb_lignes_ecrites = 0
        
        _safe_log_failure(
            logger,
            exc,
            message="Échec récupération Google Sheet.",
            url=str(args.url),
            out_dir=str(out_dir),
        )
        return 1

    finally:
        if date_fin_exe is None:
            date_fin_exe = _now_paris()

        try:
            with _open_pg_connection() as conn:
                write_run_metric(
                    conn=conn,
                    nom_pipeline="mod20_recup_gsheet",
                    date_debut_exe=date_debut_exe,
                    date_fin_exe=date_fin_exe,
                    statut=statut,
                    nb_lignes_lues=int(nb_lignes_lues),
                    nb_lignes_ecrites=int(nb_lignes_ecrites),
                    nb_anomalies=0,
                    logger=logger,
                    raise_on_error=True,
                )
                conn.commit()
        except Exception as metric_exc:  # noqa: BLE001
            logger.error("Impossible d'écrire la métrique : %s", metric_exc)


if __name__ == "__main__":
    raise SystemExit(main())
