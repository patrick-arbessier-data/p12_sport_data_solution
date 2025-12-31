# -------------------------------------------------------------------
# Action 0 - Objectif du script
# -------------------------------------------------------------------
"""
Chemin :
- soda/run_soda_docker.py

Commande CLI (depuis la racine du repo) :
- python soda/run_soda_docker.py

Objectif :
- Exécuter des scans Soda dans un conteneur Docker (Soda Core),
  en s'appuyant sur les fichiers du repo (config + checks).
- Produire des logs projet homogènes via src.utils.logger.
- Laisser Soda écrire ses résultats directement sur stdout (pas d'interception).

Contraintes :
- Les secrets/paramètres Postgres ne sont pas codés en dur : ils viennent des variables d'environnement.
- En cas d'échec d'un scan Soda : retour non-zéro (le pipeline doit échouer).
"""
# -------------------------------------------------------------------

from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from typing import List, Sequence

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


# -------------------------------------------------------------------
# Action 1 - Imports projet (logger)
# -------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from src.utils.logger import get_logger, log_failure, log_success
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Imports projet impossibles. Vérifier que la racine du repo est dans PYTHONPATH "
        "et que 'src' est un package importable (présence de __init__.py)."
    ) from exc


# -------------------------------------------------------------------
# Action 2 - Configuration
# -------------------------------------------------------------------

DEFAULT_SODA_IMAGE = "sodadata/soda-core:v3.0.32"
DEFAULT_DATASOURCE = "sportdata"

SODA_CONFIG = Path("soda/config/ds_postgres.yml")
DEFAULT_CHECKS: Sequence[Path] = (
    Path("soda/checks/metier_salarie.yml"),
    Path("soda/checks/metier_activite.yml"),
    Path("soda/checks/transverse_coherence_bi.yml"),
)

ENV_REQUIRED = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")


# -------------------------------------------------------------------
# Action 3 - Helpers (env, validation, commande docker)
# -------------------------------------------------------------------

def _origin() -> str:
    """
    Déterminer une origine d'exécution stable.
    - CLI par défaut
    - surcharge possible via P12_ORIGIN (ex: "KESTRA")
    """
    return os.getenv("P12_ORIGIN", "CLI")


def _load_env(repo_root: Path) -> None:
    """
    Charger .env si python-dotenv est disponible.
    But : permettre une exécution CLI sans export manuel des variables.
    """
    env_path = repo_root / ".env"
    if load_dotenv is None:
        return
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)


def _require_env(logger, key: str) -> str:
    """
    Récupérer une variable d'environnement requise.
    Échec immédiat si absente/vidée.
    """
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Variable d'environnement {key} absente.")
    return value.strip()


def _resolve_paths(repo_root: Path, config_rel: Path, checks_rel: Sequence[Path]) -> tuple[Path, List[Path]]:
    """
    Résoudre les chemins repo -> absolus (host) et valider l'existence.
    """
    config_path = (repo_root / config_rel).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Fichier config Soda manquant : {config_path}")

    checks_abs: List[Path] = []
    for p in checks_rel:
        abs_p = (repo_root / p).resolve()
        if not abs_p.exists():
            raise FileNotFoundError(f"Fichier checks Soda manquant : {abs_p}")
        checks_abs.append(abs_p)

    return config_path, checks_abs


def _docker_scan_cmd(
    *,
    repo_root: Path,
    soda_image: str,
    datasource: str,
    config_host: Path,
    checks_host: Path,
    pg_host: str,
    pg_port: str,
) -> List[str]:
    """
    Construire une commande docker run qui exécute un scan Soda.
    Important : on laisse stdout/stderr à Soda (pas de capture).
    """
    # Mapping repo -> /sodacl (référence stable côté conteneur)
    volume = f"{str(repo_root.resolve())}:/sodacl"

    # Chemins attendus côté conteneur (miroir du repo)
    config_in_container = f"/sodacl/{SODA_CONFIG.as_posix()}"
    checks_in_container = f"/sodacl/{Path('soda/checks') / checks_host.name}".replace("\\", "/")

    return [
        "docker",
        "run",
        "--rm",
        # On s'appuie sur les variables déjà chargées côté host (via .env en CLI, ou env Kestra).
        "-e",
        f"PGHOST={pg_host}",
        "-e",
        f"PGPORT={pg_port}",
        "-e",
        f"PGUSER={os.getenv('PGUSER', '')}",
        "-e",
        f"PGPASSWORD={os.getenv('PGPASSWORD', '')}",
        "-e",
        f"PGDATABASE={os.getenv('PGDATABASE', '')}",
        "-v",
        volume,
        soda_image,
        "scan",
        "-d",
        datasource,
        "-c",
        config_in_container,
        checks_in_container,
    ]

def _pg_host_for_container(pg_host: str) -> str:
    """
    Adapter PGHOST pour un conteneur Docker :
    - localhost / 127.0.0.1 doivent devenir host.docker.internal
    """
    host = (pg_host or "").strip()
    if host in ("localhost", "127.0.0.1"):
        return "host.docker.internal"
    return host


# -------------------------------------------------------------------
# Action 4 - Main
# -------------------------------------------------------------------

def main() -> int:
    logger = get_logger("soda", origin=_origin(), level=os.getenv("LOG_LEVEL", "INFO"))

    try:
        _load_env(REPO_ROOT)

        # Variables attendues (fail fast si absent)
        for k in ENV_REQUIRED:
            _require_env(logger, k)

        pg_host = os.getenv("PGHOST", "").strip()
        pg_port = os.getenv("PGPORT", "").strip()
        pg_host_container = _pg_host_for_container(pg_host)
        pg_db = os.getenv("PGDATABASE", "").strip()

        soda_image = os.getenv("SODA_IMAGE", DEFAULT_SODA_IMAGE)
        datasource = os.getenv("SODA_DATASOURCE", DEFAULT_DATASOURCE)

        config_path, checks_paths = _resolve_paths(REPO_ROOT, SODA_CONFIG, DEFAULT_CHECKS)

        logger.info(
            "Démarrage Soda (Docker) | image=%s | datasource=%s | config=%s | checks=%s | cible=%s:%s/%s",
            soda_image,
            datasource,
            str(config_path),
            ", ".join([str(p) for p in checks_paths]),
            pg_host_container,
            pg_port,
            pg_db,
        )
        # Exécution séquentielle : un scan par fichier checks (rc non-zéro => échec pipeline)
        for i, checks_file in enumerate(checks_paths, start=1):
            logger.info("Scan %s/%s : %s", i, len(checks_paths), str(checks_file))

            cmd = _docker_scan_cmd(
                repo_root=REPO_ROOT,
                soda_image=soda_image,
                datasource=datasource,
                config_host=config_path,
                checks_host=checks_file,
                pg_host=pg_host_container,
                pg_port=pg_port,
            )

            # Laisser Soda écrire sur stdout/stderr directement
            completed = subprocess.run(cmd, check=False)
            if completed.returncode != 0:
                raise RuntimeError(f"Soda scan en échec (rc={completed.returncode}) sur {checks_file}")

        log_success(logger, "Soda : scans terminés avec succès.")
        return 0

    except Exception as exc:
        log_failure(logger, exc, "Soda : échec des scans.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
