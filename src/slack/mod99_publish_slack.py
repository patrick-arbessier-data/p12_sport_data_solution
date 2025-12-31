#-------------------------------------------------
# mod99_publish_slack.py
#-------------------------------------------------
"""
# Publier les activités validées sur Slack via Webhook.


Commande d'exécution
--------------------
python src/slack/mod99_publish_slack.py

Arguments
---------
Aucun argument CLI direct. Configuration via variables d'environnement.

Objectifs
---------
- Lire le dernier fichier GSheet traité (data/processed).
- Optimisation : Récupérer en masse (Bulk) les infos DB pour toutes les lignes du CSV.
- Publier un message Slack de félicitations formaté selon le type d'activité.
- Gérer l'idempotence via le flag metier.activite.flag_slack.
- Fallback : si FORCE_WEBHOOK=1 et rien à publier, republier les 10 dernières.

Entrées
-------
- Fichiers :
	data/processed/*_declaratif_activites_gsheet.csv
	.env

- Tables :
	metier.activite
	sec.lien_salarie
	sec.rh_salarie

- Variables d'environnement :
	SLACK_WEBHOOK_URL, FORCE_WEBHOOK, PG*

Sorties
-------
- Slack : Messages publiés.
- Tables : metier.activite (UPDATE flag_slack), ops.run_metrique.

Traitements & fonctionnalités
-----------------------------
- Chargement CSV en mémoire (liste).
- Requête SQL unique pour récupérer l'état DB de toutes les lignes (clé composite).
- Boucle de traitement et publication Slack.
- Mise à jour DB (commit par lot ou unitaire selon logique).

Contraintes
-----------
- Idempotence stricte.
- Performance : Minimiser les allers-retours DB (Pattern N+1 supprimé).

Observations & remarques
------------------------
- Version optimisée pour réduire la latence réseau DB.

"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import csv
import inspect
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple, Dict, List
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import psycopg

# -------------------------------------------------------------------
# Action 01 - Configuration globale et constantes
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FORCE_FALLBACK_LIMIT = 10
TZ_PARIS = ZoneInfo("Europe/Paris")
NOM_PIPELINE = "mod99_publish_slack"


# -------------------------------------------------------------------
# Action 02 - Utilitaires système
# -------------------------------------------------------------------

def _add_project_root_to_syspath() -> None:
    """Ajouter la racine projet au sys.path."""
    root = str(PROJECT_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

_add_project_root_to_syspath()

try:
    from src.utils.logger import get_logger, log_failure, log_success, write_run_metric  # type: ignore
except Exception:
    from utils.logger import get_logger, log_failure, log_success, write_run_metric  # type: ignore


# -------------------------------------------------------------------
# Action 03 - Gestion de l'environnement
# -------------------------------------------------------------------

def _load_dotenv_if_present(project_root: Path) -> None:
    """Charger un .env si présent."""
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    raw = env_path.read_text(encoding="utf-8")
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value

def _clean_env_value(value: str) -> str:
    v = str(value or "").strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v

def _require_env(name: str) -> str:
    value = _clean_env_value(os.getenv(name, ""))
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value

def _env_flag_is_one(name: str) -> bool:
    return _clean_env_value(os.getenv(name, "")) == "1"

def _detect_origin() -> str:
    forced = _clean_env_value(os.getenv("ORIGIN", ""))
    if forced: return forced
    if any(k.startswith("KESTRA_") for k in os.environ): return "KESTRA"
    if Path("/workspace").exists(): return "KESTRA"
    return "CLI"


# -------------------------------------------------------------------
# Action 04 - Parsing et Structures de données
# -------------------------------------------------------------------

@dataclass(frozen=True)
class ActiviteRow:
    """Ligne brute du CSV."""
    cle_salarie: str
    date_debut: datetime
    duree_sec: int
    distance_m: int | None
    type_activite: str
    commentaire: str
    source_donnee: str

@dataclass
class DbActivityInfo:
    """Infos récupérées de la DB pour une activité."""
    id_activite: int
    flag_slack: bool | None
    prenom: str
    nom: str

def _parse_datetime_tz(value: str) -> datetime:
    v = (value or "").strip()
    if not v: raise ValueError("date_debut vide")
    if v.endswith("Z"): v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _parse_int_like(value: str, required: bool) -> int | None:
    v = (value or "").strip()
    if not v:
        if required: raise ValueError("entier manquant")
        return None
    v = v.replace(",", ".")
    if re.fullmatch(r"\d+\.0+", v):
        v = v.split(".", 1)[0]
    return int(float(v))

def _has_header_row(first_row: list[str]) -> bool:
    lowered = [c.strip().lower() for c in first_row]
    needed = {"cle_salarie", "date_debut", "duree_sec", "distance_m", "type_activite"}
    return needed.issubset(set(lowered))

def _parse_row_dict(r: dict[str, str]) -> Optional[ActiviteRow]:
    def _get(name: str) -> str:
        for k, v in r.items():
            if k.strip().lower() == name: return str(v or "")
        return ""
    try:
        cle_salarie = _get("cle_salarie").strip()
        date_debut = _parse_datetime_tz(_get("date_debut"))
        duree_sec = int(_parse_int_like(_get("duree_sec"), required=True) or 0)
        distance_raw = _get("distance_m").strip()
        distance_m = _parse_int_like(distance_raw, required=False) if distance_raw else None
        type_activite = _get("type_activite").strip()
        commentaire = _get("commentaire").strip()
        source_donnee = _get("source_donnee").strip() or "csv_gsheet"
        
        if not cle_salarie or not type_activite: raise ValueError("cle/type vides")
        
        return ActiviteRow(
            cle_salarie=cle_salarie, date_debut=date_debut, duree_sec=duree_sec,
            distance_m=distance_m, type_activite=type_activite,
            commentaire=commentaire, source_donnee=source_donnee
        )
    except Exception:
        return None

def _parse_row_list(cells: list[str]) -> Optional[ActiviteRow]:
    try:
        cle_salarie = str(cells[0]).strip()
        date_debut = _parse_datetime_tz(str(cells[1]))
        duree_sec = int(_parse_int_like(str(cells[2]), required=True) or 0)
        distance_raw = str(cells[3]).strip()
        distance_m = _parse_int_like(distance_raw, required=False) if distance_raw else None
        type_activite = str(cells[4]).strip()
        commentaire = str(cells[5]).strip() if len(cells) > 5 else ""
        source_donnee = str(cells[6]).strip() if len(cells) > 6 else "csv_gsheet"
        source_donnee = source_donnee or "csv_gsheet"
        
        if not cle_salarie or not type_activite: raise ValueError("cle/type vides")

        return ActiviteRow(
            cle_salarie=cle_salarie, date_debut=date_debut, duree_sec=duree_sec,
            distance_m=distance_m, type_activite=type_activite,
            commentaire=commentaire, source_donnee=source_donnee
        )
    except Exception:
        return None

# -------------------------------------------------------------------
# Action 04-a - Localisation des données sources
# -------------------------------------------------------------------

def _find_latest_gsheet_csv(project_root: Path) -> Optional[Path]:
    """Trouver le dernier fichier *_declaratif_activites_gsheet.csv dans data/processed/."""
    folder = project_root / "data" / "processed"
    if not folder.exists():
        return None

    candidates = list(folder.glob("*_declaratif_activites_gsheet.csv"))
    if not candidates:
        return None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def _load_all_csv_rows(csv_path: Path, logger: Any) -> List[ActiviteRow]:
    """Lire tout le CSV en une fois et retourner les objets valides."""
    rows: List[ActiviteRow] = []
    
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader_peek = csv.reader(f, delimiter=",")
        try:
            first = next(reader_peek)
        except StopIteration:
            return []

    has_header = _has_header_row(first)

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        if has_header:
            reader = csv.DictReader(f, delimiter=",")
            for idx, r in enumerate(reader, start=1):
                row_obj = _parse_row_dict(r)
                if row_obj: rows.append(row_obj)
                else: logger.warning(f"Ligne {idx} ignorée (parsing).")
        else:
            reader = csv.reader(f, delimiter=",")
            for idx, cells in enumerate(reader, start=1):
                row_obj = _parse_row_list(cells)
                if row_obj: rows.append(row_obj)
                else: logger.warning(f"Ligne {idx} ignorée (parsing).")
    return rows


# -------------------------------------------------------------------
# Action 05 - Interaction DB
# -------------------------------------------------------------------

def _fetch_db_infos_bulk(conn: Any, rows: List[ActiviteRow]) -> Dict[Tuple[str, datetime], DbActivityInfo]:
    """
    Récupérer les infos DB pour une liste d'activités en une seule requête.
    CORRECTIF : Utilisation de tuples aplatis pour contourner l'erreur 'anonymous composite types'.
    """
    if not rows:
        return {}
    
    # On filtre les rows pertinentes
    target_rows = [r for r in rows if r.source_donnee == 'csv_gsheet']
    if not target_rows:
        return {}
    
    # Construction de la liste des paramètres à plat : [cle1, date1, cle2, date2, ...]
    flat_params = []
    for r in target_rows:
        flat_params.append(r.cle_salarie)
        flat_params.append(r.date_debut)
        
    # Construction de la clause IN ((%s, %s), (%s, %s), ...)
    # On génère autant de placeholders (%s, %s) qu'il y a de lignes
    placeholders = ",".join(["(%s, %s)"] * len(target_rows))

    with conn.cursor() as cur:
        query = f"""
            SELECT 
                a.cle_salarie,
                a.date_debut,
                a.id_activite, 
                a.flag_slack,
                rs.prenom,
                rs.nom
            FROM metier.activite a
            JOIN sec.lien_salarie ls ON ls.cle_salarie = a.cle_salarie
            JOIN sec.rh_salarie rs ON rs.id_salarie_brut = ls.id_salarie_brut
            WHERE a.source_donnee = 'csv_gsheet'
            AND (a.cle_salarie, a.date_debut) IN ({placeholders})
        """
        # Exécution avec la liste aplatie des paramètres
        cur.execute(query, flat_params) 
        results = cur.fetchall()
    
    mapping = {}
    for row in results:
        # row: cle, date, id, flag, prenom, nom
        k = (str(row[0]), row[1]) # Clé (cle_salarie, date_debut)
        info = DbActivityInfo(
            id_activite=row[2],
            flag_slack=row[3],
            prenom=str(row[4] or "").strip(),
            nom=str(row[5] or "").strip()
        )
        mapping[k] = info
    return mapping


# -------------------------------------------------------------------
# Action 06 - Message Slack & Envoi
# -------------------------------------------------------------------

def _format_distance_km(distance_m: int | None) -> str:
    if distance_m is None: return "distance inconnue"
    return f"{float(distance_m)/1000.0:.1f} km"

def _format_duree(duree_sec: int) -> str:
    if duree_sec <= 0: return "durée inconnue"
    return f"{int(round(duree_sec/60.0))} min"

def _build_message(prenom: str, nom: str, type_activite: str, distance_km: str, duree_txt: str, commentaire: str) -> str:
    full_name = f"{prenom} {nom}".strip()
    comment_txt = f" ({commentaire.strip()})" if commentaire.strip() else ""
    
    sports_endurance = {"Randonnée", "Runing", "Triathlon", "Natation"}
    if type_activite in sports_endurance and (distance_km is None or "inconnue" in distance_km):
        print(f"WARNING: Distance manquante (cle_salarie={full_name}).")

    if type_activite == "Randonnée":
        dist = distance_km if distance_km else "distance non renseignée"
        return f"Magnifique {full_name} ! Une randonnée de {dist} terminée en {duree_txt}, Félicitations !{comment_txt} 🏕"
    
    if type_activite == "Runing":
        dist = distance_km if distance_km else "distance non renseignée"
        return f"Bravo {full_name} ! Tu viens de courir {dist} en {duree_txt} ! Belle énergie !{comment_txt} 🔥🏅"

    if type_activite == "Triathlon":
        dist = distance_km if distance_km else "distance non renseignée"
        return f"Fantastique {full_name} ! Performance haut niveau : {duree_txt} sur {dist}. Endurance !{comment_txt} 🏅"

    if type_activite == "Natation":
        dist = distance_km if distance_km else "distance non renseignée"
        return f"Grandiose {full_name} ! Nager pendant {duree_txt}, parcours {dist}. Comme un poisson !{comment_txt} 🔥"

    return f"Félicitations {full_name} ! Discipline {type_activite} pratiquée pendant {duree_txt}. Bravo !{comment_txt} 🏅"

def _slack_post(webhook_url: str, text: str) -> None:
    payload = {"text": text}
    body = json.dumps(payload).encode("utf-8")
    req = Request(webhook_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=10) as resp: _ = resp.read()
    except (HTTPError, URLError) as exc:
        raise RuntimeError(f"Erreur Slack : {exc}") from exc


# -------------------------------------------------------------------
# Action 07 - Fallback & Metrics
# -------------------------------------------------------------------

def _fetch_last_activities_fallback(conn: Any, limit: int) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.id_activite, rs.prenom, rs.nom, a.type_activite, a.duree_sec, a.distance_m, COALESCE(a.commentaire, '')
            FROM metier.activite a
            JOIN sec.lien_salarie ls ON ls.cle_salarie = a.cle_salarie
            JOIN sec.rh_salarie rs ON rs.id_salarie_brut = ls.id_salarie_brut
            WHERE a.source_donnee = 'csv_gsheet'
            ORDER BY a.date_debut DESC, a.id_activite DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()

def _write_metric_safe(conn: Any, logger: Any, start: datetime, status: str, lues: int, ecrites: int, ano: int):
    if not conn: return
    try:
        write_run_metric(
            conn=conn, nom_pipeline=NOM_PIPELINE,
            date_debut_exe=start, date_fin_exe=datetime.now(TZ_PARIS),
            statut=status, nb_lignes_lues=lues, nb_lignes_ecrites=ecrites,
            nb_anomalies=ano, logger=logger
        )
    except Exception: pass

def _log_s(logger: Any, msg: str, ctx: dict):
    if "context" in inspect.signature(log_success).parameters: log_success(logger, message=msg, context=ctx)
    else: log_success(logger, message=msg, **ctx)

def _log_f(logger: Any, msg: str, exc: Exception, ctx: dict):
    if "context" in inspect.signature(log_failure).parameters: log_failure(logger, message=msg, exc=exc, context=ctx)
    else: log_failure(logger, message=msg, exc=exc, **ctx)


# -------------------------------------------------------------------
# Action 08 - Main
# -------------------------------------------------------------------

def main() -> int:
    _load_dotenv_if_present(PROJECT_ROOT)
    logger = get_logger(script="mod99_publish_slack", origin=_detect_origin())
    start_time = datetime.now(tz=TZ_PARIS)
    
    nb_anomalies = 0
    published_count = 0
    
    try:
        webhook = _require_env("SLACK_WEBHOOK_URL")
        pg_conf = {k: _require_env(v) for k, v in [
            ("host", "PGHOST"), ("port", "PGPORT"), ("dbname", "PGDATABASE"),
            ("user", "PGUSER"), ("password", "PGPASSWORD")
        ]}
        pg_conf["port"] = int(pg_conf["port"])
        force_webhook = _env_flag_is_one("FORCE_WEBHOOK")

        # 1. Lecture CSV unique
        csv_path = _find_latest_gsheet_csv(PROJECT_ROOT)
        rows = []
        if csv_path:
            rows = _load_all_csv_rows(csv_path, logger)
        
        nb_lues = len(rows)

        with psycopg.connect(**pg_conf) as conn:
            conn.execute("SET TIME ZONE 'Europe/Paris';")
            
            # 2. Récupération Bulk DB
            # On récupère l'état de toutes les lignes en 1 requête
            db_map = _fetch_db_infos_bulk(conn, rows) if rows else {}

            # 3. Traitement en mémoire
            for r in rows:
                if r.source_donnee != "csv_gsheet": continue
                
                # Lookup mémoire immédiat
                info = db_map.get((r.cle_salarie, r.date_debut))
                
                if not info:
                    continue # Activité pas en base, on skip
                
                if (info.flag_slack is True) and (not force_webhook):
                    continue # Déjà publié

                # Construction Message
                msg = _build_message(
                    info.prenom, info.nom, r.type_activite,
                    _format_distance_km(r.distance_m), _format_duree(r.duree_sec), r.commentaire
                )

                # Envoi Slack + Update unitaire
                # Note: On garde l'update unitaire pour la sécurité transactionnelle item par item
                try:
                    _slack_post(webhook, msg)
                    with conn.cursor() as cur:
                        cur.execute("UPDATE metier.activite SET flag_slack = TRUE WHERE id_activite = %s", (info.id_activite,))
                    conn.commit()
                    published_count += 1
                except Exception:
                    nb_anomalies += 1
                    # On continue pour les autres lignes même si une plante Slack

            # 4. Fallback si rien publié
            if published_count == 0 and force_webhook:
                logger.info(f"Fallback FORCE: republier {FORCE_FALLBACK_LIMIT} dernières.")
                fb_rows = _fetch_last_activities_fallback(conn, FORCE_FALLBACK_LIMIT)
                
                if not fb_rows:
                    _log_s(logger, "FORCE_WEBHOOK: rien à republier.", {"rows": 0})
                else:
                    for row in fb_rows:
                        # row: id, prenom, nom, type, duree, dist, comm
                        msg = _build_message(row[1], row[2], row[3], _format_distance_km(row[5]), _format_duree(row[4]), row[6])
                        try:
                            _slack_post(webhook, msg)
                            with conn.cursor() as cur:
                                cur.execute("UPDATE metier.activite SET flag_slack = TRUE WHERE id_activite = %s", (row[0],))
                            conn.commit()
                            published_count += 1
                        except Exception:
                            nb_anomalies += 1

                    _log_s(logger, "Slack publiés (Fallback).", {"rows": published_count})
                    _write_metric_safe(conn, logger, start_time, "FORCE_FALLBACK", nb_lues, published_count, nb_anomalies)
                    return 0

            # Fin nominale
            status = "SUCCESS" if published_count > 0 else "NOOP"
            _log_s(logger, f"Slack publiés: {published_count}", {"rows": published_count})
            _write_metric_safe(conn, logger, start_time, status, nb_lues, published_count, nb_anomalies)
            return 0

    except Exception as exc:
        _log_f(logger, "Echec global script.", exc, {})
        # Tentative metric failure si possible
        try:
            with psycopg.connect(**pg_conf) as conn:
                 _write_metric_safe(conn, logger, start_time, "FAILURE", nb_lues if 'nb_lues' in locals() else 0, 0, 1)
        except: pass
        return 2

if __name__ == "__main__":
    sys.exit(main())
