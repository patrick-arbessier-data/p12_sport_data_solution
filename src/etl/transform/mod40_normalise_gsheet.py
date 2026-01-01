#-------------------------------------------------
# mod40_normalise_gsheet.py
#-------------------------------------------------
"""
Transformer un export Google Sheet (CSV) en un CSV normalisé
(format déclaratif activités) avec contrôles de cohérence.

Commande d'exécution
--------------------
.\\.venv\\Scripts\\python.exe .\\src\\etl\\transform\\mod40_normalise_gsheet.py

Arguments
---------
--input : chemin CSV gsheet (optionnel, défaut : dernier fichier *_decla_sheet.csv dans data/raw/).
--out-dir : répertoire de sortie (optionnel, défaut : <repo>/data/processed).
--config : chemin config_pipeline.yml (optionnel, défaut : <repo>/src/utils/config_pipeline.yml).
--mapping : chemin activite_mapping.yml (optionnel, défaut : <repo>/src/utils/activite_mapping.yml).
--origin : origine d'exécution (CLI|KESTRA). Défaut : CLI.
--log-level : niveau de logs (DEBUG|INFO|WARNING|ERROR). Défaut : INFO.

Objectifs
---------
- Lire le dernier export gsheet présent dans data/raw/ (sauf si --input).
- Appliquer des contrôles de cohérence stricts (erreurs bloquantes).
- Détecter et logger les anomalies non bloquantes (ex: sport différent).
- Produire un fichier normalisé unique pour chargement ultérieur.

Entrées
-------
- Fichiers :
  - <repo>/data/raw/*_decla_sheet.csv (fichier source).
  - <repo>/data/raw/*rh*.xlsx (référentiel RH).
  - <repo>/data/raw/*sport*.xlsx (référentiel Sport).
  - <repo>/src/utils/config_pipeline.yml (configuration métier).
  - <repo>/src/utils/activite_mapping.yml (mapping colonnes).
  - <repo>/.env (variables d'environnement).

Sorties
-------
- Fichiers :
  - <repo>/data/processed/<TS>_declaratif_activites_gsheet.csv (si succès).
  - <repo>/logs/<TS>_gsheet_erreur.csv (si erreurs bloquantes).
  - <repo>/logs/<TS>_gsheet_anomalie.csv (si anomalies).

- Tables :
  - ops.run_metrique (métriques d'exécution).

- Alerting :
  - Notification Slack via src.slack.alerting en cas d'erreurs/anomalies.

Traitements & fonctionnalités
-----------------------------
- Chargement de la configuration externe (YAML).
- Chargement et nettoyage vectorisé des colonnes (strip, guillemets).
- Normalisation des noms/prénoms pour jointure RH.
- Parsing vectorisé des dates (DD/MM/YYYY -> Europe/Paris 12:00).
- Parsing vectorisé des durées (HH:MM:SS -> secondes).
- Parsing vectorisé des distances (km -> mètres).
- Jointure avec le référentiel RH pour valider les salariés.
- Jointure avec le référentiel Sport pour comparer le sport déclaré.
- Identification des erreurs bloquantes (parsing invalide, salarié inconnu, ambiguïté).
- Identification des anomalies (sport non déclaré, sport différent, doublons quasi-identiques).
- Pseudonymisation de l'ID salarié via sel cryptographique (P12_PSEUDO_SALT).
- Déduplication fonctionnelle (1 activité par jour par salarié).

Contraintes
-----------
- Si des erreurs bloquantes sont détectées (> 0), le script échoue (exit 1) et ne produit pas de sortie dans data/processed.
- Les fichiers logs (erreurs/anomalies) sont générés dans logs/ pour analyse.
- L'alerting Slack est déclenché si erreurs ou anomalies > 0.
- La métrique ops.run_metrique est écrite systématiquement (même en cas d'échec, bloc finally).
- Les noms de colonnes (entrée/sortie/ref) sont définis dans src/utils/activite_mapping.yml.

Observations & remarques
------------------------
- Si la colonne "Commentaire" existe et est non vide, la valeur est encadrée par des guillemets pour l'affichage Slack.
- Les sports non-endurance (config) voient leur distance forcée à NULL (None).
- Le dédoublonnage final utilise la règle métier : conserver l'activité la plus pertinente du jour.
"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import argparse
import inspect
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import psycopg
import yaml

# -------------------------------------------------------------------
# Action 01 - Contexte repo + imports projet
# -------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
TZ_NAME = "Europe/Paris"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# CLI : charger .env sans écraser les variables déjà présentes
load_dotenv(REPO_ROOT / ".env", override=False)

try:
    from src.utils.logger import get_logger, log_failure, log_success, write_run_metric
    from src.utils.normalisation import (
        charger_yaml,
        dedupliquer_activites_par_jour,
        get_required,
        normaliser_texte,
        pseudonymiser_cle_salarie,
    )
except ImportError:
    # Fallback ou erreur explicite si nécessaire, ici on laisse l'erreur monter
    raise


# -------------------------------------------------------------------
# Action 02 - Chargement Config & Constantes
# -------------------------------------------------------------------
# NOTE: Les constantes sont chargées dynamiquement dans le main() via le YAML.
# Ici on garde des valeurs par défaut ou vides si besoin, mais la logique
# repose sur le chargement du fichier activite_mapping.yml.

def _load_mapping_config(path_yaml: Path) -> Dict[str, Any]:
    """Charger la configuration de mapping YAML."""
    if not path_yaml.exists():
        raise FileNotFoundError(f"Configuration mapping manquante : {path_yaml}")
    with open(path_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# -------------------------------------------------------------------
# Action 03 - Utilitaires génériques (temps + logs)
# -------------------------------------------------------------------
def _timestamp_paris() -> str:
    """Construire un timestamp Paris au format YYYYMMDD_HHMMSS."""
    return datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y%m%d_%H%M%S")


def _safe_log_failure(logger: Any, exc: BaseException, message: str, **context: Any) -> None:
    """
    Émettre un log d'échec en restant compatible avec la signature réelle de log_failure().
    """
    suffix = ""
    if context:
        suffix = " | " + " | ".join(f"{k}={v}" for k, v in context.items())

    try:
        sig = inspect.signature(log_failure)
        params = sig.parameters
        has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

        if has_varkw:
            try:
                log_failure(logger, exc, message=message, **context)
                return
            except TypeError:
                log_failure(logger, message, exc, **context)
                return

        if "context" in params:
            try:
                log_failure(logger, exc, message=message, context=context)
                return
            except TypeError:
                log_failure(logger, f"{message}{suffix}", exc, context=context)
                return

        try:
            log_failure(logger, exc, message=f"{message}{suffix}")
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
    """
    Émettre un log de succès en restant compatible avec la signature réelle de log_success().
    """
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
                log_success(logger, message=message, context=context)
                return
            except TypeError:
                log_success(logger, f"{message}{suffix}", context=context)
                return

        try:
            log_success(logger, message=f"{message}{suffix}")
            return
        except TypeError:
            log_success(logger, f"{message}{suffix}")
            return

    except Exception:  # noqa: BLE001
        try:
            logger.info("%s%s", message, suffix)
        except Exception:  # noqa: BLE001
            pass


# -------------------------------------------------------------------
# Action 04 - Utilitaires I/O (détection des fichiers)
# -------------------------------------------------------------------
def _pick_latest_gsheet_file(data_raw_dir: Path) -> Path:
    """Sélectionner le dernier fichier '*_decla_sheet.csv' dans data/raw/."""
    candidates = sorted(
        data_raw_dir.glob("*_decla_sheet.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"Aucun fichier '*_decla_sheet.csv' trouvé dans {data_raw_dir}")
    return candidates[0]


def _find_excel(data_raw_dir: Path, contains_any: list[str]) -> Path:
    """Sélectionner le dernier .xlsx dont le nom contient un des tokens (insensible à la casse)."""
    tokens = [t.lower() for t in contains_any]
    xls = [p for p in data_raw_dir.glob("*.xlsx") if any(tok in p.name.lower() for tok in tokens)]
    xls = sorted(xls, key=lambda p: p.stat().st_mtime, reverse=True)

    if not xls:
        raise FileNotFoundError(f"Aucun fichier .xlsx trouvé (filtres={contains_any}) dans {data_raw_dir}")
    return xls[0]


# -------------------------------------------------------------------
# Action 05 - Vectorisation Helpers
# -------------------------------------------------------------------
def _vectorized_parse_date_paris(series: pd.Series) -> pd.Series:
    """
    Parse dd/mm/yyyy -> datetime (tz=Europe/Paris, 12:00:00).
    Retourne une série de datetime (NaT si erreur).
    """
    # 1. Nettoyage string
    s = series.astype(str).str.strip().str.strip('"')

    # 2. Parsing vers datetime (naif)
    dt = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")

    # 3. Force 12:00:00
    # Note: On additionne timedelta pour éviter .replace lent en itératif
    dt = dt + pd.Timedelta(hours=12)

    # 4. Localize
    return dt.dt.tz_localize(ZoneInfo(TZ_NAME), ambiguous='NaT', nonexistent='NaT')


def _vectorized_parse_duree(series: pd.Series) -> pd.Series:
    """
    Parse HH:MM:SS -> secondes (int).
    Retourne NaN si erreur.
    """
    s = series.astype(str).str.strip().str.strip('"')

    # Regex pour extraire HH, MM, SS
    # (?P<H>\d+):(?P<M>\d+):(?P<S>\d+)
    extracted = s.str.extract(r'^(?P<H>\d+):(?P<M>\d+):(?P<S>\d+)$')

    # Conversion en numeric
    h = pd.to_numeric(extracted['H'], errors='coerce').fillna(0)
    m = pd.to_numeric(extracted['M'], errors='coerce').fillna(0)
    sec = pd.to_numeric(extracted['S'], errors='coerce').fillna(0)

    # Validation (valeurs >= 0 implicite avec regex \d+)
    # On vérifie si le regex a matché (si H est NaN, tout est NaN)
    # Mask des valides
    valid_mask = extracted['H'].notna()

    total_sec = h * 3600 + m * 60 + sec
    return total_sec.where(valid_mask, np.nan)  # NaN si pas de match


def _vectorized_parse_distance(series: pd.Series) -> pd.Series:
    """
    Parse km (string fr/en) -> mètres (int).
    Retourne NaN si erreur/vide.
    """
    s = series.astype(str).str.strip().str.strip('"')

    # Replace , par .
    s = s.str.replace(',', '.', regex=False)

    # Vide -> NaN
    # Conversion numeric
    vals = pd.to_numeric(s, errors='coerce')

    # < 0 -> NaN
    vals = vals.where(vals >= 0, np.nan)

    # km -> m
    return (vals * 1000).round()


def _vectorized_quote_comment(series: pd.Series) -> pd.Series:
    """
    Ajoute des guillemets si non vide et non présents.
    """
    s = series.fillna("").astype(str).str.strip().str.strip('"').str.strip()
    mask_not_empty = s != ""

    # Format '"{s}"'
    return s.where(~mask_not_empty, '"' + s + '"')


# -------------------------------------------------------------------
# Action 06 - Compatibilité signatures
# -------------------------------------------------------------------
def _dedup_par_jour_compat(df: pd.DataFrame, logger: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Wrapper compatibilité pour dedupliquer_activites_par_jour."""
    result: Any
    try:
        params = inspect.signature(dedupliquer_activites_par_jour).parameters
        if "logger" in params:
            result = dedupliquer_activites_par_jour(df, logger=logger)
        else:
            result = dedupliquer_activites_par_jour(df)
    except TypeError:
        result = dedupliquer_activites_par_jour(df)

    if isinstance(result, tuple) and len(result) == 2:
        return result

    if isinstance(result, pd.DataFrame):
        return result, pd.DataFrame()

    raise TypeError("Retour inattendu de dedupliquer_activites_par_jour().")


# -------------------------------------------------------------------
# Action 07 - Chargement référentiels
# -------------------------------------------------------------------
def _load_ref_rh(path: Path, required_cols: list[str]) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, keep_default_na=False)
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes RH manquantes : {missing}")

    df = df[required_cols].copy()
    # On suppose que les colonnes 0, 1, 2 sont ID, Nom, Prénom dans l'ordre de required_cols
    # Pour être robuste : utiliser les noms exacts
    id_col = required_cols[0]
    nom_col = required_cols[1]
    prenom_col = required_cols[2]
    
    df[id_col] = df[id_col].str.strip()

    # Vectorisation de normaliser_texte
    df["Nom_norm"] = df[nom_col].map(normaliser_texte)
    df["Prenom_norm"] = df[prenom_col].map(normaliser_texte)

    # Détection ambiguïtés
    df["Ambigu"] = df.duplicated(subset=["Nom_norm", "Prenom_norm"], keep=False)

    return df


def _load_ref_sport(path: Path, required_cols: list[str]) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, keep_default_na=False)
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes Sport manquantes : {missing}")

    df = df[required_cols].copy()
    id_col = required_cols[0]
    sport_col = required_cols[1]

    df[id_col] = df[id_col].str.strip()
    df[sport_col] = df[sport_col].str.strip()

    df = df.drop_duplicates(subset=[id_col]).reset_index(drop=True)
    return df


# -------------------------------------------------------------------
# Action 08 - Normalisation gsheet (vectorisée)
# -------------------------------------------------------------------
def _normalize_gsheet_vectorized(
    df_gs: pd.DataFrame,
    df_rh: pd.DataFrame,
    df_sport: pd.DataFrame,
    salt: str,
    sports_non_endurance: set[str],
    logs_dir: Path,
    run_tag: str,
    logger: Any,
    config_mapping: Dict[str, Any],
) -> tuple[pd.DataFrame, int, int]:

    # Extraction Configuration Mapping
    norm_cfg = config_mapping["normalisation"]
    gsheet_cols = norm_cfg["gsheet_cols_required"]
    cols_out = norm_cfg["cols_out"]
    
    # Noms colonnes RH/Sport (depuis config)
    rh_cols = norm_cfg["referentiels"]["rh"]["required_cols"]
    rh_id_col = rh_cols[0]
    
    sport_cols = norm_cfg["referentiels"]["sport"]["required_cols"]
    sport_id_col = sport_cols[0]
    sport_val_col = sport_cols[1]

    # Noms colonnes Gsheet spécifiques
    gs_nom = "Nom"
    gs_prenom = "Prénom"
    gs_date = "Date"
    gs_duree = "Durée de l'activité"
    gs_dist = "Distance parcourue en kms"
    gs_sport = "Sport type"
    gs_comment = "Commentaire"
    gs_horodateur = "Horodateur"

    # 1. Préparation DataFrame entrée
    for col in gsheet_cols + [gs_comment]:
        if col in df_gs.columns:
            df_gs[col] = df_gs[col].fillna("").astype(str).str.strip().str.strip('"')

    # Clés de normalisation
    df_gs["Nom_norm"] = df_gs[gs_nom].map(normaliser_texte)
    df_gs["Prenom_norm"] = df_gs[gs_prenom].map(normaliser_texte)

    # 2. Parsing Vectorisé
    df_gs["date_parsed"] = _vectorized_parse_date_paris(df_gs[gs_date])
    df_gs["duree_sec"] = _vectorized_parse_duree(df_gs[gs_duree])
    df_gs["distance_m"] = _vectorized_parse_distance(df_gs[gs_dist])
    df_gs["commentaire_out"] = _vectorized_quote_comment(df_gs.get(gs_comment, pd.Series()))

    # 3. Jointure RH
    # RH Cols: ID, Nom, Prénom
    merged = df_gs.merge(
        df_rh[["Nom_norm", "Prenom_norm", rh_id_col, "Ambigu"]],
        on=["Nom_norm", "Prenom_norm"],
        how="left",
        indicator="_merge_rh"
    )

    # 4. Jointure Sport
    # Sport Cols: ID, Sport
    merged = merged.merge(
        df_sport.rename(columns={sport_val_col: "sport_declare"}),
        left_on=rh_id_col, # Join sur ID salarié RH
        right_on=sport_id_col, # Join sur ID salarié Sport
        how="left"
    )

    # 5. Détection des ERREURS BLOQUANTES
    mask_inconnu = (merged["_merge_rh"] == "left_only")
    mask_ambigu = (merged["Ambigu"] == True)
    mask_sport_vide = (merged[gs_sport] == "")
    mask_date_ko = merged["date_parsed"].isna()
    mask_duree_ko = merged["duree_sec"].isna()

    input_dist_non_vide = (merged[gs_dist] != "")
    res_dist_nan = merged["distance_m"].isna()
    mask_dist_ko = input_dist_non_vide & res_dist_nan

    mask_parsing = mask_date_ko | mask_duree_ko | mask_dist_ko

    merged["err_raison"] = None
    merged.loc[mask_parsing, "err_raison"] = "parsing_invalide"
    merged.loc[mask_sport_vide, "err_raison"] = "sport_manquant"
    merged.loc[mask_ambigu, "err_raison"] = "nom_prenom_ambigu"
    merged.loc[mask_inconnu, "err_raison"] = "nom_prenom_inconnu"

    # Extraction Erreurs
    df_errors = merged[merged["err_raison"].notna()].copy()
    nb_erreurs = len(df_errors)

    if not df_errors.empty:
        out_err = df_errors[gsheet_cols + ["err_raison"]].rename(columns={"err_raison": "raison"})
        logs_dir.mkdir(parents=True, exist_ok=True)
        out_err.to_csv(logs_dir / f"{run_tag}_gsheet_erreur.csv", index=False, encoding="utf-8")

    # 6. Filtrage des Valides
    valid_rows = merged[merged["err_raison"].isna()].copy()

    if valid_rows.empty:
        return pd.DataFrame(columns=cols_out), nb_erreurs, 0

    # 7. Gestion Anomalies
    valid_rows["ano_raison"] = None

    mask_non_endurance = valid_rows[gs_sport].isin(sports_non_endurance)
    valid_rows.loc[mask_non_endurance, "distance_m"] = None

    sp_saisi_norm = valid_rows[gs_sport].map(normaliser_texte)
    sp_decl_norm = valid_rows["sport_declare"].fillna("").map(normaliser_texte)

    mask_sp_non_decl = (sp_decl_norm == "")
    valid_rows.loc[mask_sp_non_decl, "ano_raison"] = "sport_non_declare"

    mask_sp_diff = (sp_decl_norm != "") & (sp_decl_norm != sp_saisi_norm)
    valid_rows.loc[mask_sp_diff & ~mask_sp_non_decl, "ano_raison"] = "sport_different"

    df_anos_1 = valid_rows[valid_rows["ano_raison"].notna()].copy()

    # 8. Pseudonymisation
    valid_rows["cle_salarie"] = valid_rows[rh_id_col].apply(
        lambda x: pseudonymiser_cle_salarie(id_salarie_brut=str(x), salt=salt)
    )

    # 9. Dédoublonnage "Quasi-identique"
    valid_rows["_key_dedup"] = (
        valid_rows["Nom_norm"] + "|" +
        valid_rows["Prenom_norm"] + "|" +
        valid_rows[gs_date] + "|" +
        valid_rows[gs_sport].map(normaliser_texte) + "|" +
        valid_rows[gs_duree] + "|" +
        valid_rows[gs_dist]
    )

    valid_rows = valid_rows.sort_values(by=["_key_dedup", gs_horodateur], ascending=[True, False])
    mask_dup_quasi = valid_rows.duplicated(subset=["_key_dedup"], keep="first")
    df_anos_quasi = valid_rows[mask_dup_quasi].copy()
    df_anos_quasi["ano_raison"] = "doublon_quasi_identique_ecarte"

    valid_rows_unique = valid_rows[~mask_dup_quasi].copy()

    # 10. Unicité Salarié / Jour
    valid_rows_unique["date_debut"] = valid_rows_unique["date_parsed"]
    df_final, df_drop_jour = _dedup_par_jour_compat(valid_rows_unique, logger=logger)

    if not df_drop_jour.empty:
        df_drop_jour["ano_raison"] = "plusieurs_activites_jour"

    # 11. Compilation Anomalies
    cols_ano = gsheet_cols + ["ano_raison"]

    all_anos = pd.concat([df_anos_1, df_anos_quasi, df_drop_jour], ignore_index=True)
    nb_anomalies = len(all_anos)

    if not all_anos.empty:
        # Affectation directe pour écraser toute colonne 'raison' existante
        all_anos["raison"] = all_anos["ano_raison"]

        # Garantie des colonnes manquantes
        for c in gsheet_cols:
            if c not in all_anos.columns:
                all_anos[c] = ""

        # Sélection stricte : Une seule colonne 'raison' + les colonnes gsheet
        out_ano = all_anos[["raison"] + gsheet_cols]

        logs_dir.mkdir(parents=True, exist_ok=True)
        out_ano.to_csv(logs_dir / f"{run_tag}_gsheet_anomalie.csv", index=False, encoding="utf-8")

    # 12. Finalisation Sortie
    df_final["type_activite"] = df_final[gs_sport]
    df_final["commentaire"] = df_final["commentaire_out"]
    df_final["source_donnee"] = "csv_gsheet"
    df_final["date_debut"] = df_final["date_debut"].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    df_final["duree_sec"] = df_final["duree_sec"].astype("Int64")
    df_final["distance_m"] = df_final["distance_m"].astype("Int64")

    df_out = df_final[cols_out].copy()

    return df_out, nb_erreurs, nb_anomalies


# -------------------------------------------------------------------
# Action 09 - CLI
# -------------------------------------------------------------------
def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normaliser un export Google Sheet vers le format déclaratif activités.")
    parser.add_argument("--input", default=None, help="Chemin du CSV gsheet.")
    parser.add_argument("--out-dir", default=None, help="Répertoire de sortie.")
    parser.add_argument("--config", default=None, help="Chemin config_pipeline.yml.")
    parser.add_argument("--mapping", default=None, help="Chemin activite_mapping.yml.")
    parser.add_argument("--origin", default="CLI", help="Origine d'exécution.")
    parser.add_argument("--log-level", default="INFO", help="Niveau de logs.")
    return parser.parse_args(argv)


# -------------------------------------------------------------------
# Action 10 - Main
# -------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logger = get_logger("mod40_normalise_gsheet", origin=str(args.origin), level=str(args.log_level))

    tz_paris = ZoneInfo(TZ_NAME)
    date_debut_exe = datetime.now(tz_paris)

    statut = "FAILED_EXCEPTION"
    lignes_lues = 0
    lignes_ecrites = 0
    anomalies_total = 0

    data_raw_dir = REPO_ROOT / "data" / "raw"
    data_processed_dir = REPO_ROOT / "data" / "processed"
    logs_dir = REPO_ROOT / "logs"
    
    config_path = Path(args.config) if args.config else (REPO_ROOT / "src" / "utils" / "config_pipeline.yml")
    mapping_path = Path(args.mapping) if args.mapping else (REPO_ROOT / "src" / "utils" / "activite_mapping.yml")

    in_path: Path | None = None
    out_path: Path | None = None

    try:
        in_path = Path(args.input) if args.input else _pick_latest_gsheet_file(data_raw_dir)
        out_dir = Path(args.out_dir) if args.out_dir else data_processed_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        run_tag = _timestamp_paris()
        out_path = out_dir / f"{run_tag}_declaratif_activites_gsheet.csv"

        cfg = charger_yaml(config_path)
        map_cfg = _load_mapping_config(mapping_path)
        
        # Extraction params mapping
        norm_map = map_cfg["normalisation"]
        gsheet_cols_req = norm_map["gsheet_cols_required"]
        rh_cols_req = norm_map["referentiels"]["rh"]["required_cols"]
        sport_cols_req = norm_map["referentiels"]["sport"]["required_cols"]

        rh_path = _find_excel(data_raw_dir, ["rh"])
        sport_path = _find_excel(data_raw_dir, ["sport"])

        salt = os.getenv("P12_PSEUDO_SALT")
        if not salt:
            raise RuntimeError("Variable d'environnement P12_PSEUDO_SALT manquante.")

        regles = get_required(cfg, "regles_simulation")
        sports_non_endurance = set(get_required(regles, "sports_non_endurance"))

        logger.info("Démarrage normalisation gsheet (Vectorisée) | input=%s | output=%s", in_path, out_path)

        # Lecture
        df_gs = pd.read_csv(in_path, dtype=str, keep_default_na=False)
        lignes_lues = len(df_gs)

        missing = [c for c in gsheet_cols_req if c not in df_gs.columns]
        if missing:
            raise KeyError(f"Colonnes gsheet manquantes : {missing}")

        df_rh = _load_ref_rh(rh_path, rh_cols_req)
        df_sport = _load_ref_sport(sport_path, sport_cols_req)

        # Traitement
        df_out, nb_erreurs, nb_anomalies_nb = _normalize_gsheet_vectorized(
            df_gs=df_gs,
            df_rh=df_rh,
            df_sport=df_sport,
            salt=salt,
            sports_non_endurance=sports_non_endurance,
            logs_dir=logs_dir,
            run_tag=run_tag,
            logger=logger,
            config_mapping=map_cfg,
        )

        anomalies_total = int(nb_erreurs) + int(nb_anomalies_nb)

        # Alerting Slack
        if nb_erreurs > 0 or nb_anomalies_nb > 0:
            try:
                from src.slack.alerting import main as slack_alerting_main
            except ImportError as exc:
                statut = "FAILED"
                _safe_log_failure(logger, exc, message="Imports projet impossibles (alerting Slack).")
                return 1

            rc_alert = slack_alerting_main([
                "--pipeline", "mod40",
                "--origin", str(args.origin),
                "--log-level", str(args.log_level),
            ])
            if int(rc_alert) != 0:
                statut = "FAILED"
                _safe_log_failure(logger, RuntimeError("Slack Alert Failed"), message="Échec alerting Slack (bloquant).")
                return 1

        # Stop si Erreurs
        if nb_erreurs > 0:
            statut = "FAILED"
            lignes_ecrites = 0
            _safe_log_failure(
                logger,
                RuntimeError(f"Erreurs gsheet détectées : {nb_erreurs}."),
                message="Normalisation gsheet en échec (erreurs bloquantes).",
                erreurs=nb_erreurs,
            )
            return 1

        # Succès
        df_out.to_csv(out_path, index=False, encoding="utf-8")
        lignes_ecrites = len(df_out)
        statut = "SUCCESS"

        _safe_log_success(logger, "Normalisation gsheet terminée.", rows=lignes_ecrites)
        return 0

    except Exception as exc:  # noqa: BLE001
        statut = "FAILED_EXCEPTION"
        _safe_log_failure(logger, exc, message="Échec normalisation gsheet.")
        return 1

    finally:
        date_fin_exe = datetime.now(tz_paris)
        try:
            with psycopg.connect(
                host=os.getenv("PGHOST"), port=os.getenv("PGPORT"),
                user=os.getenv("PGUSER"), password=os.getenv("PGPASSWORD"),
                dbname=os.getenv("PGDATABASE"),
            ) as conn:
                write_run_metric(
                    conn=conn, nom_pipeline="mod40_normalise_gsheet",
                    date_debut_exe=date_debut_exe, date_fin_exe=date_fin_exe,
                    statut=statut, nb_lignes_lues=int(lignes_lues),
                    nb_lignes_ecrites=int(lignes_ecrites), nb_anomalies=int(anomalies_total),
                    logger=logger, raise_on_error=True,
                )
                conn.commit()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
