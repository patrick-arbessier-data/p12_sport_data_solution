#-------------------------------------------------
# normalisation.py
#-------------------------------------------------
"""
Module utilitaire de normalisation et de configuration commune.

Commande d'exécution
--------------------
Ce module est destiné à être importé, pas exécuté directement.
from src.utils.normalisation import (
    normaliser_texte,
    dedupliquer_activites_par_jour,
    charger_yaml,
    get_required,
    pseudonymiser_cle_salarie,
    mois_dernieres_n_fenetres
)

Arguments
---------
Aucun (module utilitaire).

Objectifs
---------
- Centraliser les fonctions communes utilisées par plusieurs scripts du pipeline.
- Garantir une source de vérité unique pour la pseudonymisation (cle_salarie).
- Normaliser les textes (noms, prénoms) pour fiabiliser les comparaisons et jointures.
- Fournir des helpers pour la lecture de configuration YAML avec accès sécurisé aux clés.
- Gérer la déduplication des activités selon la contrainte métier (1 activité/salarié/jour).
- Calculer les fenêtres temporelles mensuelles glissantes.

Entrées
-------
- Fichiers :
    Configuration YAML (chemins variables selon appelant)
    
- Variables d'environnement :
    P12_PSEUDO_SALT (pseudonymisation déterministe)

Sorties
-------
Aucune (fonctions pures retournant des valeurs transformées).

Traitements & fonctionnalités
-----------------------------
- Normalisation de texte : minuscules, suppression des accents (NFKD), strip.
- Déduplication DataFrame pandas selon clé composite (cle_salarie, jour Europe/Paris).
- Pseudonymisation SHA256 déterministe avec sel.
- Parsing YAML avec gestion d'erreurs explicites.
- Accès sécurisé aux clés imbriquées dans un dictionnaire (notation pointée).
- Calcul de fenêtre mensuelle glissante (N derniers mois).

Contraintes
-----------
- Code simple utilisant la bibliothèque standard Python + PyYAML + pandas.
- Docstrings en français, respect PEP8.
- Pseudonymisation strictement cohérente avec la génération de données simulées.
- Déduplication timezone-aware (Europe/Paris) pour éviter les ambiguïtés de date.

Observations & remarques
------------------------
- PyYAML doit être installé (levée ImportError si absent).
- La fonction dedupliquer_activites_par_jour retourne un tuple (df_ok, df_ecarte).
- Les lignes écartées contiennent une colonne "raison" pour traçabilité des anomalies.
- La pseudonymisation utilise le format sal_[12 caractères hexadécimaux].
"""

# ---------------
# IMPORTS
# ---------------
from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yaml
except ImportError as exc:
    raise ImportError(
        "PyYAML n'est pas installé. Ajouter PyYAML dans l'environnement Python "
        "avant d'utiliser ce module (voir requirements.txt)."
    ) from exc

# -------------------------------------------------------------------
# Action 01 - Normalisation de texte
# -------------------------------------------------------------------
def normaliser_texte(valeur: str | None) -> str:
    """
    Normaliser un texte (minuscule + suppression des accents).

    Args:
        valeur: Valeur d'entrée (str ou None).

    Returns:
        Chaîne normalisée (jamais None).
    """
    if valeur is None:
        return ""
    texte = str(valeur).strip().lower()
    texte = unicodedata.normalize("NFKD", texte)
    return "".join(ch for ch in texte if not unicodedata.combining(ch))


# -------------------------------------------------------------------
# Action 02 - Déduplication activités par jour
# -------------------------------------------------------------------
def dedupliquer_activites_par_jour(
    df: "pd.DataFrame",
    cle_salarie_col: str = "cle_salarie",
    date_debut_col: str = "date_debut",
    tz_name: str = "Europe/Paris",
    keep: str = "latest",
    ordre_col: str | None = None,
    raison: str = "plusieurs_activites_jour",
) -> tuple["pd.DataFrame", "pd.DataFrame"]:
    """
    Dédupliquer un DataFrame pour garantir au plus 1 ligne par 
    (cle_salarie, jour Europe/Paris).

    Paramètres
    ----------
    df : pd.DataFrame
        Données contenant au minimum cle_salarie_col et date_debut_col.
    cle_salarie_col : str
        Nom de la colonne cle_salarie.
    date_debut_col : str
        Nom de la colonne date_debut (ISO avec offset recommandé).
    tz_name : str
        Timezone de référence pour dériver le jour local (YYYY-MM-DD).
    keep : {"latest", "first"}
        Stratégie de conservation en cas de doublon sur (cle_salarie, jour).
        - "latest" : conserver la ligne la plus récente selon ordre_col 
          (ou date_debut_col si ordre_col absent)
        - "first" : conserver la première occurrence (ordre d'entrée)
    ordre_col : str | None
        Colonne de tri pour définir la "plus récente" (ex : horodateur).
        Si None, la colonne date_debut_col est utilisée.
    raison : str
        Valeur ajoutée dans la colonne 'raison' du DataFrame des lignes écartées.

    Retours
    -------
    (df_ok, df_ecarte) : tuple[pd.DataFrame, pd.DataFrame]
        - df_ok : données dédupliquées (1 ligne max par (cle_salarie, jour))
        - df_ecarte : lignes supprimées pour cause de doublon, avec une colonne 'raison'
    """
    import pandas as pd
    from zoneinfo import ZoneInfo

    if cle_salarie_col not in df.columns:
        raise KeyError(f"Colonne manquante : {cle_salarie_col}")
    if date_debut_col not in df.columns:
        raise KeyError(f"Colonne manquante : {date_debut_col}")

    # -----------------------------------
    # Action 02-a : Dériver le jour local
    # -----------------------------------
    tz = ZoneInfo(tz_name)
    dt = pd.to_datetime(df[date_debut_col], errors="coerce", utc=False)
    if dt.isna().any():
        nb = int(dt.isna().sum())
        raise ValueError(
            f"{date_debut_col} contient {nb} valeurs non parseables "
            "(impossible de dédupliquer)."
        )

    if getattr(dt.dt, "tz", None) is not None:
        dt_local = dt.dt.tz_convert(tz)
    else:
        dt_local = dt.dt.tz_localize(tz)

    jour_local = dt_local.dt.strftime("%Y-%m-%d")
    work = df.copy()
    work["_jour_local"] = jour_local

    # -----------------------------------
    # Action 02-b : Résoudre les doublons
    # -----------------------------------
    subset = [cle_salarie_col, "_jour_local"]

    if keep not in {"latest", "first"}:
        raise ValueError("keep doit être 'latest' ou 'first'.")

    if keep == "first":
        mask_dup = work.duplicated(subset=subset, keep="first")
        df_ecarte = work.loc[mask_dup].copy()
        df_ok = work.loc[~mask_dup].copy()
    else:
        sort_col = ordre_col or date_debut_col
        if sort_col not in work.columns:
            raise KeyError(f"ordre_col introuvable : {sort_col}")

        sort_series = pd.to_datetime(work[sort_col], errors="coerce")
        if sort_series.isna().all():
            work["_sort_key"] = work[sort_col].astype(str)
        else:
            work["_sort_key"] = sort_series

        work = work.sort_values(
            by=["_jour_local", cle_salarie_col, "_sort_key"],
            ascending=[True, True, True]
        )
        mask_keep = ~work.duplicated(subset=subset, keep="last")
        df_ok = work.loc[mask_keep].copy()
        df_ecarte = work.loc[~mask_keep].copy()

        df_ok = df_ok.drop(columns=["_sort_key"])
        df_ecarte = df_ecarte.drop(columns=["_sort_key"])

    # -----------------------------------
    # Action 02-c : Préparer les anomalies
    # -----------------------------------
    if len(df_ecarte) > 0:
        df_ecarte = df_ecarte.drop(columns=["_jour_local"])
        df_ecarte.insert(0, "raison", raison)
    else:
        df_ecarte = df_ecarte.drop(columns=["_jour_local"])

    df_ok = df_ok.drop(columns=["_jour_local"])
    return df_ok, df_ecarte


# -------------------------------------------------------------------
# Action 03 - Lecture YAML et accès aux clés
# -------------------------------------------------------------------
def charger_yaml(path: Path) -> dict[str, Any]:
    """
    Charger une configuration YAML.

    Args:
        path: Chemin vers le fichier YAML.

    Returns:
        Contenu YAML sous forme de dict ({} si fichier vide).

    Raises:
        FileNotFoundError: si le fichier n'existe pas.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fichier YAML introuvable : {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_required(dct: dict[str, Any], key_path: str) -> Any:
    """
    Accéder à une clé requise dans un dict via notation "a.b.c".

    Args:
        dct: Dictionnaire source.
        key_path: Chemin de clé 
            (ex: "regles_eligibilite.fenetre_activites.nb_mois").

    Returns:
        La valeur trouvée.

    Raises:
        KeyError: si un segment du chemin est manquant.
    """
    cur: Any = dct
    for part in key_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(
                f"Clé manquante dans config : '{key_path}' (segment '{part}')"
            )
        cur = cur[part]
    return cur


# -------------------------------------------------------------------
# Action 04 - Pseudonymisation cle_salarie
# -------------------------------------------------------------------
def _pseudonymiser_id(id_brut: str, salt: str) -> str:
    """
    Pseudonymiser un identifiant salarié (déterministe).

    Args:
        id_brut: Identifiant brut (référentiel RH).
        salt: Sel (secret) chargé depuis l'environnement.

    Returns:
        Identifiant pseudonymisé (format: sal_[12 hex]).
    """
    digest = hashlib.sha256((salt + "|" + id_brut).encode("utf-8")).hexdigest()
    return f"sal_{digest[:12]}"


def pseudonymiser_cle_salarie(id_salarie_brut: str, salt: str) -> str:
    """
    Pseudonymiser l'identifiant salarié (stable, déterministe).

    Cette API explicite doit être utilisée par les scripts de transformation
    (ex: Google Sheet) pour rester cohérente avec la simulation CSV.

    Args:
        id_salarie_brut: Identifiant salarié brut (référentiel RH).
        salt: Sel (secret) chargé depuis l'environnement.

    Returns:
        cle_salarie au format sal_[12 hex].
    """
    return _pseudonymiser_id(id_brut=id_salarie_brut, salt=salt)


# -------------------------------------------------------------------
# Action 05 - Fenêtre mensuelle glissante
# -------------------------------------------------------------------
def mois_dernieres_n_fenetres(
    now: datetime,
    fenetre_mois: int,
    inclure_mois_courant: bool,
) -> list[tuple[int, int]]:
    """
    Retourner la liste (année, mois) des N derniers mois.

    Si inclure_mois_courant = False :
    - ancre au dernier jour du mois précédent (mois complets).
    Si inclure_mois_courant = True :
    - ancre dans le mois courant.

    Args:
        now: Date de référence (timezone-aware).
        fenetre_mois: Nombre de mois à couvrir (> 0).
        inclure_mois_courant: Inclure ou non le mois courant.

    Returns:
        Liste triée de tuples (année, mois), du plus ancien au plus récent.
    """
    if fenetre_mois <= 0:
        raise ValueError("fenetre_mois doit être > 0.")

    ancre = now
    if not inclure_mois_courant:
        premier_jour_mois = datetime(
            now.year, now.month, 1, 0, 0, 0, tzinfo=now.tzinfo
        )
        ancre = premier_jour_mois - timedelta(days=1)

    y, m = ancre.year, ancre.month
    mois: list[tuple[int, int]] = []

    for _ in range(fenetre_mois):
        mois.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    mois.reverse()
    return mois
