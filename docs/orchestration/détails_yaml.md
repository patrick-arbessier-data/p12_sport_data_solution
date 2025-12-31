# Détails du flux Kestra — Pipeline « Avantages Sportifs »

Ce document décrit l’orchestration Kestra en **modules fonctionnels** (MODxx) et les **conditions S/G/P/E/B** associées.
Conformément à la règle validée, **les IDs techniques Kestra ne sont pas mentionnés ici** : seules les références **MODxx** + **flags** sont utilisées.

---

## 1) Flags de pilotage (S/G/P) + garde-fous (E/B)

| Flag  | Source                                | Signification                                                  | Valeurs                   |
| ----- | ------------------------------------- | -------------------------------------------------------------- | ------------------------- |
| **S** | Variable d’environnement `SIMULATION` | Simulation demandée                                            | `1` oui / `0` non         |
| **G** | MOD21                                 | Google Sheet *activités* a changé                              | `1` changé / `0` inchangé |
| **P** | MOD22                                 | Fichier de paramétrage (`config_param_avantage.yml`) a changé  | `1` changé / `0` inchangé |
| **E** | MOD24                                 | Fichiers Excel RH/sport (dossier `data/raw/*.xlsx`) ont changé | `1` changé / `0` inchangé |
| **B** | MOD25                                 | **Bootstrap RH** requis (socle RH absent en base)              | `1` requis / `0` non      |

Flag complémentaire :

- **FORCE_GMAPS** (env `FORCE_GMAPS`) : force l’exécution de MOD80 **uniquement si le run n’est pas Cas 0** (ne bypass jamais le STOP).

---

## 2) Règles de routage (résumé)

### 2.1 Cas 0 (STOP)

Le run s’arrête **après le préambule** si :

- **S=0, G=0, P=0, E=0, B=0**

> Note : `FORCE_GMAPS=1` **ne** fait pas sortir du Cas 0.

### 2.2 Exécution du “flux principal”

Le flux principal s’exécute si **au moins un flag est actif** :

- **S=1 OU G=1 OU P=1 OU E=1 OU B=1**

### 2.3 Conditions clés par module

- **MOD40 (normalisation GSheet)** : si **G=1**
- **MOD50 (simulation + vérif)** : si **S=1**
- **MOD60 (load RH)** : si **E=1 OU B=1**
- **MOD62 (load activités)** : si **DATA_CHANGED = (S=1 OU G=1)**
- **MOD80 (distances)** : si **E=1 OU B=1 OU FORCE_GMAPS=1**, mais **jamais en Cas 0**
- **MOD99 (Slack)** : si **G=1** (publication uniquement lors d’un changement GSheet)

---

## 3) Vue d’ensemble (ordre d’exécution)

### 3.1 Préambule (toujours exécuté)

1. **MOD10** — Installation dépendances Python (venv persistée dans `/pydeps`)
2. **MOD20** — Extraction GSheet (CSV)
3. **MOD21** — Détection “GSheet changed” → flag **G**
4. **MOD22** — Détection “Param changed” → flag **P**
5. **MOD23** — Log des flags **S/G/P** + garde-fous (E/B)
6. **MOD24** — Détection “Excel changed” → flag **E**
7. **MOD25** — Détection “Bootstrap RH” → flag **B**

### 3.2 Routage (Cas 0 vs flux principal)

### **MOD30** — Routage S/G/P/E/B

- Cas 0 → STOP
- Sinon → exécution des modules métier (MOD40..MOD99) avec leurs conditions

---

## 4) Détail par module

+-----------------------------------

### [MOD10] Installation des dépendances

- **Objectif :** garantir un environnement Python reproductible dans Kestra.
- **Implémentation :**
  - crée (si absent) un venv dans `/pydeps/venv`
  - installe `requirements.txt` (cache pip dans `/pip-cache`)
- **Entrées :** `/workspace/requirements.txt`
- **Sorties :** venv persistée sur le volume Kestra `/pydeps`

+-----------------------------------

### [MOD20] Extraction Google Sheet (source activités)

- **Objectif :** récupérer le GSheet “activités” en CSV.
- **Implémentation :** exécute le module Python `src.gsheet.mod20_recup_gsheet`.
- **Entrées :** credentials/URL GSheet (via env ou config projet), dossier projet monté `/workspace`.
- **Sorties :**
  - fichier CSV extrait dans `data/raw/` ou `data/processed/` (selon script)
  - **flag fichier** `data/processed/gsheet_unchanged.flag` présent si aucune différence vs run précédent

+-----------------------------------

### [MOD21] Détection GSheet changed → flag G

- **Objectif :** calculer **G** sans refaire un diff complet (basé sur le flag produit par MOD20).
- **Règle :**
  - si `gsheet_unchanged.flag` existe → **G=0**
  - sinon → **G=1**
- **Sorties Kestra :** `gsheet_changed` = `0`/`1`

+-----------------------------------

### [MOD22] Détection Param changed → flag P

- **Objectif :** détecter un changement du paramétrage (taux prime, seuils, etc.).
- **Implémentation :**
  - hash SHA256 du fichier `/workspace/src/utils/config_param_avantage.yml`
  - persistance du hash dans `/pydeps/config_param_avantage.sha256`
- **Sorties Kestra :** `param_changed` = `0`/`1`

+-----------------------------------

### [MOD23] Log flags S/G/P/E/B

- **Objectif :** tracer les flags d’entrée de routage dans les logs Kestra.
- **Contenu :** affiche S (env), G (MOD21), P (MOD22), E (MOD24), B (MOD25).

+-----------------------------------

### [MOD24] Détection Excel changed → flag E

- **Objectif :** détecter une modification des sources Excel RH/sport (garde-fou technique).
- **Implémentation (principe) :**
  - calcule un hash sur l’ensemble des fichiers `data/raw/*.xlsx`
  - persiste le hash dans `/pydeps/rh_excel_bundle.sha256`
- **Sorties Kestra :** `excel_changed` = `0`/`1`

+-----------------------------------

### [MOD25] Détection Bootstrap RH → flag B

- **Objectif :** garantir qu’un premier run ne casse pas (tables RH manquantes en base).
- **Règle :**
  - **B=1** si le socle RH n’est pas présent (tables attendues absentes ou vides)
  - **B=0** sinon
- **Sorties Kestra :** `bootstrap_rh` = `0`/`1`

+-----------------------------------

### [MOD30] Routage S/G/P/E/B (Cas 0 vs flux principal)

- **Objectif :** éviter toute exécution inutile quand rien n’a changé (Cas 0).
- **Condition flux principal :** `S=1 OR G=1 OR P=1 OR E=1 OR B=1`
- **Sinon :** STOP (Cas 0)

+-----------------------------------

### [MOD40] Normalisation GSheet (activités)

- **Condition :** **G=1**
- **Objectif :** normaliser le CSV activités (libellés, types, formats) avant chargement.
- **Implémentation :** `python -m src.gsheet.mod40_normalise_gsheet`

+-----------------------------------

### [MOD50] Simulation + vérification

- **Condition :** **S=1**
- **Objectif :** générer un historique cohérent (12 mois) + valider la simulation.
- **Implémentation :**
  - `python -m src.gene_lignes.mod50_simuler_activites_csv`
  - `python -m src.gene_lignes.mod50_verif_simulation`

+-----------------------------------

### [MOD60] Chargement RH (socle)

- **Condition :** **E=1 OU B=1**
- **Objectif :** charger/mettre à jour les tables RH (adresse, salaire, mode de déplacement, etc.).
- **Implémentation :** `python -m src.etl.load.mod60_load_rh_tables`
- **Note :** ce module est un **garde-fou** (indépendant de S/G/P).

+-----------------------------------

### [MOD61] Chargement paramètres “avantage”

- **Condition :** exécuté dans tout run hors Cas 0 (flux principal)
- **Objectif :** charger le paramétrage (taux prime, règles) en base.
- **Implémentation :** `python -m src.etl.load.mod61_load_param_avantage`

+-----------------------------------

### [MOD62] Chargement activités

- **Condition :** **DATA_CHANGED = (S=1 OU G=1)**
- **Objectif :** charger les activités (réelles ou simulées) en base.
- **Implémentation :** `python -m src.etl.load.mod62_load_activite_table`

+-----------------------------------

### [MOD70] Tests Soda — tables

- **Condition :** exécuté dans tout run hors Cas 0 (flux principal)
- **Objectif :** contrôles de cohérence sur les tables (ex. nulls, bornes, intégrité).
- **Implémentation :**
  - `soda scan -d sportdata -c soda/config/ds_postgres.yml soda/checks/metier_salarie.yml`
  - `soda scan -d sportdata -c soda/config/ds_postgres.yml soda/checks/metier_activite.yml`
  
**Note réseau (exécution en conteneur Soda / Kestra)** : la connexion Postgres se fait sur le réseau Docker.
Utiliser `PGHOST=sportdb` (nom du service Postgres sur le réseau) et `PGPORT=5432` (port interne conteneur).

Depuis un conteneur sur le réseau Docker (Kestra, Soda Docker, autres services Docker) → PGHOST=sportdb et PGPORT=5432

Depuis Windows / hors Docker (CLI local, Python .venv, DBeaver, etc.) → PGHOST=127.0.0.1 et PGPORT=5433 (car c’est le port publié 5433->5432 de ton conteneur Postgres)

+--------------------------------------

### [MOD80] Enrichissement distances (Google Maps)

- **Condition :** **E=1 OU B=1 OU FORCE_GMAPS=1**, mais **jamais en Cas 0**
- **Objectif :** recalculer les distances domicile ↔ entreprise pour valider les modes de déplacement RH.
- **Implémentation :** `python -m src.etl.enrich.mod80_recup_distances`
- **Entrées :**
  - adresse entreprise (note de cadrage)
  - API key Google Maps (`GOOGLE_MAPS_API_KEY`)
- **Sorties :** tables/vues d’enrichissement utilisées par les contrôles d’éligibilité

+-----------------------------------

### [MOD90] Préparation BI / éligibilité / KPI

- **Condition :** exécuté dans tout run hors Cas 0 (flux principal)
- **Objectif :** produire les vues consommées par Power BI et les étapes KPI.
- **Implémentation :**
  - `python -m src.etl.bi.mod90_prepa_vues_bi`
  - `python -m src.etl.bi.mod90_prepa_eligibilite`
  - `python -m src.etl.bi.mod90_prepa_vues_kpi`

+-----------------------------------

### [MOD71] Tests Soda — vues BI

- **Condition :** exécuté dans tout run hors Cas 0 (flux principal)
- **Objectif :** contrôles sur les vues (format, agrégats, cohérence BI).
- **Implémentation :** `soda scan -d sportdata -c soda/config/ds_postgres.yml soda/checks/transverse_coherence_bi.yml`

**Note réseau (exécution en conteneur Soda / Kestra)** : la connexion Postgres se fait sur le réseau Docker.
Utiliser `PGHOST=sportdb` (nom du service Postgres sur le réseau) et `PGPORT=5432` (port interne conteneur).

Depuis un conteneur sur le réseau Docker (Kestra, Soda Docker, autres services Docker) → PGHOST=sportdb et PGPORT=5432

Depuis Windows / hors Docker (CLI local, Python .venv, DBeaver, etc.) → PGHOST=127.0.0.1 et PGPORT=5433 (car c’est le port publié 5433->5432 de ton conteneur Postgres)

+-----------------------------------

### [MOD95] Historisation

- **Condition :** exécuté dans tout run hors Cas 0 (flux principal)
- **Objectif :** historiser les KPI / résultats pour rejouer un historique si une source/paramètre change.
- **Implémentation :** `python -m src.etl.bi.mod95_run_histo`

+-----------------------------------

### [MOD99] Publication Slack

- **Condition :** **G=1** uniquement
- **Objectif :** publier les messages Slack correspondant aux activités (favoriser l’émulation).
- **Implémentation :** `python -m src.slack.mod99_publish_slack --origin KESTRA`
- **Entrée :** `SLACK_WEBHOOK_URL` (env) / option `FORCE_WEBHOOK` (env, selon implémentation script)
