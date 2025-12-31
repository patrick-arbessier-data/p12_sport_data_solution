# P12 — POC Avantages Sportifs (Pipeline Data + Orchestration Kestra)

## 1) Contexte et objectif

Ce dépôt présente un **POC pédagogique** visant à démontrer la capacité à **concevoir, exécuter et piloter** un pipeline data de bout en bout :

- ingestion de déclaratifs d’activité (Google Sheet export CSV) ;
- chargement et mise à jour de tables PostgreSQL ;
- préparation de vues pour Power BI (BI / éligibilité / KPI) ;
- contrôles qualité (Soda) ;
- historisation de KPI ;
- notifications Slack **uniquement lorsque le GSheet a changé**.

> Priorité : robustesse, simplicité, traçabilité (projet de formation, pas d’optimisation avancée).

---

## 2) Stack et versions (référence repo)

- **Kestra** : `kestra/kestra:v1.1.4`
- **PostgreSQL** : `postgres:18` (DB Kestra + DB métier)
- **Python** (tâches Kestra) : `python:3.13-slim`
- **Soda** (tâches Kestra) : `sodadata/soda-core:v3.0.32`

Dépendances Python (cf. `requirements.txt`) :

- `python-dotenv`, `PyYAML`, `pandas`, `openpyxl`, `SQLAlchemy`, `psycopg[binary]`, `requests`, `pytest`

---

## 3) Pré-requis

- Docker Desktop (Windows)
- Accès à un Google Sheet (URL export CSV)
- (Optionnel selon exécution) Google Maps API Key
- Slack Webhook URL (canal messages) + Slack Webhook URL (alerting)

---

## 4) Configuration (.env)

Créer un fichier `.env` à la racine à partir de `.env.exemple` **(ne pas committer)**.

Variables attendues (extrait) :

- Orchestration / options :
  - `SIMULATION=0|1`
  - `FORCE_WEBHOOK=0|1`
  - `FORCE_GMAPS=0|1`
- Intégrations :
  - `URL_GSHEET=...`
  - `GOOGLE_MAPS_API_KEY=...`
  - `SLACK_WEBHOOK_URL=...`
  - `SLACK_ALERTING_URL=...`
- PostgreSQL (DB métier) :
  - `PGHOST=127.0.0.1`
  - `PGPORT=5433`
  - `PGUSER=postgres`
  - `PGPASSWORD=postgres`
  - `PGDATABASE=SportDataSolution`
- Divers :
  - `P12_PSEUDO_SALT=...`

---

## 5) Démarrage de l’infrastructure (Docker Compose)

Lancer l’infrastructure :

```bash
docker compose up -d
```

Services démarrés (cf. `docker-compose.yml`) :

- `kestra-db` : PostgreSQL dédié à Kestra (interne au réseau docker)
- `sportdb` : PostgreSQL métier (exposé en local)
  - Port hôte : `127.0.0.1:5433` → conteneur `5432`
- `kestra` : UI + orchestrateur
  - UI : `http://localhost:8080` (ou `${KESTRA_PORT}` si défini)

Arrêt :

```bash
docker compose down
```

---

## 6) Orchestration Kestra (S / G / P)

Le pipeline est piloté par 3 flags :

- **S** : simulation demandée (`SIMULATION=1`)
- **G** : Google Sheet a changé (`gsheet_changed=1`, calculé par MOD21)
- **P** : fichier paramètres a changé (`param_changed=1`, calculé par MOD22)

Règle principale :

- **Cas 0** : `S=0` et `G=0` et `P=0` → **STOP** (aucun traitement aval)
- Sinon : exécuter uniquement les modules nécessaires selon le scénario.

Documentation de référence (MODxx + S/G/P uniquement) :

- `docs/orchestration/table_decision.md`
- `docs/orchestration/schema_logique.md`
- `docs/orchestration/plan_de_test.md`

---

## 7) Modules (repères MODxx)

Le flow Kestra versionné (référence repo) : `p12.orchestration.pipeline_avantages_sportifs.yaml`

### Préambule (toujours exécuté)

- **MOD10** — Installation dépendances Python dans un venv persisté (`/pydeps/venv`) si `requirements.txt` a changé (hash SHA256)
- **MOD20** — Extraction GSheet (CSV) vers `data/raw` (via script Python)
- **MOD21** — Détection de changement GSheet (sortie Kestra `gsheet_changed=0|1`)
- **MOD22** — Détection changement paramètres `src/utils/config_param_avantage.yml` (sortie Kestra `param_changed=0|1`, hash persisté dans `/pydeps`)
- **MOD23** — Log debug des flags S/G/P (utile en soutenance)
- **MOD30** — Routage global : STOP (Cas 0) ou exécution aval

### Traitements aval (si Cas ≠ 0)

- **MOD40** — Normalisation GSheet (uniquement si `G=1`)
- **MOD50** — Simulation (uniquement si `S=1`)
- **MOD60** — Chargement tables RH (si `S=1` ou `G=1`)
- **MOD61** — Chargement paramètres avantages (si Cas ≠ 0)
- **MOD62** — Chargement table activités (si `S=1` ou `G=1`)
- **MOD70** — Soda checks tables
- **MOD80** — Enrich distances (si `S=1` ou `G=1` ou `FORCE_GMAPS=1`)
- **MOD90** — Préparation vues :
  - vues BI
  - vues éligibilité
  - vues KPI
- **MOD71** — Soda checks BI
- **MOD95** — Historisation (hors Cas 0)
- **MOD99** — Slack (uniquement si `G=1`)

---

## 8) Données d’entrée et sorties (repères)

### Entrées principales

- `data/raw/Données+RH.xlsx` (données sensibles)
- `data/raw/Données+Sportive.xlsx`
- Google Sheet déclaratifs (URL dans `.env` : `URL_GSHEET`)
- Paramètres :
  - `src/utils/config_param_avantage.yml`
  - `src/utils/config_pipeline.yml`

### Sorties principales

- `data/raw/<Date>_decla_gsheet.csv` : extractions GSheet historisées
- `data/processed/<Date>_declaratif_activites_gsheet.csv` : déclaratifs normalisés historisés
- `logs/<Date>_gsheet_anomalie.csv` / `logs/<Date>_gsheet_erreurs.csv` : anomalies/erreurs normalisation
- DB PostgreSQL (sportdb) : tables et vues consommables (Power BI)
- `outputs/rapports_SportDataSolution.pbix` : rapports Power BI

---

## 9) Structure du dépôt (extrait)

- `src/` : scripts Python (extract / load / transform / BI / slack / utils)
- `kestra/` : flow Kestra (versionné)
- `soda/` : config + checks
- `sql/` : DDL + vues SQL versionnées
- `docs/orchestration/` : table décision + schéma logique + plan de test
- `data/` : raw / processed
- `outputs/` : Power BI

---

## 10) Exécution et tests

Les 8 scénarios (Cas 0 → Cas 7) sont décrits et testés dans :

- `docs/orchestration/plan_de_test.md`

Rappel : Slack s’exécute **uniquement si `G=1`**.

---

## 11) Dépannage (symptômes fréquents)

- **STOP inattendu** : vérifier MOD23 (valeurs S/G/P) + vérifier `URL_GSHEET` dans `.env`
- **Slack non déclenché** : confirmer `G=1` (MOD21) + vérifier `SLACK_WEBHOOK_URL`
- **MOD80 absent** : confirmer `FORCE_GMAPS=1` ou (`S=1`/`G=1`) + clé `GOOGLE_MAPS_API_KEY`
- **Problème de montage workspace** : adapter le bind-mount dans le YAML Kestra (`/run/desktop/mnt/host/d/...:/workspace`) au chemin local du dépôt
