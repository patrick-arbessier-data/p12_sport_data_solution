# POC Avantages Sportifs — Sport Data Solution

POC d’un **pipeline data** orchestré par **Kestra** : ingestion des déclarations (Google Forms → Google Sheet), enrichissement (Google Maps), calcul des avantages, contrôles qualité (Soda), puis préparation de données pour consommation BI.

---

## TL;DR — lancer un 1er run rapidement

### Pré-requis

- **Docker Desktop** + **Docker Compose** opérationnels.
- **PostgreSQL métier** démarré **ET** schémas/tables/vues initialisés. Voir : [Initialisation DB PostgreSQL](#3).
- Le fichier **`.env`** (à la racine, copie de `.env.exemple`) :

- Options :
  - `SIMULATION=0|1`
  - `FORCE_WEBHOOK=0|1`
  - `FORCE_GMAPS=0|1`
- Intégrations :
  - `URL_GSHEET=...`
  - `GOOGLE_MAPS_API_KEY=...`
  - `SLACK_WEBHOOK_URL=...`
  - `SLACK_ALERTING_URL=...`
- PostgreSQL :
  - `PGHOST=127.0.0.1`
  - `PGPORT=5433`
  - `PGUSER=postgres`
  - `PGPASSWORD=postgres`
  - `PGDATABASE=SportDataSolution`
- Pseudonymisation :
  - `P12_PSEUDO_SALT=...`

---

### Lancer le pipeline

**Démarrer la stack**

```bash
   docker compose up -d
```

---

**Déclencher le flow Kestra “on-demand”**

- UI Kestra : `http://localhost:8080`
- Flow : `p12.orchestration.pipeline_avantages_sportifs`
- Exécuter le flow

### Générer des données pour la BI

- Option 1 — **Déclarations *réelles*** : remplir une déclaration d'activité via [Google Forms](https://forms.gle/jg3DpAHN6RrJujz49) (les noms/prénoms doivent exister dans [le fichier RH accessible ici](data/raw/Données+RH.xlsx).
- Option 2 — **Ajout de déclarations simulées** : mettre `SIMULATION=1` dans `.env`.
  - Au run initial : des simulations de déclaratifs seront intégrées
  - Pour les runs suivants **les anciennes simulations sont remplacées** par un nouveau jeu de données.
  - *La volumétrie et les scénarios des simulations se règlent dans `src/utils/config_pipeline.yml`.*

En cas de modification de `SIMULATION` :
Pour prendre en compte le changement (0|1), exécuter **obligatoirement** :

```bash
    docker compose up -d --force-recreate kestra
```

---

## 1) Ce que fait le POC

**A la première exécution du flow :**

- Récupération / détection de changements des entrées (Google Sheet, paramètres).
- Génération de déclaratifs simulés (`SIMULATION=1` recommandé).
- Chargement en base PostgreSQL (tables métier + vues BI).
- Enrichissement distances via Google Maps.
- Préparation des vues pour BI.
- Tests qualité (Soda) : tables puis vues BI.
- Notifications Slack pour chaque nouveau déclaratif valide, réalisé via Google Forms.
- Historisation du run et des vues générées.

**À chaque exécution suivante :**

- Récupération / détection de changements des entrées (Google Sheet, paramètres).
- (Optionnel) Génération de déclaratifs simulés (si `SIMULATION=1`).
- Chargement/MAJ en base PostgreSQL (tables métier + vues BI).
- (Optionnel) récupération des distances via Google Maps (si forcé --> FORCE_GMAPS=1 dans .env).
- Préparation des vues pour BI.
- Tests qualité (Soda) : tables puis vues BI.
- Notifications Slack pour chaque nouveau déclaratif valide, réalisé via Google Forms (si forcé --> FORCE_SLACK=1 dans .env, toutes les notifications sont ré-émises).
- Historisation du run et des vues générées.

**Important :**

- Le POC est conçu pour alimenter des **rapports BI**.
- Le calcul des distances  fonctionne **uniquement** la clé Google MAPS est configurée dans `.env`.
- Les notifications Slack ne fonctionnent **uniquement** les clés Webhook sont configurées dans `.env`.

---

## 2) Les 3 paramètres essentiels --> comment "jouer" avec le POC

### 2.1 SIMULATION

- Fichier : `.env`
- Valeurs : `SIMULATION=0|1`
- Effet :
  - `SIMULATION=1` → le run génère des déclaratifs simulés (CSV) **et remplace** les simulations précédentes.
  - `SIMULATION=0` → aucune nouvelle simulation (les anciennes restent utilisables).
- Configuration des simulations : `src/utils/config_pipeline.yml`, section `activites.*`.
- **Attention** : toute modification de `.env` doit être suivie d’un redémarrage de Kestra :

  ```bash
  docker compose up -d --force-recreate kestra
  ```

### 2.2 Paramètres de calcul des avantages

- Fichier : `src/utils/config_param_avantage.yml`
- Effet :
  - Le pipeline détecte les changements et recalcule automatiquement les avantages au run suivant.
  - Impact direct sur les rapports BI.

### 2.3 Déclarations réelles via Google Forms

- Action : remplir le formulaire Google Forms.
- Effet : une nouvelle ligne arrive dans le Google Sheet → intégrée au run suivant.
- **Validité** :
  - nom/prénom saisis doivent correspondre à un salarié existant dans `data/raw/Données+RH.xlsx`.
  - En cas de non-correspondance : le flow est marqué **FAILED** et une alerte Slack est envoyée.

---

## 3) - Initialisation DB PostgreSQL

> À faire au **premier lancement** (ou après reset complet de la base).

1. Démarrer PostgreSQL (conteneur `sportdb` via `docker compose up -d`).
2. Se connecter à la base métier (ex : pgAdmin, DBeaver, psql…).
3. Exécuter le script :
   - Fichier : `sql/ddl/create_all.sql`
   - Résultat attendu : création des schémas/tables/vues requis.

---

## 4) Architecture

### 4.1 Composants

- **Kestra** : orchestrateur + UI.
- **PostgreSQL métier** (`sportdb`) : données RH, déclaratifs, calculs, vues BI.
- **PostgreSQL Kestra** (`kestra-db`) : stockage interne Kestra.
- **Sources externes** :
  - Google Forms → Google Sheet (déclaratifs *réels*)
  - Google Maps (distances)
  - Slack (notifications)

### 4.2 Réseau / ports (local)

- Kestra UI : `http://localhost:8080`
- PostgreSQL métier : exposé en local (selon `docker-compose.yml`, souvent `127.0.0.1:5433` → conteneur `5432`)

---

## 5) Orchestration Kestra

Le flow Kestra pilote l’exécution via des **flags** (décision de routage) et des **garde-fous techniques**.

Voir les [détails des tâches du flow Kestra](docs/orchestration/détails_yaml.md).

### Flags de décision (scénarios S/G/P)

- **S (SIMULATION)** : variable d’environnement `SIMULATION` (`0|1`).
- **G (GSHEET_CHANGED)** : flag (`gsheet_changed=0|1`) calculée à partir du flag produit par l’extraction GSheet.
- **P (PARAM_CHANGED)** : flag (`param_changed=0|1`) basée sur un hash du fichier paramètres.

### Garde-fous techniques (hors matrice S/G/P)

- **E (EXCEL_CHANGED)** : flag (`excel_changed=0|1`) si les Excels RH/Sport ont changé (ou baseline absente).
- **B (BOOTSTRAP_RH)** : flag (`bootstrap_rh=0|1`) si le socle RH est absent ou vide en base.

## 6) Catalogue des modules (MODxx) du pipeline

Le flow est découpé en **modules** (`MODxx`) pour rendre le pipeline lisible et testable.
Voir les [détails des MODS](docs/orchestration/table_decision.md).

### 6.1 Lecture recommandée

- Pour exécuter : retenir uniquement les **3 paramètres** (section 2) + l’init DB (section 3).
- Pour comprendre : lire le [schema logique](docs/orchestration/schema_logique.md) et le [plan de test](docs/orchestration/plan_de_test.md).

### 6.2 Mapping "macro"

- **Préambule** : installation dépendances, extraction/détection de changements (GSheet/paramètres), logs de flags.
- **Simulation** (conditionnelle) : génération + vérification des déclaratifs simulés.
- **Chargements DB** : tables RH, paramètres, activités, opérations.
- **Qualité** : Soda (tables puis vues BI).
- **Enrichissement** : distances Google Maps (surtout au 1er chargement, inutile ensuite).
- **BI** : préparation vues + KPI.
- **Historisation** : écritures d’historiques.
- **Notifications** : Slack.

---

## 7) Contrôles qualité Soda

Deux familles :

- **MOD70 — tables** : contrôles sur tables métier (nulls, doublons, cohérence…).
- **MOD71 — vues BI** : contrôles sur vues (format, agrégats, cohérence BI…).

### Exécuter Soda en Docker

exemple (bash / Linux) :

```bash
docker run --rm \
  --network p12-infra_p12-net \
  -v ${PWD}:/workspace \
  -w /workspace \
  --env-file ./.env \
  -e PGHOST=sportdb \
  -e PGPORT=5432 \
  sodadata/soda-core:v3.0.32 \
  scan -d sportdata -c soda/config/ds_postgres.yml soda/checks/metier_salarie.yml
```

> Depuis Soda (Docker), `PGHOST=sportdb` et `PGPORT=5432` visent **le conteneur** PostgreSQL sur le réseau docker.

---

## 8) Conventions & règles de données

- **Clé métier** : `cle_salarie` est la clé de référence pour les jointures (pas `id_salarie_brut` côté BI).
- **Référence temporelle** : `Europe/Paris` (les dates “jour” sont évaluées en heure locale Paris).
- **Simulation** : contrainte d’unicité "1 déclaratif / jour / salarié" (heure fixée à **12:00**).
- **Référentiels** : règles/mappings centralisés dans `src/utils/config_pipeline.yml` (pas de mapping en dur ailleurs).

---

## 9) Dépannage

- **Flow FAIL dès le début** : vérifier `.env` (variables requises, valeurs) et redémarrer Kestra si `.env` a changé.
- **Erreurs DB** : vérifier l’exécution de `sql/ddl/create_all.sql` et la connexion PostgreSQL.
- **Pas de notifications Slack** : vérifier que `SLACK_WEBHOOK_URL` / `SLACK_ALERTING_URL` sont renseignées.
- **Peu de données BI** : activer `SIMULATION=1` pour générer un volume significatif de déclaratifs.

---

## 10) Ressources du dépôt

- Pipeline Kestra : `p12.orchestration.pipeline_avantages_sportifs.yaml`
- Configuration : `src/utils/config_pipeline.yml`, `src/utils/config_param_avantage.yml`
- SQL (init) : `sql/ddl/create_all.sql`
- Soda : `soda/config/*`, `soda/checks/*`

## 11) Structure du dépôt (overview)

```text
.
├─ docker-compose.yml
├─ requirements.txt
├─ .env.exemple
├─ p12.orchestration.pipeline_avantages_sportifs.yaml        (flow Kestra)
├─ docs/
│  └─ orchestration/
│     ├─ détails_yaml.md
│     ├─ table_decision.md
│     ├─ schema_logique.md
│     └─ plan_de_test.md
├─ src/
│  ├─ etl/            (extract / load / transform / BI)
│  ├─ gene_lignes/    (générateur de déclaratifs simulés)
│  ├─ slack/          (notification & alerting Slack)
│  ├─ maps/           (enrich distances)
│  └─ utils/          (config_pipeline.yml, config_param_avantage.yml, etc.)
├─ soda/              (config + checks)
├─ sql/               (DDL + vues SQL versionnées)
├─ data/
│  ├─ raw/
│  └─ processed/
└─ outputs/           (Power BI)
```

Repères :

- **Flow Kestra** : `p12.orchestration.pipeline_avantages_sportifs.yaml`
- **Docs orchestration** : `docs/orchestration/`
- **Code Python** : `src/` (extract / load / BI / gene_lignes / Slack / Maps / utils)
- **Soda** : `soda/` (config + checks)
- **SQL** : `sql/` (DDL + vues + requêtes externalisées)
- **Données locales** : `data/raw`, `data/processed`
- **Exports** : `outputs/` (rapports pbix)
