# POC Avantages Sportifs — Sport Data Solution

Pipeline data end-to-end orchestré par Kestra pour tester la faisabilité technique
d'un dispositif d'avantages sportifs en entreprise : une prime de 5 % du salaire
pour les salariés commutant en mode actif, et 5 jours bien-être pour ceux cumulant
au moins 15 activités physiques sur la fenêtre retenue. Le POC valide la chaîne
d'ingestion, les règles d'éligibilité et l'impact financier selon plusieurs scénarios.

---

## Architecture / flux

Le pipeline est orchestré par Kestra et s'appuie sur deux bases PostgreSQL distinctes :

- `kestra-db` : backend interne de Kestra ;
- `sportdb` : base métier `SportDataSolution`.

Le tout est défini dans `docker-compose.yml`.

### Logique de routage

Le flow n'exécute l'aval que si au moins une condition est vraie :

- `S` : `SIMULATION=1`
- `G` : Google Sheet modifié
- `P` : `config_param_avantage.yml` modifié
- `E` : fichiers Excel modifiés
- `BOOTSTRAP_RH` : socle RH absent ou vide

Cette logique évite les traitements et recalculs inutiles.

---

## Stack

| Outil / composant            | Rôle                                          | Pourquoi                                                     |
| ---------------------------- | --------------------------------------------- | ------------------------------------------------------------ |
| Kestra                       | orchestration                                 | centraliser l'exécution, le suivi et le routage conditionnel |
| Python                       | ETL, simulation, BI                           | modulariser les traitements métier                           |
| PostgreSQL                   | stockage métier / sécurité / ops / BI         | structurer les données et les vues                           |
| Soda                         | contrôles qualité                             | automatiser les vérifications sur tables et vues             |
| Google Maps API              | calcul / contrôle des distances               | fiabiliser les trajets déclarés                              |
| MS Excel                     | source RH / sport legacy                      | charger le socle salarié                                     |
| Google Forms / Google Sheets | collecte déclarative                          | alimenter le mode réel                                       |
| CSV                          | intermédiaires de transformation / simulation | supporter certains échanges et traitements                   |
| Slack API / Webhook          | notifications et alerting                     | publier les messages métier et anomalies                     |
| Power BI                     | restitution                                   | analyser coûts, éligibilité et KPI                           |

---

## Structure du repo

```text
.
├─ docker-compose.yml
├─ dockerfile
├─ requirements.txt
├─ .env.exemple
├─ p12.orchestration.pipeline_avantages_sportifs.yaml
├─ README.md
├─ data/
│  ├─ raw/
│  └─ processed/
├─ docs/
│  └─ orchestration/
├─ outputs/
├─ soda/
│  ├─ checks/
│  └─ config/
├─ sql/
│  └─ ddl/
├─ src/
│  ├─ etl/
│  ├─ gene_lignes/
│  ├─ maps/
│  ├─ slack/
│  └─ utils/
```

Repères utiles :

- orchestration : `p12.orchestration.pipeline_avantages_sportifs.yaml`
- paramètres métier : `src/utils/config_param_avantage.yml`
- paramètres pipeline / simulation : `src/utils/config_pipeline.yml`
- mapping RH : `rh_mapping.yml`
- mapping activités : `activite_mapping.yml`
- DDL : `sql/ddl/create_all.sql`
- checks Soda : `soda/checks/`

---

## Prérequis

- Docker Desktop
- Docker Compose
- accès à `http://localhost:8080` pour Kestra
- accès PostgreSQL métier sur `127.0.0.1:5433`

### Variables attendues dans `.env`

#### Pilotage

- `SIMULATION=0|1`
- `FORCE_WEBHOOK=0|1`
- `FORCE_GMAPS=0|1`

#### Intégrations

- `URL_GSHEET=...`
- `GOOGLE_MAPS_API_KEY=...`
- `SLACK_WEBHOOK_URL=...`
- `P12_PSEUDO_SALT=...`

#### PostgreSQL métier

- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`

#### PostgreSQL Kestra

- `KESTRA_PG_USER`
- `KESTRA_PG_PASSWORD`
- `KESTRA_PG_DB`

Sans clé Google Maps, `MOD80` ne peut pas fonctionner normalement.  
Sans webhook Slack, `MOD99` n'émettra pas de notification.  
Sans `URL_GSHEET`, le mode réel n'est pas exploitable ; utiliser alors `SIMULATION=1`.

---

## Installation

### 1. Cloner le repo

```bash
git clone https://github.com/patrick-arbessier-data/p12_sport_data_solution.git
cd p12_sport_data_solution
```

### 2. Créer le `.env`

```bash
cp .env.exemple .env
```

Puis compléter les variables.

### 3. Démarrer la stack

```bash
docker compose up -d
```

Services exposés :

- Kestra : `http://localhost:8080`
- PostgreSQL métier : `127.0.0.1:5433`

### 4. Initialiser la base métier

```bash
psql -h 127.0.0.1 -p 5433 -U postgres -d SportDataSolution -f sql/ddl/create_all.sql
```

### 5. Importer le flow Kestra

Importer dans l'UI Kestra :

- `p12.orchestration.pipeline_avantages_sportifs.yaml`

---

## Exécution

### Mode réel

- soumettre une déclaration via Google Forms ;
- `MOD20` récupère le Google Sheet ;
- `MOD21` détecte le changement ;
- `MOD40` normalise ;
- `MOD62` charge les activités ;
- `MOD99` publie les notifications Slack si activées.

### Mode simulation

Dans `.env` :

```env
SIMULATION=1
```

Dans ce cas :

- `MOD50` génère les déclaratifs simulés ;
- la volumétrie, le scénario et la fenêtre sont pilotés par `src/utils/config_pipeline.yml` ;
- `activite_mapping.yml` est utilisé pour structurer les colonnes attendues et la simulation.

### Cas de relance typiques

Relancer le flow après :

- ajout d'une déclaration réelle ;
- modification de `config_param_avantage.yml` ;
- modification de `config_pipeline.yml` ;
- modification des fichiers Excel RH / sport ;
- besoin de recalcul ou renvoi de notifications.

---

## Données d'entrée / sortie

### Entrées

| Source                  | Module | Détail                                          |
| ----------------------- | ------ | ----------------------------------------------- |
| Excel RH / sport legacy | MOD60  | fichiers dans `data/raw/`, via `rh_mapping.yml` |
| Google Forms / Sheets   | MOD20  | déclarations réelles, normalisées par MOD40     |
| Déclarations simulées   | MOD50  | paramétrées dans `config_pipeline.yml`          |
| Paramètres métier       | MOD61  | `config_param_avantage.yml`                     |

### Sorties

| Destination | Module   | Contenu                                       |
| ----------- | -------- | --------------------------------------------- |
| PostgreSQL  | MOD60-95 | tables RH, activités, vues BI, KPI historisés |
| Power BI    | MOD90    | éligibilité, coûts, scénarios, KPI            |
| Slack       | MOD99    | notifications par salarié                     |

---

## Contrôles / validation

### Contrôles de routage

- `MOD21` : Google Sheet
- `MOD22` : paramètres avantages
- `MOD24` : fichiers Excel
- `MOD25` : bootstrap RH

### Contrôles qualité (Soda)

- `MOD70` : tables métier
- `MOD71` : vues BI

### Contrôle des trajets

- `MOD80` via Google Maps API

### Validation opérationnelle

Un run est considéré correct si :

- les modules attendus s'exécutent sans erreur ;
- les chargements PostgreSQL sont effectués ;
- les checks Soda passent ;
- les vues BI sont préparées ;
- les notifications Slack partent si configurées.

---

## Limites

- dépendance à Google Maps, Google Sheets et Slack ;
- nécessité de renseigner un `.env` local ;
- exécution principalement locale ;
- pas de packaging production ;
- besoin de données réelles actives ou de simulation pour alimenter les runs.

---

## Pistes d'amélioration

- ajouter un jeu de données anonymisé si un mode démo autonome est souhaité ;
- améliorer la navigation dans la documentation existante des tables, vues et indicateurs BI ;
- prévoir CI/CD ou environnement de staging en cas d'évolution vers un MVP.
