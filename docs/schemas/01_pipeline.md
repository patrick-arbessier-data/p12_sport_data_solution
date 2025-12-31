# Schéma du pipeline

```mermaid
flowchart TD
  %% Styles conservés
  classDef source fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#000
  classDef process fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
  classDef storage fill:#f3e5f5,stroke:#7b1fa2,stroke-width:3px,color:#000
  classDef output fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#000
  classDef orch fill:#ffebee,stroke:#c62828,stroke-width:2px,stroke-dasharray: 5 5,color:#000
  classDef soda fill:#c8e6c9,stroke:#4caf50,stroke-width:3px,color:#000
  classDef bi fill:#fff8e1,stroke:#ff9800,stroke-width:2px,color:#000
  classDef decision fill:#f3e5f5,stroke:#9c27b0,stroke-width:3px,color:#000
  classDef alert fill:#ffcdd2,stroke:#d32f2f,stroke-width:2px,color:#000

  %% Orchestration au sommet
  subgraph Orchestration
    K("⏱️ Kestra<br/>scheduled / on-demand"):::orch
  end

  %% Sources à gauche
  subgraph Sources
    direction TB
    RH("📄 Excel RH"):::source
    SPORT("📄 Excel Sport"):::source
    GSHEET("📄 Google Forms<br/>|<br/>v<br/>Google Sheet<br/>Avantages Sportifs"):::source
  end

  %% PostgreSQL central
  subgraph DB ["🐘 PostgreSQL"]
    POSTGRES[("Base Données<br/>Sportdata")]:::storage
  end

  %% Pipeline principal avec SUBGRAPHS (vertical)
  subgraph Pipeline ["🛠️ Pipeline Kestra"]
    direction TB
    
    subgraph ETL ["⚙️ ETL Python"]
      direction TB
      INSTALL("🔧 Installation<br/>Dépendances"):::process
      INGEST("📄 Import GSheet"):::process
      DETECT("🔍 detect_gsheet_unchanged<br/>unchanged=0/1"):::process
      IF_CHANGE{"if_gsheet_changed<br/>unchanged=0 OR<br/>SIMULATION=1?"}:::decision
      NORMALISE("🔄 normalise_gsheet"):::process
      ALERT_GSHEET("🚨 Webhook p12-alerting<br/>Erreurs/Anomalies GSheet"):::alert
      SIMU{"🎲 SIMULATION=1?"}:::decision
      SIM_YES("✅ Générer Simulation"):::process
      
      subgraph LOAD_GROUP ["📥 Chargement Données"]
        direction LR
        LOAD_RH("RH Tables"):::process
        LOAD_PARAM("Paramètres<br/>Avantages"):::process
        LOAD_ACT("Déclaratifs<br/>Activités"):::process
      end
    end
    
    subgraph DQ ["✅ Data Quality"]
      SODA("Soda<br/>Contrôles Qualité"):::soda
    end
    
    subgraph ROUTE ["🗺️ Distances"]
      ROUTES("Calculs distances<br/>G-Maps API"):::process
    end
    
    subgraph BI ["📊 Préparation BI"]
      VUES("Préparer Vues BI"):::bi
      ELIG("Préparer Éligibilité"):::bi
      PBI_REFRESH("📊 Power BI Refresh"):::bi
      KPI("🔢 Refresh Vues KPI SQL"):::bi
    end
  end

  %% Outputs à droite
  subgraph Outputs ["📊 Reporting & Alerting"]
    PBI("📈 Power BI"):::output
    MON("🔍 Monitoring<br/>Kestra + Postgres"):::output
    SLACK("💬 Slack p12-messaging<br/>Notifications"):::output
    SLACK_ALERT("📢 Slack p12-alerting<br/>GSheet erreurs/anomalies"):::alert
  end

  %% Flux principal RÉEL du YAML + ALERT + FLÈCHES UNIDIRECTIONNELLES
  K -.-> INSTALL
  GSHEET --> INGEST
  
  INSTALL --> INGEST --> DETECT --> IF_CHANGE
  IF_CHANGE -->|OUI| NORMALISE --> ALERT_GSHEET
  IF_CHANGE -->|NON| STOP["⏹️ STOP sans erreur"]
  
  ALERT_GSHEET --> SIMU
  SIMU -->|Oui| SIM_YES
  SIM_YES --> LOAD_RH
  SIMU -->|Non| LOAD_RH
  
  %% Sources vers RH Tables
  RH -.-> LOAD_RH
  SPORT -.-> LOAD_RH
  
  LOAD_RH --> LOAD_PARAM --> LOAD_ACT --> SODA --> ROUTES --> VUES --> ELIG --> PBI_REFRESH --> KPI

  %% Interactions Postgres UNIDIRECTIONNELLES
  LOAD_RH -.-> POSTGRES
  LOAD_PARAM -.-> POSTGRES
  LOAD_ACT <--> POSTGRES
  SODA -.-> POSTGRES
  ROUTES -.-> POSTGRES
  VUES -.-> POSTGRES
  ELIG -.-> POSTGRES
  KPI -.-> POSTGRES

  %% Outputs
  POSTGRES --> PBI
  PBI_REFRESH -.-> PBI
  POSTGRES --> MON
  KPI --> MON
  KPI --> SLACK
  ALERT_GSHEET -.-> SLACK_ALERT
  K -.-> SLACK
  K -.-> MON
