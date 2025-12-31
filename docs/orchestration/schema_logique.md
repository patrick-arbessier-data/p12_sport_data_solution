# SCHEMA

```mermaid
flowchart TD

  %% =========================
  %% PREAMBULE (toujours execute)
  %% =========================
  A0(["Trigger : planif_jour_09"]) --> A1

  subgraph PRE["Préambule—tjs exécuté"]
    direction TB
    A1["MOD10<br/>install_requirements"]
    A2["MOD20<br/>extract_gsheet"]
    A3["MOD21<br/>detect_gsheet_changed<br/>G = 1 si changed = 1"]
    A4["MOD22<br/>detect_param_changed<br/>P = 1 si param_changed = 1"]
    A5["MOD23<br/>log_flags<br/>S/G/P/E/B"]
    A1 --> A2 --> A3 --> A4 --> A5
  end

  %% =========================
  %% GARDE-FOUS (E/B) — mini-bloc parallèle
  %% =========================
  A5 --> J0

  subgraph GF["Garde-fous RH (parallèle) — E/B"]
    direction LR
    E0["MOD24<br/>detect_excel_changed<br/>E = 1 si changed = 1"]
    B0["MOD25<br/>detect_bootstrap_rh<br/>B = 1 si bootstrap = 1"]
  end

  A5 --> E0
  A5 --> B0
  E0 --> J0
  B0 --> J0

  %% =========================
  %% DECISION S (une seule fois)
  %% =========================
  J0 --> DS{"S = 1 ?<br/>(simulation demandée)"}

  %% =========================
  %% Cas 0 (uniquement si S=0)
  %% Cas 0 = STOP si G=0, P=0, E=0, B=0
  %% (FORCE_GMAPS ne bypass jamais Cas 0)
  %% =========================
  DS -- Non --> D0{"Cas 0 ?<br/>G=0 et P=0<br/>et E=0 et B=0"}
  D0 -- Oui --> Z0(["STOP : fin immédiate<br/>(post-préambule)"])
  D0 -- Non --> D1S0{"G = 1 ?<br/>(GSheet changé)"}

  %% =========================
  %% Test G si S=1 (pas de Cas 0 possible)
  %% =========================
  DS -- Oui --> D1S1{"G = 1 ?<br/>(GSheet changé)"}

    %% =========================
  %% BRANCHE G=1 (commune) : normalisation toujours faite
  %% puis simulation optionnelle selon S (sans re-tester S)
  %% =========================
  D1S0 -- "Oui<br/>Cas 2 / Cas 6<br/>(G=1)" --> BRG0
  D1S1 -- "Oui<br/>Cas 4 / Cas 7<br/>(G=1)" --> BRG0

  subgraph BRG["G=1<br/>Cas 2 / Cas 4<br/>Cas 6 / Cas 7"]
    direction TB
    BRG0["MOD40<br/>if_normalise_needed<br/>→ normalise_gsheet"]

    %% Simulation uniquement si S=1 (Cas 4 / Cas 7)
    BRG_S1["MOD50<br/>if_simulation<br/>→ simuler_activites<br/>→ verif_simulation"]

    %% MOD60 conditionnel (E=1 ou B=1)
    BRG_D60{"MOD60 ?<br/>(E=1 ou B=1)"}
    BRG_RH["MOD60<br/>load_rh_tables"]

    %% Charges toujours nécessaires pour G=1 (activités) + params
    BRG_LOADS["MOD61 + MOD62<br/>load_param_avantage<br/>load_activite_table"]

    BRG_DQ1["MOD70<br/>soda_checks_tables"]

    %% MOD80 conditionnel (E=1 ou B=1 ou FORCE_GMAPS=1)
    BRG_D80{"MOD80 ?<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)"}
    BRG_ENR["MOD80<br/>recup_distances (GMaps)"]

    BRG_BI["MOD90<br/>prepa_vues_bi<br/>prepa_eligibilite<br/>prepa_vues_kpi"]
    BRG_DQ2["MOD71<br/>soda_checks_bi"]
    BRG_H["MOD95<br/>run_histo"]
    BRG_SLACK["MOD99<br/>publish_slack_webhook<br/>(G=1 uniquement)"]
    ENDG(["END"])

    %% Entrées par cas (symétrie)
    BRG0 -->|Cas 2 / Cas 6| BRG_D60
    BRG0 -->|Cas 4 / Cas 7| BRG_S1 --> BRG_D60

    %% MOD60 optionnel (labels normés)
    BRG_D60 -- "Oui<br/>Cas 2 / Cas 4 /<br />Cas 6 / Cas 7<br/>(E=1 ou B=1)" --> BRG_RH --> BRG_LOADS
    BRG_D60 -- "Non<br/>Cas 2 / Cas 4 /<br />Cas 6 / Cas 7<br/>(E=0 et B=0)" --> BRG_LOADS

    %% Suite commune
    BRG_LOADS --> BRG_DQ1 --> BRG_D80

    %% MOD80 optionnel (labels normés)
    BRG_D80 -- "Oui<br/>Cas 2 / Cas 4 /<br />Cas 6 / Cas 7<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)" --> BRG_ENR --> BRG_BI
    BRG_D80 -- "Non<br/>Cas 2 / Cas 4 <br />Cas 6 / Cas 7<br/>(E=0 et B=0<br/>et FORCE_GMAPS=0)" --> BRG_BI

    BRG_BI --> BRG_DQ2 --> BRG_H --> BRG_SLACK --> ENDG
  end

  %% =========================
  %% BRANCHE G=0 si S=1 (Cas 1 / Cas 5) : pas de Slack
  %% =========================
  D1S1 -- Non --> BRS0

  subgraph BRS["G=0 & S=1<br />Cas 1 / Cas 5 (sans Slack)"]
    direction TB
    BRS0["MOD50<br/>if_simulation<br/>→ simuler_activites<br/>→ verif_simulation"]

    %% MOD60 conditionnel (E=1 ou B=1)
    BRS_D60{"MOD60 ?<br/>(E=1 ou B=1)"}
    BRS_RH["MOD60<br/>load_rh_tables"]

    %% Charges nécessaires pour S=1 (activités) + params
    BRS_LOADS["MOD61 + MOD62<br/>load_param_avantage<br/>load_activite_table"]

    BRS_DQ1["MOD70<br/>soda_checks_tables"]

    %% MOD80 conditionnel (E=1 ou B=1 ou FORCE_GMAPS=1)
    BRS_D80{"MOD80 ?<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)"}
    BRS_ENR["MOD80<br/>recup_distances (GMaps)"]

    BRS_BI["MOD90<br/>prepa_vues_bi<br/>prepa_eligibilite<br/>prepa_vues_kpi"]
    BRS_DQ2["MOD71<br/>soda_checks_bi"]
    BRS_H["MOD95<br/>run_histo"]
    ENDS(["END"])

    BRS0 -->|Cas 1 / Cas 5| BRS_D60

    BRS_D60 -- "Oui<br/>Cas 1 / Cas 5<br/>(E=1 ou B=1)" --> BRS_RH --> BRS_LOADS
    BRS_D60 -- "Non<br/>Cas 1 / Cas 5<br/>(E=0 et B=0)" --> BRS_LOADS

    BRS_LOADS --> BRS_DQ1 --> BRS_D80

    BRS_D80 -- "Oui<br/>Cas 1 / Cas 5<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)" --> BRS_ENR --> BRS_BI
    BRS_D80 -- "Non<br/>Cas 1 / Cas 5<br/>(E=0 et B=0<br/>et FORCE_GMAPS=0)" --> BRS_BI

    BRS_BI --> BRS_DQ2 --> BRS_H --> ENDS
  end

  %% =========================
  %% BRANCHE G=0 si S=0 : décider P (Cas 3) ou garde-fou E/B
  %% =========================
  %% Déporter + expliciter le "Non" (S=0 & G=0 => Cas 3 ou GF E/B)
  D1S0 --> D1S0_G0
  D1S0_G0 -- "Non<br/>S=0 & G=0<br/>Cas 3 ou GF E/B" --> D3{"P = 1 ?<br/>(param change)"}

  %% Nœud tampon invisible (sert uniquement à positionner le label)
  D1S0_G0[" "]
  style D1S0_G0 fill:transparent,stroke:transparent,color:transparent;


  %% -- Cas 3 seul (ou Cas 3 + garde-fous E/B)
  D3 -- Oui --> BRP0

  subgraph BRP["G=0 & S=0 & P=1<br/>Cas 3 (sans Slack)"]
    direction TB

    %% MOD60 conditionnel (E=1 ou B=1)
    BRP_D60{"MOD60 ?<br/>(E=1 ou B=1)"}
    BRP_RH["MOD60<br/>load_rh_tables"]

    BRP0["MOD61<br/>load_param_avantage"]
    BRP1["MOD70<br/>soda_checks_tables"]

    %% MOD80 conditionnel (E=1 ou B=1 ou FORCE_GMAPS=1)
    BRP_D80{"MOD80 ?<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)"}
    BRP_ENR["MOD80<br/>recup_distances (GMaps)"]

    BRP2["MOD90<br/>prepa_vues_bi<br/>prepa_eligibilite<br/>prepa_vues_kpi"]
    BRP3["MOD71<br/>soda_checks_bi"]
    BRP4["MOD95<br/>run_histo"]
    ENDP(["END"])

    BRP_D60 -- "Oui<br/>Cas 3<br/>(E=1 ou B=1)" --> BRP_RH --> BRP0
    BRP_D60 -- "Non<br/>Cas 3<br/>(E=0 et B=0)" --> BRP0

    BRP0 --> BRP1 --> BRP_D80

    BRP_D80 -- "Oui<br/>Cas 3<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)" --> BRP_ENR --> BRP2
    BRP_D80 -- "Non<br/>Cas 3<br/>(E=0 et B=0<br/>et FORCE_GMAPS=0)" --> BRP2

    BRP2 --> BRP3 --> BRP4 --> ENDP
  end

  %% -- Si P=0, ce n'est plus "inatteignable" : on teste les garde-fous E/B
  D3 -- Non --> D4{"E=1 ou B=1 ?<br/>(garde-fou RH)"}

  %% =========================
  %% BRANCHE garde-fou E/B seul (S=0, G=0, P=0, mais E=1 ou B=1)
  %% =========================
  D4 -- "Oui<br/>GF E/B<br/>(E=1 ou B=1)" --> BRE0

  subgraph BRE["G=0 & S=0 & P=0<br/>Garde-fou E/B (sans Slack)"]
    direction TB
    BRE0["MOD60<br/>load_rh_tables"]
    BRE1["MOD61<br/>load_param_avantage"]
    BRE2["MOD70<br/>soda_checks_tables"]

    %% MOD80 conditionnel (E=1 ou B=1 ou FORCE_GMAPS=1)
    BRE_D80{"MOD80 ?<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)"}
    BRE_ENR["MOD80<br/>recup_distances (GMaps)"]

    BRE3["MOD90<br/>prepa_vues_bi<br/>prepa_eligibilite<br/>prepa_vues_kpi"]
    BRE4["MOD71<br/>soda_checks_bi"]
    BRE5["MOD95<br/>run_histo"]
    ENDE(["END"])

    BRE0 --> BRE1 --> BRE2 --> BRE_D80

    BRE_D80 -- "Oui<br/>GF E/B<br/>(E=1 ou B=1<br/>ou FORCE_GMAPS=1)" --> BRE_ENR --> BRE3
    BRE_D80 -- "Non<br/>GF E/B<br/>(E=0 et B=0<br/>et FORCE_GMAPS=0)" --> BRE3

    BRE3 --> BRE4 --> BRE5 --> ENDE
  end

  %% -- Sinon : ne doit pas arriver, car Cas 0 aurait STOP
  D4 -- Non --> Z1(["Inatteignable<br />Si Cas 0 correct<br/>S=0, G=0, P=0,<br /> E=0, B=0 => STOP"])

