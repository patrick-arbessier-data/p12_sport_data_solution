# Schémas de la base

```mermaid
flowchart LR

  %% =========================
  %% GROUPES (par schéma)
  %% =========================
  subgraph SEC["SEC (RH sensible)"]
    direction TB
    SEC_RH["rh_salarie<br/><sub>id_salarie_brut<br/>nom<br/>prenom<br />...</sub>"]
    SEC_LIEN["lien_salarie<br/><sub>id_salarie_brut<br/>cle_salarie<br/>date_creation<br />...</sub>"]
  end
  
  subgraph OPS["OPS (Monitoring & Histo)"]
    direction LR
    OPS_RUN["histo_run<br/><sub>id_histo_run<br/>ts_histo<br/>date_histo_paris</sub>"]
    OPS_VUES["(VUES BI)<br />--> histo_kpi_age<br />--> histo_kpi_jours_sup<br/>--> histo_kpi_cout_prime<br/>--> histo_kpi_cout_prime_fixe<br />--> histo_kpi_incoherences<br/>"]
    OPS_MET["run_metrique<br/><sub>id_run<br/>nom_pipeline<br/>date_debut_exe<br />...</sub>"]
  end

  subgraph MET["METIER (Activités)"]
    direction LR
    MET_SAL["salarie<br/><sub>cle_salarie<br />...</sub>"]
    MET_ACT["activite<br/><sub>id_activite<br/>cle_salarie<br />...</sub>"]
    MET_CTRL["ctrl_trajet<br/><sub>id_ctrl<br/>cle_salarie<br />...</sub>"]
    MET_PARAM["param<br/><sub>code_param<br/>valeur_param<br />date_effet</sub>"]
  end

  %% =========================
  %% RELATIONS (FK du dump)
  %% Parent --> Enfant (lecture intuitive)
  %% =========================
  SEC_RH -->|id_salarie_brut| SEC_LIEN
  SEC_LIEN -->|cle_salarie| MET_SAL
  MET_SAL -->|cle_salarie| MET_ACT
  MET_SAL -->|cle_salarie| MET_CTRL

  OPS_RUN -->|id_histo_run| OPS_VUES

