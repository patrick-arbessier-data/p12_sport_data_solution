-- ====================================================================
-- create_all.sql
--
-- Objectif : créer les schémas + la structure (séquences, tables,
--            contraintes, index) de la base SportDataSolution.
--
-- Source : SportDataSolution_schema.sql (extrait DDL / sans données)
-- Remarque : ce script ne charge aucune donnée (pas de COPY/INSERT).
-- ====================================================================

BEGIN;

-- ====================================================================
-- 1) Schémas
-- ====================================================================
CREATE SCHEMA IF NOT EXISTS sec;
CREATE SCHEMA IF NOT EXISTS metier;
CREATE SCHEMA IF NOT EXISTS ops;

-- ====================================================================
-- 2) Séquences
-- ====================================================================
CREATE SEQUENCE metier.activite_id_activite_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE SEQUENCE metier.ctrl_trajet_id_ctrl_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE SEQUENCE ops.histo_run_id_histo_run_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE SEQUENCE ops.run_metrique_id_run_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- ====================================================================
-- 3) Tables
-- ====================================================================
CREATE TABLE metier.activite (
    id_activite bigint NOT NULL,
    cle_salarie text NOT NULL,
    date_debut timestamp with time zone NOT NULL,
    duree_sec integer NOT NULL,
    type_activite text NOT NULL,
    distance_m integer,
    commentaire text,
    source_donnee text NOT NULL,
    date_ingestion timestamp with time zone DEFAULT now() NOT NULL,
    flag_slack boolean DEFAULT false NOT NULL,
    CONSTRAINT ck_activite__distance_non_negative CHECK ((distance_m >= 0)),
    CONSTRAINT ck_activite__duree_non_negative CHECK ((duree_sec >= 0))
);
CREATE TABLE metier.ctrl_trajet (
    id_ctrl bigint NOT NULL,
    cle_salarie text NOT NULL,
    mode_trajet text NOT NULL,
    distance_m integer NOT NULL,
    duree_sec integer NOT NULL,
    seuil_km numeric(6,2),
    est_incoherent boolean DEFAULT false NOT NULL,
    date_ctrl timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_ctrl_trajet__distance_non_negative CHECK ((distance_m >= 0)),
    CONSTRAINT ck_ctrl_trajet__duree_non_negative CHECK ((duree_sec >= 0))
);
CREATE TABLE metier.param (
    code_param text NOT NULL,
    valeur_param text NOT NULL,
    date_effet date
);
CREATE TABLE metier.salarie (
    cle_salarie text NOT NULL,
    nom text NOT NULL,
    prenom text NOT NULL,
    bu text,
    mod_depl_decl text,
    date_maj timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE sec.lien_salarie (
    id_salarie_brut text NOT NULL,
    cle_salarie text NOT NULL,
    date_creation timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE sec.rh_salarie (
    id_salarie_brut text NOT NULL,
    nom text NOT NULL,
    prenom text NOT NULL,
    date_naissance date,
    bu text,
    date_embauche date,
    brut_annuel numeric(12,2),
    type_contrat text,
    nb_jours_cp integer,
    adresse_dom text,
    mod_depl_decl text,
    actif boolean DEFAULT true NOT NULL,
    sport_declare text
);
CREATE TABLE ops.histo_kpi_age (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    date_run_paris date,
    tranche_age text,
    nb_salaries bigint,
    nb_salaries_eligibles_prime bigint,
    cout_annuel_total_prime_variable numeric,
    cout_annuel_total_prime_fixe numeric,
    nb_salaries_eligibles_bien_etre bigint,
    jours_bien_etre_attribues_total integer
);
CREATE TABLE ops.histo_kpi_cout_prime (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    taux_prime numeric,
    nb_salaries_eligibles_prime bigint,
    cout_annuel_total_prime numeric
);
CREATE TABLE ops.histo_kpi_cout_prime_fixe (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    montant_prime numeric,
    nb_salaries_eligibles_prime bigint,
    cout_annuel_total_prime numeric
);
CREATE TABLE ops.histo_kpi_incoherences (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    nb_incoherences_detectees bigint
);
CREATE TABLE ops.histo_kpi_jours_sup (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    min_activites_bien_etre integer,
    jours_bien_etre integer,
    nb_salaries_eligibles_bien_etre bigint,
    nb_jours_supplementaires integer,
    periode_debut date,
    periode_fin date
);
CREATE TABLE ops.histo_kpi_pratique_sport (
    id_histo_run bigint NOT NULL,
    ts_snapshot timestamp with time zone NOT NULL,
    type_activite_normalise text,
    nb_activites integer,
    periode_debut date,
    periode_fin date
);
CREATE TABLE ops.histo_run (
    id_histo_run bigint NOT NULL,
    ts_histo timestamp with time zone DEFAULT now() NOT NULL,
    date_histo_paris date DEFAULT (timezone('Europe/Paris'::text, now()))::date NOT NULL,
    status text DEFAULT 'RUNNING'::text NOT NULL,
    error_message text,
    param_effectif jsonb,
    CONSTRAINT ck_ops_histo_run_status CHECK ((status = ANY (ARRAY['RUNNING'::text, 'SUCCESS'::text, 'FAILURE'::text])))
);
CREATE TABLE ops.run_metrique (
    id_run bigint NOT NULL,
    nom_pipeline text NOT NULL,
    date_debut_exe timestamp with time zone NOT NULL,
    date_fin_exe timestamp with time zone,
    statut text NOT NULL,
    nb_lignes_lues integer DEFAULT 0 NOT NULL,
    nb_lignes_ecrites integer DEFAULT 0 NOT NULL,
    nb_anomalies integer DEFAULT 0 NOT NULL,
    "Origine" text,
    CONSTRAINT ck_run_metrique__compteurs_non_negative CHECK (((nb_lignes_lues >= 0) AND (nb_lignes_ecrites >= 0) AND (nb_anomalies >= 0)))
);

-- ====================================================================
-- 4) Defaults (liaison colonnes -> séquences)
-- ====================================================================
ALTER TABLE ONLY metier.activite ALTER COLUMN id_activite SET DEFAULT nextval('metier.activite_id_activite_seq'::regclass);
ALTER TABLE ONLY metier.ctrl_trajet ALTER COLUMN id_ctrl SET DEFAULT nextval('metier.ctrl_trajet_id_ctrl_seq'::regclass);
ALTER TABLE ONLY ops.histo_run ALTER COLUMN id_histo_run SET DEFAULT nextval('ops.histo_run_id_histo_run_seq'::regclass);
ALTER TABLE ONLY ops.run_metrique ALTER COLUMN id_run SET DEFAULT nextval('ops.run_metrique_id_run_seq'::regclass);

-- ====================================================================
-- 5) Ownership des séquences
-- ====================================================================
ALTER SEQUENCE metier.activite_id_activite_seq OWNED BY metier.activite.id_activite;
ALTER SEQUENCE metier.ctrl_trajet_id_ctrl_seq OWNED BY metier.ctrl_trajet.id_ctrl;
ALTER SEQUENCE ops.histo_run_id_histo_run_seq OWNED BY ops.histo_run.id_histo_run;
ALTER SEQUENCE ops.run_metrique_id_run_seq OWNED BY ops.run_metrique.id_run;

-- ====================================================================
-- 6) Contraintes (PK / FK / UNIQUE)
-- ====================================================================
ALTER TABLE ONLY metier.activite
    ADD CONSTRAINT activite_pkey PRIMARY KEY (id_activite);
ALTER TABLE ONLY metier.ctrl_trajet
    ADD CONSTRAINT ctrl_trajet_pkey PRIMARY KEY (id_ctrl);
ALTER TABLE ONLY metier.param
    ADD CONSTRAINT param_pkey PRIMARY KEY (code_param);
ALTER TABLE ONLY metier.salarie
    ADD CONSTRAINT salarie_pkey PRIMARY KEY (cle_salarie);
ALTER TABLE ONLY ops.histo_run
    ADD CONSTRAINT histo_run_pkey PRIMARY KEY (id_histo_run);
ALTER TABLE ONLY ops.run_metrique
    ADD CONSTRAINT run_metrique_pkey PRIMARY KEY (id_run);
ALTER TABLE ONLY sec.lien_salarie
    ADD CONSTRAINT lien_salarie_cle_salarie_key UNIQUE (cle_salarie);
ALTER TABLE ONLY sec.lien_salarie
    ADD CONSTRAINT lien_salarie_pkey PRIMARY KEY (id_salarie_brut);
ALTER TABLE ONLY sec.rh_salarie
    ADD CONSTRAINT rh_salarie_pkey PRIMARY KEY (id_salarie_brut);
ALTER TABLE ONLY metier.activite
    ADD CONSTRAINT fk_activite__salarie FOREIGN KEY (cle_salarie) REFERENCES metier.salarie(cle_salarie);
ALTER TABLE ONLY metier.ctrl_trajet
    ADD CONSTRAINT fk_ctrl_trajet__salarie FOREIGN KEY (cle_salarie) REFERENCES metier.salarie(cle_salarie);
ALTER TABLE ONLY metier.salarie
    ADD CONSTRAINT fk_metier_salarie__lien_salarie FOREIGN KEY (cle_salarie) REFERENCES sec.lien_salarie(cle_salarie);
ALTER TABLE ONLY ops.histo_kpi_age
    ADD CONSTRAINT fk_histo_kpi_age_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY ops.histo_kpi_cout_prime_fixe
    ADD CONSTRAINT fk_histo_kpi_cout_prime_fixe_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY ops.histo_kpi_cout_prime
    ADD CONSTRAINT fk_histo_kpi_cout_prime_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY ops.histo_kpi_incoherences
    ADD CONSTRAINT fk_histo_kpi_incoherences_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY ops.histo_kpi_jours_sup
    ADD CONSTRAINT fk_histo_kpi_jours_sup_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY ops.histo_kpi_pratique_sport
    ADD CONSTRAINT fk_histo_kpi_pratique_sport_run FOREIGN KEY (id_histo_run) REFERENCES ops.histo_run(id_histo_run) ON DELETE CASCADE;
ALTER TABLE ONLY sec.lien_salarie
    ADD CONSTRAINT fk_lien_salarie__rh_salarie FOREIGN KEY (id_salarie_brut) REFERENCES sec.rh_salarie(id_salarie_brut);

-- ====================================================================
-- 7) Index
-- ====================================================================
CREATE UNIQUE INDEX uq_metier_activite_cle_date_source_idx ON metier.activite USING btree (cle_salarie, date_debut, source_donnee);
CREATE INDEX idx_histo_kpi_age_run ON ops.histo_kpi_age USING btree (id_histo_run);
CREATE INDEX idx_histo_kpi_cout_prime_fixe_run ON ops.histo_kpi_cout_prime_fixe USING btree (id_histo_run);
CREATE INDEX idx_histo_kpi_cout_prime_run ON ops.histo_kpi_cout_prime USING btree (id_histo_run);
CREATE INDEX idx_histo_kpi_incoherences_run ON ops.histo_kpi_incoherences USING btree (id_histo_run);
CREATE INDEX idx_histo_kpi_jours_sup_run ON ops.histo_kpi_jours_sup USING btree (id_histo_run);
CREATE INDEX idx_histo_kpi_pratique_sport_run ON ops.histo_kpi_pratique_sport USING btree (id_histo_run);

COMMIT;
