-- =============================================================================
-- sql/views/vues_kpi.sql
-- Vues KPI
--
-- KPI attendus :
-- - Coût annuel total de la prime (agrégé)
-- - Coût annuel total de la prime (montant fixe) (agrégé)
-- - Nombre de jours supplémentaires (agrégé)
-- - Pratique sportive : répartition par type d’activité (agrégé)
-- - Remonter les erreurs : nombre d’incohérences détectées (agrégé)
-- - Répartition par tranche d’âge : éligibilité et coût des avantages (agrégé)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- A) KPI 1 — Coût annuel total de la prime (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vue_kpi_cout_prime CASCADE;

CREATE VIEW metier.vue_kpi_cout_prime AS
WITH prm AS (
    SELECT MAX(CASE WHEN code_param = 'taux_prime' THEN valeur_param::numeric END) AS taux_prime
    FROM metier.vw_bi_param_effectif
),
rh AS (
    SELECT l.cle_salarie, r.brut_annuel
    FROM sec.lien_salarie l
    JOIN sec.rh_salarie r ON r.id_salarie_brut = l.id_salarie_brut
)
SELECT
    prm.taux_prime,
    COUNT(*) FILTER (WHERE e.est_eligible_prime = true) AS nb_salaries_eligibles_prime,
    ROUND(
        SUM(
            CASE
                WHEN e.est_eligible_prime = true THEN (COALESCE(rh.brut_annuel, 0) * prm.taux_prime)
                ELSE 0
            END
        )::numeric
    , 2) AS cout_annuel_total_prime
FROM metier.vue_elig_prime e
CROSS JOIN prm
LEFT JOIN rh ON rh.cle_salarie = e.cle_salarie
GROUP BY prm.taux_prime;


-- ---------------------------------------------------------------------------
-- B) KPI 2 — Coût annuel total de la prime (montant fixe) (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vue_kpi_cout_prime_fixe CASCADE;

CREATE VIEW metier.vue_kpi_cout_prime_fixe AS
WITH prm AS (
    SELECT MAX(CASE WHEN code_param = 'montant_prime_fixe' THEN valeur_param::numeric END) AS montant_prime
    FROM metier.vw_bi_param_effectif
),
elig AS (
    SELECT COUNT(*) FILTER (WHERE est_eligible_prime = true) AS nb_salaries_eligibles_prime
    FROM metier.vue_elig_prime
)
SELECT
    prm.montant_prime,
    elig.nb_salaries_eligibles_prime,
    ROUND((elig.nb_salaries_eligibles_prime * prm.montant_prime)::numeric, 2) AS cout_annuel_total_prime
FROM prm
CROSS JOIN elig;


-- ---------------------------------------------------------------------------
-- C) KPI 3 — Nombre de jours supplémentaires (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vue_kpi_jours_supplementaires CASCADE;

CREATE VIEW metier.vue_kpi_jours_supplementaires AS
WITH prm AS (
    SELECT
        MAX(CASE WHEN code_param = 'min_activites_bien_etre' THEN valeur_param::integer END) AS min_activites_bien_etre,
        MAX(CASE WHEN code_param = 'jours_bien_etre' THEN valeur_param::integer END) AS jours_bien_etre
    FROM metier.vw_bi_param_effectif
)
SELECT
    prm.min_activites_bien_etre,
    prm.jours_bien_etre,
    COUNT(*) FILTER (WHERE e.est_eligible_bien_etre = true) AS nb_salaries_eligibles_bien_etre,
    SUM(e.jours_bien_etre_attribues)::integer AS nb_jours_supplementaires,
    MIN(e.periode_debut) AS periode_debut,
    MIN(e.periode_fin) AS periode_fin
FROM metier.vue_elig_bien_etre e
CROSS JOIN prm
GROUP BY
    prm.min_activites_bien_etre,
    prm.jours_bien_etre;


-- ---------------------------------------------------------------------------
-- D) KPI 4 — Pratique sportive : répartition par type d’activité (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vue_kpi_pratique_sportive CASCADE;

CREATE VIEW metier.vue_kpi_pratique_sportive AS
WITH periode AS (
    SELECT
        (date_trunc('month', now() AT TIME ZONE 'Europe/Paris')::date - INTERVAL '12 months')::date AS periode_debut,
        (date_trunc('month', now() AT TIME ZONE 'Europe/Paris')::date - INTERVAL '1 day')::date AS periode_fin
)
SELECT
    a.type_activite_normalise,
    COUNT(*)::integer AS nb_activites,
    p.periode_debut,
    p.periode_fin
FROM metier.vw_bi_activite a
CROSS JOIN periode p
WHERE
    a.est_exclue_structurelle = false
    AND a.date_activite BETWEEN p.periode_debut AND p.periode_fin
GROUP BY
    a.type_activite_normalise,
    p.periode_debut,
    p.periode_fin
ORDER BY
    nb_activites DESC,
    a.type_activite_normalise;


-- ---------------------------------------------------------------------------
-- E) KPI 5 — Remonter les erreurs : nombre d’incohérences détectées (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vue_kpi_incoherences CASCADE;

CREATE VIEW metier.vue_kpi_incoherences AS
SELECT COUNT(*) FILTER (WHERE est_incoherent = true) AS nb_incoherences_detectees
FROM metier.vw_bi_ctrl_trajet;


-- ---------------------------------------------------------------------------
-- F) KPI 6 — Avantages par tranche d’âge (agrégé)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS metier.vw_kpi_age CASCADE;

CREATE VIEW metier.vw_kpi_age AS
WITH ref AS (
    SELECT (timezone('Europe/Paris', now()))::date AS date_run_paris
),
base AS (
    SELECT
        s.cle_salarie,
        s.date_naissance,
        ref.date_run_paris,
        CASE
            WHEN s.date_naissance IS NULL THEN NULL
            ELSE date_part('year', age(ref.date_run_paris, s.date_naissance))::int
        END AS age_annees
    FROM metier.vw_bi_salarie s
    CROSS JOIN ref
),
base_tranche AS (
    SELECT
        b.cle_salarie,
        b.date_run_paris,
        CASE
            WHEN b.age_annees IS NULL THEN 'INCONNU'
            WHEN b.age_annees < 26 THEN '- de 25'
            WHEN b.age_annees BETWEEN 26 AND 35 THEN '26-35'
            WHEN b.age_annees BETWEEN 36 AND 45 THEN '36-45'
            WHEN b.age_annees BETWEEN 46 AND 55 THEN '46-55'
            ELSE '56+'
        END AS tranche_age
    FROM base b
),
prm AS (
    SELECT
        MAX(CASE WHEN code_param = 'taux_prime' THEN valeur_param::numeric END) AS taux_prime,
        MAX(CASE WHEN code_param = 'montant_prime_fixe' THEN valeur_param::numeric END) AS montant_prime_fixe
    FROM metier.vw_bi_param_effectif
),
rh AS (
    SELECT l.cle_salarie, r.brut_annuel
    FROM sec.lien_salarie l
    JOIN sec.rh_salarie r ON r.id_salarie_brut = l.id_salarie_brut
),
elig_prime AS (
    SELECT cle_salarie, est_eligible_prime
    FROM metier.vue_elig_prime
),
elig_bien_etre AS (
    SELECT cle_salarie, est_eligible_bien_etre, jours_bien_etre_attribues
    FROM metier.vue_elig_bien_etre
)
SELECT
    b.date_run_paris,
    b.tranche_age,
    COUNT(DISTINCT b.cle_salarie) AS nb_salaries,
    COUNT(DISTINCT b.cle_salarie) FILTER (WHERE ep.est_eligible_prime = true) AS nb_salaries_eligibles_prime,
    ROUND(
        SUM(
            CASE
                WHEN ep.est_eligible_prime = true THEN (COALESCE(rh.brut_annuel, 0) * prm.taux_prime)
                ELSE 0
            END
        )::numeric
    , 2) AS cout_annuel_total_prime_variable,
    ROUND(
        (COUNT(DISTINCT b.cle_salarie) FILTER (WHERE ep.est_eligible_prime = true) * prm.montant_prime_fixe)::numeric
    , 2) AS cout_annuel_total_prime_fixe,
    COUNT(DISTINCT b.cle_salarie) FILTER (WHERE eb.est_eligible_bien_etre = true) AS nb_salaries_eligibles_bien_etre,
    SUM(COALESCE(eb.jours_bien_etre_attribues, 0))::integer AS jours_bien_etre_attribues_total
FROM base_tranche b
CROSS JOIN prm
LEFT JOIN elig_prime ep ON ep.cle_salarie = b.cle_salarie
LEFT JOIN rh ON rh.cle_salarie = b.cle_salarie
LEFT JOIN elig_bien_etre eb ON eb.cle_salarie = b.cle_salarie
GROUP BY
    b.date_run_paris,
    b.tranche_age,
    prm.taux_prime,
    prm.montant_prime_fixe;
