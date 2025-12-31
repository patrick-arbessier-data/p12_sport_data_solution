/*
#-----------------------------------------------------------------------
# vue_elig_bien_etre.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Définir la vue calculant l'éligibilité aux avantages bien-être.
*/

CREATE OR REPLACE VIEW metier.vue_elig_bien_etre AS
WITH params AS (
    SELECT
        MAX(CASE WHEN code_param = 'min_activites_bien_etre' THEN valeur_param::integer END) AS min_activites_bien_etre,
        MAX(CASE WHEN code_param = 'jours_bien_etre' THEN valeur_param::integer END) AS jours_bien_etre
    FROM metier.vw_bi_param_effectif
),
periode AS (
    SELECT
        (date_trunc('month', now() AT TIME ZONE 'Europe/Paris')::date - INTERVAL '12 months')::date AS periode_debut,
        (date_trunc('month', now() AT TIME ZONE 'Europe/Paris')::date - INTERVAL '1 day')::date AS periode_fin
),
act AS (
    SELECT
        a.cle_salarie,
        COUNT(*)::integer AS nb_activites_periode
    FROM metier.vw_bi_activite a
    CROSS JOIN periode p
    WHERE
        a.est_exclue_structurelle = false
        AND a.date_activite BETWEEN p.periode_debut AND p.periode_fin
    GROUP BY a.cle_salarie
)
SELECT
    s.cle_salarie,
    p.periode_debut,
    p.periode_fin,
    COALESCE(a.nb_activites_periode, 0) AS nb_activites_periode,
    prm.min_activites_bien_etre,
    COALESCE(prm.jours_bien_etre, 0) AS jours_bien_etre,
    CASE
        WHEN prm.min_activites_bien_etre IS NULL THEN false
        ELSE (COALESCE(a.nb_activites_periode, 0) >= prm.min_activites_bien_etre)
    END AS est_eligible_bien_etre,
    CASE
        WHEN prm.min_activites_bien_etre IS NULL THEN 0
        WHEN COALESCE(a.nb_activites_periode, 0) >= prm.min_activites_bien_etre
            THEN COALESCE(prm.jours_bien_etre, 0)
        ELSE 0
    END AS jours_bien_etre_attribues
FROM metier.vw_bi_salarie s
CROSS JOIN periode p
CROSS JOIN params prm
LEFT JOIN act a ON a.cle_salarie = s.cle_salarie;
