/*
#-----------------------------------------------------------------------
# vue_avantages_salarie.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Synthèse des avantages par salarié.
*/

CREATE OR REPLACE VIEW metier.vue_avantages_salarie AS
SELECT
    s.cle_salarie,
    p.est_eligible_prime,
    p.motif_ineligibilite,
    b.est_eligible_bien_etre,
    b.nb_activites_periode,
    b.jours_bien_etre_attribues,
    b.periode_debut,
    b.periode_fin,
    CASE
        WHEN p.est_eligible_prime = true AND b.est_eligible_bien_etre = true THEN 'PRIME+BIEN_ETRE'
        WHEN p.est_eligible_prime = true AND b.est_eligible_bien_etre = false THEN 'PRIME_SEULE'
        WHEN p.est_eligible_prime = false AND b.est_eligible_bien_etre = true THEN 'BIEN_ETRE_SEUL'
        ELSE 'RIEN'
    END AS categorie_avantage
FROM metier.vw_bi_salarie s
LEFT JOIN metier.vue_elig_prime p ON p.cle_salarie = s.cle_salarie
LEFT JOIN metier.vue_elig_bien_etre b ON b.cle_salarie = s.cle_salarie;
