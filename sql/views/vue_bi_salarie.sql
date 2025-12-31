/*
#-----------------------------------------------------------------------
# vue_bi_salarie.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Vue BI salariés (inclut la normalisation du mode de déplacement déclaré).
*/

CREATE OR REPLACE VIEW metier.vw_bi_salarie AS
SELECT
    ls.cle_salarie,
    rs.nom,
    rs.prenom,
    rs.bu,
    rs.type_contrat,
    rs.date_embauche,
    rs.date_naissance,
    -- Placeholder : CASE normalisation mode déplacement
    {mode_norm_sql}
FROM sec.lien_salarie ls
JOIN sec.rh_salarie rs ON rs.id_salarie_brut = ls.id_salarie_brut;
