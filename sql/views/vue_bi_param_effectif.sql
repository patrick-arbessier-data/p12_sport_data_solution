/*
#-----------------------------------------------------------------------
# vue_bi_param_effectif.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Vue BI paramètres (clé/valeur) pour calculs d'éligibilité et KPI.
*/

CREATE OR REPLACE VIEW metier.vw_bi_param_effectif AS
WITH ranked AS (
    SELECT
        p.code_param,
        p.valeur_param,
        p.date_effet,
        ROW_NUMBER() OVER (
            PARTITION BY p.code_param
            ORDER BY p.date_effet DESC
        ) AS rn
    FROM metier.param p
    WHERE p.date_effet <= (now() AT TIME ZONE 'Europe/Paris')::date
)
SELECT
    code_param,
    valeur_param,
    date_effet
FROM ranked
WHERE rn = 1;
