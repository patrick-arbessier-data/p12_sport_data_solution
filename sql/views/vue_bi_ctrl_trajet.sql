/*
#-----------------------------------------------------------------------
# vue_bi_ctrl_trajet.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Vue contrôle trajet (dernier contrôle par salarié).
*/

CREATE OR REPLACE VIEW metier.vw_bi_ctrl_trajet AS
WITH src0 AS (
    SELECT
        c.id_ctrl,
        c.cle_salarie,
        c.mode_trajet,
        c.distance_m,
        CASE
            WHEN c.distance_m IS NULL THEN NULL
            ELSE round((c.distance_m::numeric / 1000), 3)
        END AS distance_km,
        c.duree_sec,
        CASE
            WHEN c.duree_sec IS NULL THEN NULL
            ELSE round((c.duree_sec::numeric / 60), 3)
        END AS duree_min,
        c.seuil_km,
        c.est_incoherent,
        c.date_ctrl,
        -- Placeholder : CASE normalisation mode trajet
        {mode_norm_sql}
    FROM metier.ctrl_trajet c
),
ranked AS (
    SELECT
        src0.*,
        ROW_NUMBER() OVER (
            PARTITION BY src0.cle_salarie
            ORDER BY src0.date_ctrl DESC, src0.id_ctrl DESC
        ) AS rn
    FROM src0
)
SELECT
    cle_salarie,
    mode_trajet_normalise,
    mode_trajet_inconnu,
    distance_m,
    distance_km,
    duree_sec,
    duree_min,
    seuil_km,
    est_incoherent,
    date_ctrl,
    (mode_trajet_normalise = 'INCONNU') AS est_exclue_structurelle
FROM ranked
WHERE rn = 1;
