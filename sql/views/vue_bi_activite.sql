/*
#-----------------------------------------------------------------------
# vue_bi_activite.sql
#-----------------------------------------------------------------------
Objectifs
---------
- Vue activités (données dédupliquées + normalisées + champs BI).
*/

CREATE OR REPLACE VIEW metier.vw_bi_activite AS
WITH src0 AS (
    SELECT
        a.id_activite,
        a.cle_salarie,
        a.date_debut,
        (a.date_debut AT TIME ZONE 'Europe/Paris')::date AS date_activite,
        a.duree_sec,
        CASE
            WHEN a.duree_sec IS NULL THEN NULL
            ELSE round((a.duree_sec::numeric / 60), 3)
        END AS duree_min,
        a.distance_m,
        CASE
            WHEN a.distance_m IS NULL THEN NULL
            ELSE round((a.distance_m::numeric / 1000), 3)
        END AS distance_km,
        
        -- Placeholder : CASE normalisation type activité
        {type_norm_sql},
        
        a.commentaire,
        a.source_donnee
    FROM metier.activite a
),
src AS (
    SELECT
        src0.*,
        CASE
            WHEN src0.type_activite_inconnu THEN 'INCONNU'
            -- Placeholders : Listes sports endurance / non-endurance
            WHEN {type_norm_expr} IN ({types_endurance_in}) THEN 'ENDURANCE'
            WHEN {type_norm_expr} IN ({types_non_endurance_in}) THEN 'NON_ENDURANCE'
            ELSE 'INCONNU'
        END AS categorie_activite_normalise,

        (
            NOT src0.type_activite_inconnu
            AND {type_norm_expr} NOT IN ({types_endurance_in})
            AND {type_norm_expr} NOT IN ({types_non_endurance_in})
        ) AS categorie_activite_inconnu,

        md5(
            concat_ws(
                '|',
                src0.cle_salarie,
                src0.date_debut,
                src0.type_activite_normalise,
                src0.duree_sec,
                coalesce(src0.distance_m, -1),
                coalesce(btrim(src0.source_donnee), '')
            )
        ) AS cle_dedup_activite,

        CASE
            WHEN src0.type_activite_inconnu THEN TRUE
            ELSE FALSE
        END AS est_exclue_structurelle,

        CASE
            WHEN src0.type_activite_inconnu THEN 'Type activité inconnu'
            ELSE NULL
        END AS motif_exclusion
    FROM src0
),
dedup AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                s.cle_salarie,
                s.date_debut,
                s.type_activite_normalise,
                s.duree_sec,
                coalesce(s.distance_m, -1),
                coalesce(btrim(s.source_donnee), '')
            ORDER BY s.id_activite
        ) AS rn_dedup
    FROM src s
)
SELECT
    cle_salarie,
    date_debut,
    date_activite,
    duree_sec,
    duree_min,
    distance_m,
    distance_km,
    type_activite_normalise,
    type_activite_inconnu,
    categorie_activite_normalise,
    categorie_activite_inconnu,
    source_donnee,
    commentaire,
    cle_dedup_activite,
    est_exclue_structurelle,
    motif_exclusion
FROM dedup
WHERE rn_dedup = 1;
