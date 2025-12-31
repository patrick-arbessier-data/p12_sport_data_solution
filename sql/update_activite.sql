-- Fichier de requêtes pour mod62_load_activite_table.py
-- Gestion de la table metier.activite

-- name: insert_activite
-- Insertion ou mise à jour des activités (Upsert)
-- Ordre des params : cle_salarie, date_debut, duree_sec, distance_m, type_activite, commentaire, source_donnee, date_ingestion
INSERT INTO metier.activite (
    cle_salarie,
    date_debut,
    duree_sec,
    distance_m,
    type_activite,
    commentaire,
    source_donnee,
    date_ingestion
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (cle_salarie, date_debut, source_donnee)
DO UPDATE SET
    duree_sec = EXCLUDED.duree_sec,
    distance_m = EXCLUDED.distance_m,
    type_activite = EXCLUDED.type_activite,
    commentaire = EXCLUDED.commentaire;

-- name: truncate_activite
-- Vider intégralement la table activité
TRUNCATE TABLE metier.activite;

-- name: delete_by_source
-- Supprimer les activités correspondant à une liste de sources
-- Param : tableau de chaînes (ex: ARRAY['s1', 's2']) passé via ANY(%s)
DELETE FROM metier.activite
WHERE source_donnee = ANY(%s);
