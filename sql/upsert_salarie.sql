-- Fichier de requêtes pour mod60_load_rh_tables.py
-- Les paramètres sont positionnels (%s) pour permettre l'utilisation de executemany (batch processing).

-- name: deactivate_absent
-- Désactivation des salariés absents du fichier (batch via ANY)
UPDATE sec.rh_salarie 
SET actif = FALSE 
WHERE id_salarie_brut = ANY(%s) 
AND actif IS DISTINCT FROM FALSE;

-- name: upsert_rh_salarie
-- Insertion ou mise à jour des données RH (Batch Processing compatible)
-- Ordre des params : id, nom, prenom, bu, type, date_n, date_e, adr, mod, actif(bool)
INSERT INTO sec.rh_salarie (
    id_salarie_brut,
    nom,
    prenom,
    bu,
    type_contrat,
    date_naissance,
    date_embauche,
    adresse_dom,
    mod_depl_decl,
    actif
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
ON CONFLICT (id_salarie_brut)
DO UPDATE SET
    nom = EXCLUDED.nom,
    prenom = EXCLUDED.prenom,
    bu = EXCLUDED.bu,
    type_contrat = EXCLUDED.type_contrat,
    date_naissance = EXCLUDED.date_naissance,
    date_embauche = EXCLUDED.date_embauche,
    adresse_dom = EXCLUDED.adresse_dom,
    mod_depl_decl = EXCLUDED.mod_depl_decl,
    actif = TRUE
WHERE
    sec.rh_salarie.nom IS DISTINCT FROM EXCLUDED.nom
    OR sec.rh_salarie.prenom IS DISTINCT FROM EXCLUDED.prenom
    OR sec.rh_salarie.bu IS DISTINCT FROM EXCLUDED.bu
    OR sec.rh_salarie.type_contrat IS DISTINCT FROM EXCLUDED.type_contrat
    OR sec.rh_salarie.date_naissance IS DISTINCT FROM EXCLUDED.date_naissance
    OR sec.rh_salarie.date_embauche IS DISTINCT FROM EXCLUDED.date_embauche
    OR sec.rh_salarie.adresse_dom IS DISTINCT FROM EXCLUDED.adresse_dom
    OR sec.rh_salarie.mod_depl_decl IS DISTINCT FROM EXCLUDED.mod_depl_decl
    OR sec.rh_salarie.actif IS DISTINCT FROM TRUE;

-- name: insert_lien_salarie
-- Création des liens stables pour les nouveaux salariés
-- Ordre des params : id_salarie_brut, cle_salarie
INSERT INTO sec.lien_salarie (id_salarie_brut, cle_salarie)
VALUES (%s, %s)
ON CONFLICT (id_salarie_brut) DO NOTHING;

-- name: upsert_metier_salarie
-- Alimentation de la table métier
-- Ordre des params : cle_salarie, nom, prenom, bu, mod_depl_decl
INSERT INTO metier.salarie (cle_salarie, nom, prenom, bu, mod_depl_decl)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (cle_salarie)
DO UPDATE SET
    nom = EXCLUDED.nom,
    prenom = EXCLUDED.prenom,
    bu = EXCLUDED.bu,
    mod_depl_decl = EXCLUDED.mod_depl_decl
WHERE
    metier.salarie.nom IS DISTINCT FROM EXCLUDED.nom
    OR metier.salarie.prenom IS DISTINCT FROM EXCLUDED.prenom
    OR metier.salarie.bu IS DISTINCT FROM EXCLUDED.bu
    OR metier.salarie.mod_depl_decl IS DISTINCT FROM EXCLUDED.mod_depl_decl;

-- name: update_sport_declare
-- Mise à jour du sport déclaré (gestion unifiée set/clear via NULL)
-- Ordre des params : sport_valeur, id_salarie, sport_valeur (pour le distinct check)
UPDATE sec.rh_salarie 
SET sport_declare = %s 
WHERE id_salarie_brut = %s 
AND sport_declare IS DISTINCT FROM %s;
