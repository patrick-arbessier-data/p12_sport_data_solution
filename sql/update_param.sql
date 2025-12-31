-- Fichier de requêtes pour mod61_load_param_avantage.py
-- Gestion des paramètres métier dans la table metier.param

-- name: fetch_all_params
-- Récupérer tous les paramètres existants pour effectuer le diff
SELECT code_param, valeur_param FROM metier.param;

-- name: insert_param
-- Insérer les nouveaux paramètres
-- Ordre des params : code_param, valeur_param, date_effet
INSERT INTO metier.param (code_param, valeur_param, date_effet)
VALUES (%s, %s, %s)
ON CONFLICT (code_param) DO NOTHING;

-- name: update_param
-- Mettre à jour les paramètres modifiés
-- Ordre des params : valeur_param, date_effet, code_param
UPDATE metier.param
SET valeur_param = %s,
    date_effet = %s
WHERE code_param = %s;
