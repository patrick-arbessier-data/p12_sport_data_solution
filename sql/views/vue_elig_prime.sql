/*
#-----------------------------------------------------------------------
# vue_elig_prime.sql
#-----------------------------------------------------------------------

Objectifs
---------
- Définir la vue calculant l'éligibilité à la "Prime Déplacement" pour chaque salarié.
- Centraliser les règles métiers d'exclusion (trajets incohérents, salaire manquant, mode de transport non éligible).

Entrées
-------
- sec.lien_salarie : Lien entre identifiant technique et brut salarié.
- sec.rh_salarie : Données RH (salaire brut annuel).
- metier.vw_bi_salarie : Données BI consolidées des salariés (mode de transport déclaré).
- metier.vw_bi_ctrl_trajet : Résultats des contrôles de cohérence sur les trajets.

Sorties
-------
- metier.vue_elig_prime : Vue exposant le booléen d'éligibilité et le motif de rejet éventuel.

Règles de gestion
-----------------
- Un salarié est ÉLIGIBLE si :
    1. Son mode de transport déclaré est "Marche/running" ou "Vélo/Trottinette/Autres".
    2. Il a des données de trajet exploitables (non NULL).
    3. Ses trajets ne sont pas marqués comme structurellement exclus.
    4. Ses trajets ne sont pas marqués comme incohérents (vitesse excessive, etc.).
    5. Il dispose d'un salaire brut annuel valide (> 0).

- Priorité des motifs d'inéligibilité (ordre du CASE) :
    1. Salaire manquant ou nul.
    2. Données de trajet manquantes.
    3. Trajets exclus structurellement.
    4. Trajets incohérents.
    5. Mode de transport inconnu.
    6. Mode de transport non sportif (ex: Voiture, Transports en commun).

*/

CREATE OR REPLACE VIEW metier.vue_elig_prime AS
WITH sal AS (
    -- Récupération du brut annuel via la table de lien sécurisée
    -- Sert uniquement à vérifier la présence du salaire (règle d'éligibilité)
    -- Le montant n'est PAS exposé dans la vue finale pour des raisons de confidentialité
    SELECT
        l.cle_salarie,
        r.brut_annuel
    FROM sec.lien_salarie l
    JOIN sec.rh_salarie r
        ON r.id_salarie_brut = l.id_salarie_brut
),
trajet AS (
    -- Récupération de l'état du contrôle trajet pour chaque salarié
    SELECT
        cle_salarie,
        est_incoherent,
        est_exclue_structurelle
    FROM metier.vw_bi_ctrl_trajet
)
SELECT
    s.cle_salarie,
    
    -- Calcul du booléen d'éligibilité (TOUTES les conditions doivent être vraies)
    (
        -- 1. Mode de transport valide
        (s.mod_depl_decl_inconnu = false)
        AND (s.mod_depl_decl_normalise IN ('Marche/running', 'Vélo/Trottinette/Autres'))
        
        -- 2. Données trajets présentes et valides
        AND (t.cle_salarie IS NOT NULL)
        AND (t.est_exclue_structurelle = false)
        AND (t.est_incoherent = false)
        
        -- 3. Salaire valide
        AND (rh.brut_annuel IS NOT NULL)
        AND (rh.brut_annuel > 0)
    ) AS est_eligible_prime,

    -- Détermination du motif d'inéligibilité (Premier motif rencontré)
    CASE
        WHEN (rh.brut_annuel IS NULL OR rh.brut_annuel <= 0) THEN 'salaire_manquant'
        WHEN (t.cle_salarie IS NULL) THEN 'ctrl_trajet_manquant'
        WHEN (t.est_exclue_structurelle = true) THEN 'ctrl_trajet_exclu'
        WHEN (t.est_incoherent = true) THEN 'ctrl_trajet_incoherent'
        WHEN (s.mod_depl_decl_inconnu = true) THEN 'mode_inconnu'
        WHEN (s.mod_depl_decl_normalise NOT IN ('Marche/running', 'Vélo/Trottinette/Autres')) THEN 'mode_non_sportif'
        ELSE NULL -- Salarié éligible
    END AS motif_ineligibilite

FROM metier.vw_bi_salarie s
LEFT JOIN trajet t
    ON t.cle_salarie = s.cle_salarie
LEFT JOIN sal rh
    ON rh.cle_salarie = s.cle_salarie;
