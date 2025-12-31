# Plan de test — Orchestration Kestra (8 cas S/G/P + garde-fous E/BOOTSTRAP)

> **Statut : VALIDÉ** — Tous les tests ont été exécutés et conformes. Date : 2025-12-30 (Europe/Paris)

## Convention flags

- S = SIMULATION ("1" ou "0")
- G = flag produit par MOD21 (gsheet_changed : "1" si GSheet a changé, sinon "0")
- P = flag produit par MOD22 (param_changed : "1" si config_param_avantage.yml a changé, sinon "0")

Garde-fous techniques (hors matrice S/G/P) :

- E = flag produit par MOD24 (excel_changed : "1" si les Excels RH/Sportive ont changé, sinon "0")
- B = flag produit par MOD25 (bootstrap_rh : "1" si socle RH absent/vidé en base, sinon "0")

## Pré-réglages (recommandé)

- FORCE_WEBHOOK=0
- FORCE_GMAPS=0 (sinon MOD80 peut s’exécuter en dehors des cas S/G/P attendus)
- Pour les cas avec Slack (G=1) : utiliser un webhook Slack de test si besoin

Notes pratiques (pour éviter les faux positifs) :

- Pour tester les cas S/G/P « standards », viser E=0 et B=0 :
  - E=0 : ne pas modifier les fichiers Excel RH/Sportive ; s’assurer que le hash baseline MOD24 existe (faire 1 run de “mise en place” si nécessaire).
  - B=0 : s’assurer que le socle RH est bien présent en base (au moins une fois MOD60 exécuté auparavant).
- Cas 0 = STOP strict : si S=0, G=0, P=0 alors rien ne doit s’exécuter après MOD30 (y compris si FORCE_GMAPS=1).

## Modules (rappel)

- Préambule : MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- Normalisation : MOD40 (uniquement si G=1)
- Simulation : MOD50 (uniquement si S=1)
- Loads data : MOD60 (RH) + MOD61 (params) + MOD62 (activité) si DATA_CHANGED (S=1 ou G=1)
- Cas “P seul” : MOD61 uniquement (pas de MOD60, pas de MOD62)
- DQ tables : MOD70
- Distances : MOD80 (exécuté si DATA_CHANGED, sinon non attendu quand P seul)
- Vues BI/Elig/KPI : MOD90
- DQ BI : MOD71
- Historisation : MOD95 s’exécute pour tout run qui n’est pas un STOP (Cas 0)
- Slack : MOD99 (uniquement si G=1)

---

## Résultats — synthèse

- [x] Cas 0 (S=0, G=0, P=0) — OK
- [x] Cas 1 (S=1, G=0, P=0) — OK
- [x] Cas 2 (S=0, G=1, P=0) — OK
- [x] Cas 3 (S=0, G=0, P=1) — OK
- [x] Cas 4 (S=1, G=1, P=0) — OK
- [x] Cas 5 (S=1, G=0, P=1) — OK
- [x] Cas 6 (S=0, G=1, P=1) — OK
- [x] Cas 7 (S=1, G=1, P=1) — OK

### Tests garde-fous (E/B/FORCE_GMAPS)

- [x] GF1 (E=1 seul) — OK
- [x] GF2 (B=1 seul) — OK
- [x] GF3 (FORCE_GMAPS=1 hors Cas 0) — OK
- [x] GF4 (Cas 0 + FORCE_GMAPS=1 : STOP strict) — OK

---

## Ordre de déroulé recommandé (commencer par S=1)

- Cas 1 → Cas 5 → Cas 4 → Cas 7
- Puis Cas 2 → Cas 6 → Cas 3 → Cas 0
- Enfin, tests garde-fous (E/B/FORCE_GMAPS)

---

## Cas 0 — S=0, G=0, P=0 (STOP)

### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=0 : ne pas modifier config
- (Pré-requis) E=0 et B=0 pour rester en Cas 0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30

### Ne doit pas exécuter

- MOD40, MOD50, MOD60, MOD61, MOD62, MOD70, MOD80, MOD90, MOD71, MOD95, MOD99

---

## Cas 1 — S=1, G=0, P=0

### Paramétrer

- S=1 : SIMULATION=1
- G=0 : ne pas modifier le GSheet
- P=0 : ne pas modifier config
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD50, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95

### Ne doit pas exécuter

- MOD40, MOD60, MOD80, MOD99

---

## Cas 2 — S=0, G=1, P=0

### Paramétrer

- S=0 : SIMULATION=0
- G=1 : modifier le GSheet (déclaratifs d’activités)
- P=0 : ne pas modifier config
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD40, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95, MOD99

### Ne doit pas exécuter

- MOD50, MOD60, MOD80

---

## Cas 3 — S=0, G=0, P=1

### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=1 : modifier config_param_avantage.yml
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD61, MOD70, MOD90, MOD71, MOD95

### Ne doit pas exécuter

- MOD40, MOD50, MOD60, MOD62, MOD80, MOD99

---

## Cas 4 — S=1, G=1, P=0 (Cas 1+2)

### Paramétrer

- S=1 : SIMULATION=1
- G=1 : modifier le GSheet (déclaratifs d’activités)
- P=0 : ne pas modifier config
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD40, MOD50, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95, MOD99

### Ne doit pas exécuter

- MOD60, MOD80

---

## Cas 5 — S=1, G=0, P=1 (Cas 1+3)

### Paramétrer

- S=1 : SIMULATION=1
- G=0 : ne pas modifier le GSheet
- P=1 : modifier config_param_avantage.yml
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD50, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95

### Ne doit pas exécuter

- MOD40, MOD60, MOD80, MOD99

---

## Cas 6 — S=0, G=1, P=1 (Cas 2+3)

### Paramétrer

- S=0 : SIMULATION=0
- G=1 : modifier le GSheet (déclaratifs d’activités)
- P=1 : modifier config_param_avantage.yml
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD40, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95, MOD99

### Ne doit pas exécuter

- MOD50, MOD60, MOD80

---

## Cas 7 — S=1, G=1, P=1 (Cas 1+2+3)

### Paramétrer

- S=1 : SIMULATION=1
- G=1 : modifier le GSheet (déclaratifs d’activités)
- P=1 : modifier config_param_avantage.yml
- (Pré-requis) E=0 et B=0 ; FORCE_GMAPS=0

### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD40, MOD50, MOD61, MOD62, MOD70, MOD90, MOD71, MOD95, MOD99

### Ne doit pas exécuter

- MOD60, MOD80

---

## Tests garde-fous techniques (E/BOOTSTRAP/FORCE_GMAPS)

Objectif : valider que MOD60/MOD80 sont bien pilotés par E/BOOTSTRAP (et que FORCE_GMAPS ne bypass pas Cas 0).

---

### GF1 — E=1 seul (Excel changed, sans S/G/P)

#### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=0 : ne pas modifier config_param_avantage.yml
- Provoquer E=1 : modifier un des fichiers Excel RH/Sportive dans data/raw (changement réel de contenu)
- B=0 : socle RH présent en base
- FORCE_GMAPS=0

#### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD60, MOD61, MOD70, MOD80, MOD90, MOD71, MOD95

#### Ne doit pas exécuter

- MOD40, MOD50, MOD62, MOD99

---

### GF2 — B=1 seul (bootstrap RH, sans S/G/P)

#### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=0 : ne pas modifier config_param_avantage.yml
- E=0 : ne pas modifier les Excels RH/Sportive
- Provoquer B=1 : base « vide RH » (tables RH absentes ou metier.salarie + sec.rh_salarie vides)
- FORCE_GMAPS=0

#### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD60, MOD61, MOD70, MOD80, MOD90, MOD71, MOD95

#### Ne doit pas exécuter

- MOD40, MOD50, MOD62, MOD99

---

### GF3 — FORCE_GMAPS=1 (hors Cas 0) : MOD80 doit s’exécuter même si E=0 et B=0

Exemple proposé : Cas 3 + FORCE_GMAPS=1

#### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=1 : modifier config_param_avantage.yml
- E=0 : ne pas modifier les Excels RH/Sportive
- B=0 : socle RH présent en base
- FORCE_GMAPS=1

#### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30
- MOD61, MOD70, MOD80, MOD90, MOD71, MOD95

#### Ne doit pas exécuter

- MOD40, MOD50, MOD60, MOD62, MOD99

---

### GF4 — Cas 0 + FORCE_GMAPS=1 : ne doit pas bypass le STOP

#### Paramétrer

- S=0 : SIMULATION=0
- G=0 : ne pas modifier le GSheet
- P=0 : ne pas modifier config_param_avantage.yml
- E=0 et B=0
- FORCE_GMAPS=1

#### Doit exécuter

- MOD10, MOD20, MOD21, MOD22, MOD24, MOD25, MOD23, MOD30

#### Ne doit pas exécuter

- MOD40, MOD50, MOD60, MOD61, MOD62, MOD70, MOD80, MOD90, MOD71, MOD95, MOD99
