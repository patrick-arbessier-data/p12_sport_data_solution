# Table de décision — scénarios (S, G, P) + garde-fous (E, BOOTSTRAP_RH)

## Définitions des flags (source de vérité)

- **S (SIMULATION)** : variable d’environnement `SIMULATION` (0/1). `S=1` si `SIMULATION=1`, sinon `0`.
- **G (GSHEET_CHANGED)** : flag calculé par **[MOD21]**. `G=1` si le GSheet extrait diffère du run précédent, sinon `0`.
- **P (PARAM_CHANGED)** : flag calculé par **[MOD22]**. `P=1` si `config_param_avantage.yml` a changé depuis le run précédent, sinon `0`.

### Garde-fous techniques (hors matrice S/G/P)

- **E (EXCEL_CHANGED)** : flag calculé par **[MOD24]**. `E=1` si un fichier Excel d’entrée a changé depuis le run précédent (ou baseline absente), sinon `0`.
- **B (BOOTSTRAP_RH)** : flag calculé par **[MOD25]**. `B=1` si le socle RH n’est pas présent (premier run / tables RH absentes ou vides), sinon `0`.
- **FORCE_GMAPS** : variable d’environnement `FORCE_GMAPS` (0/1). Si `FORCE_GMAPS=1`, forcer l’exécution de **[MOD80]** (sauf Cas 0).

## Préambule (toujours exécuté)

Modules exécutés avant décision (ordre strict) :

- **[MOD10]** Installation des dépendances Python (venv persistée) si nécessaire
- **[MOD20]** Extraction du GSheet (CSV)
- **[MOD21]** Détection “GSheet changed” → produit `G`
- **[MOD22]** Détection “Param changed” → produit `P`
- **[MOD24]** Détection “Excel changed” → produit `E`
- **[MOD25]** Détection “Bootstrap RH” → produit `B`
- **[MOD23]** Débug : log des flags `S/G/P/E/B`
- **[MOD30]** Routage principal : STOP si `S=0 ∧ G=0 ∧ P=0`, sinon exécution de l’aval

---

## Table de décision (8 scénarios S/G/P)

> Les 8 scénarios ci-dessous décrivent la logique **S/G/P**.  
> Les garde-fous **E / BOOTSTRAP_RH / FORCE_GMAPS** s’appliquent **en plus** (voir section dédiée).

| Cas | Flags (S,G,P) | Condition logique | Recette (post-MOD30) | Slack (MOD99) |
| --- | --- | --- | --- | --- |
| Cas 0 | 0,0,0 | `S=0 ∧ G=0 ∧ P=0` | **R0** (STOP) | Non |
| Cas 1 | 1,0,0 | `S=1 ∧ G=0 ∧ P=0` | **R1** | Non |
| Cas 2 | 0,1,0 | `S=0 ∧ G=1 ∧ P=0` | **R2** | Oui |
| Cas 3 | 0,0,1 | `S=0 ∧ G=0 ∧ P=1` | **R3** | Non |
| Cas 4 | 1,1,0 | `S=1 ∧ G=1 ∧ P=0` | **R4** | Oui |
| Cas 5 | 1,0,1 | `S=1 ∧ G=0 ∧ P=1` | **R5** | Non |
| Cas 6 | 0,1,1 | `S=0 ∧ G=1 ∧ P=1` | **R6** | Oui |
| Cas 7 | 1,1,1 | `S=1 ∧ G=1 ∧ P=1` | **R7** | Oui |

---

### R0 — Cas 0 (STOP)

**Conditions (flags) :** `S=0`, `G=0`, `P=0`  
**Modules exécutés :** aucun (STOP après **[MOD30]**)  
**Slack :** non  
**Note :** `FORCE_GMAPS=1` ne bypass pas le STOP Cas 0 (règle validée).

---

### R1 — Simulation seule (Cas 1)

**Conditions (flags) :** `S=1`, `G=0`, `P=0`  
**Modules exécutés (ordre strict) :**

1. **[MOD50]** Simulation
2. **[MOD61]** Load paramètres avantages
3. **[MOD62]** Load table activités
4. **[MOD70]** DQ tables (Soda)
5. **[MOD90]** Prépa vues BI / éligibilité / KPI
6. **[MOD71]** DQ vues BI (Soda)
7. **[MOD95]** Historisation

**Slack :** non (règle validée : pas de Slack en Cas 1)

---

### R2 — GSheet changé seul (Cas 2)

**Conditions (flags) :** `S=0`, `G=1`, `P=0`  
**Modules exécutés (ordre strict) :**

1. **[MOD40]** Normalisation GSheet
2. **[MOD61]** Load paramètres avantages
3. **[MOD62]** Load table activités
4. **[MOD70]** DQ tables (Soda)
5. **[MOD90]** Prépa vues BI / éligibilité / KPI
6. **[MOD71]** DQ vues BI (Soda)
7. **[MOD95]** Historisation
8. **[MOD99]** Slack *(uniquement car `G=1`)*

**Slack :** oui (uniquement car `G=1`)

---

### R3 — Param changé seul (Cas 3)

**Conditions (flags) :** `S=0`, `G=0`, `P=1`  
**Modules exécutés (ordre strict) :**

1. **[MOD61]** Load paramètres avantages
2. **[MOD70]** DQ tables (Soda)
3. **[MOD90]** Prépa vues BI / éligibilité / KPI
4. **[MOD71]** DQ vues BI (Soda)
5. **[MOD95]** Historisation

**Slack :** non

---

### R4 — Simulation + GSheet changé (Cas 4)

**Conditions (flags) :** `S=1`, `G=1`, `P=0`  
**Modules exécutés (ordre strict) :**

1. **[MOD40]** Normalisation GSheet
2. **[MOD50]** Simulation
3. **[MOD61]** Load paramètres avantages
4. **[MOD62]** Load table activités
5. **[MOD70]** DQ tables (Soda)
6. **[MOD90]** Prépa vues BI / éligibilité / KPI
7. **[MOD71]** DQ vues BI (Soda)
8. **[MOD95]** Historisation
9. **[MOD99]** Slack *(uniquement car `G=1`)*

**Slack :** oui (uniquement car `G=1`)

---

### R5 — Simulation + Param changé (Cas 5)

**Conditions (flags) :** `S=1`, `G=0`, `P=1`  
**Modules exécutés (ordre strict) :**

1. **[MOD50]** Simulation
2. **[MOD61]** Load paramètres avantages
3. **[MOD62]** Load table activités
4. **[MOD70]** DQ tables (Soda)
5. **[MOD90]** Prépa vues BI / éligibilité / KPI
6. **[MOD71]** DQ vues BI (Soda)
7. **[MOD95]** Historisation

**Slack :** non

---

### R6 — GSheet changé + Param changé (Cas 6)

**Conditions (flags) :** `S=0`, `G=1`, `P=1`  
**Modules exécutés (ordre strict) :**

1. **[MOD40]** Normalisation GSheet
2. **[MOD61]** Load paramètres avantages
3. **[MOD62]** Load table activités
4. **[MOD70]** DQ tables (Soda)
5. **[MOD90]** Prépa vues BI / éligibilité / KPI
6. **[MOD71]** DQ vues BI (Soda)
7. **[MOD95]** Historisation
8. **[MOD99]** Slack *(uniquement car `G=1`)*

**Slack :** oui (uniquement car `G=1`)

---

### R7 — Simulation + GSheet changé + Param changé (Cas 7)

**Conditions (flags) :** `S=1`, `G=1`, `P=1`  
**Modules exécutés :** identiques à **R4** (les paramètres sont rechargés dans tous les cas)  
**Slack :** oui

---

## Garde-fous techniques — impact sur l’exécution (hors matrice S/G/P)

### Règle MOD60 (socle RH)

- Exécuter **[MOD60]** si `E=1` **ou** `B=1`.
- Sinon, ne pas exécuter **[MOD60]**.

### Règle MOD80 (distances)

- Exécuter **[MOD80]** si `FORCE_GMAPS=1` **ou** `E=1` **ou** `B=1`.
- Ne pas exécuter **[MOD80]** en Cas 0 (`S=0 ∧ G=0 ∧ P=0`), même si `FORCE_GMAPS=1`.

### Points d’insertion (ordre relatif)

Quand applicable :

- **[MOD60]** s’exécute **avant** **[MOD61]**.
- **[MOD80]** s’exécute **après** **[MOD70]** et **avant** **[MOD90]**.

---

## Notes

- **Slack (MOD99)** ne s’exécute que si `G=1`.
- Pas de risque de publication de lignes simulées : la publication Slack est basée sur le dernier fichier GSheet normalisé (la simulation n’alimente pas ce fichier).
