# Documentation — Résultats EDA complets
## Source : `notebooks/01_EDA.ipynb` sur `donnee/staging_fact_route_analysis.csv`

---

## Cellule 0 — Imports et chargement

```python
import pandas as pd        # manipulation de tableaux de données
import numpy as np         # calculs mathématiques
import matplotlib.pyplot as plt  # création de graphiques
import seaborn as sns      # graphiques statistiques avancés

df = pd.read_csv('../donnee/staging_fact_route_analysis.csv')
```

**Ce que ça fait :** On charge les bibliothèques nécessaires puis on lit le fichier CSV dans un DataFrame pandas (tableau de données en mémoire). Chaque ligne = un corridor, chaque colonne = une information sur ce corridor.

---

## Cellule 1 — Vue d'ensemble du dataset

```python
info = pd.DataFrame({
    'dtype': df.dtypes,           # type de chaque colonne
    'non_null': df.count(),       # nombre de valeurs remplies
    'null': df.isnull().sum(),    # nombre de valeurs vides
    'null_%': (df.isnull().sum() / len(df) * 100).round(1)  # % de vides
})
```

**Ce que ça fait :** On crée un tableau récapitulatif qui montre pour chaque colonne son type et combien de valeurs sont manquantes.

**Résultat :**

| Colonne | Type | Nulls | % Nulls |
|---|---|---|---|
| `origin` | Texte | 0 | 0% |
| `destination` | Texte | 0 | 0% |
| `vehicule_type` | Texte | 0 | 0% |
| `distance_km` | Nombre | 0 | 0% |
| `co2_train_kg` | Nombre | 0 | 0% |
| `co2_avion_kg` | Nombre | **132** | **8.7%** |
| `co2_saved_kg` | Nombre | **132** | **8.7%** |
| `is_substitutable` | Entier (0/1) | 0 | 0% |
| `traffic_share_pct` | Nombre | **1509** | **100%** |
| `station_lat/lon` | Coordonnées GPS | 0 | 0% |

**Total : 1509 corridors, 13 colonnes.**

**Conclusion :** Le dataset est propre. Les 132 valeurs manquantes sur `co2_avion_kg` correspondent aux corridors sans aéroport proche — ces lignes n'ont donc pas de vol comparé. `traffic_share_pct` est entièrement vide → supprimée.

```python
df[['distance_km','co2_train_kg','co2_avion_kg','co2_saved_kg']].describe().round(2)
```

**Ce que ça fait :** `.describe()` calcule automatiquement les statistiques de base (min, max, moyenne, médiane, écart-type) pour les colonnes numériques.

---

## Cellule 2 — Distribution de la cible `is_substitutable`

```python
counts = df['is_substitutable'].value_counts()
# value_counts() compte combien de fois chaque valeur apparaît (0 ou 1)

axes[0].bar(labels, [counts[0], counts[1]], color=colors)
# bar() trace un graphique en barres

axes[1].pie([counts[0], counts[1]], autopct='%1.1f%%')
# pie() trace un camembert avec les pourcentages
```

**Ce que ça fait :** On compte et on visualise le nombre de corridors substituables vs non-substituables.

**Résultat :**

| Classe | Nombre | Pourcentage |
|---|---|---|
| Substituable (1) | **1295** | **85.8%** |
| Non-substituable (0) | **214** | **14.2%** |

**Pourquoi c'est important :** Ce déséquilibre 85/14 signifie qu'un modèle qui dirait toujours "substituable" aurait 85.8% de précision sans rien apprendre. C'est pour ça qu'on utilise `class_weight='balanced'` dans l'entraînement — ça force le modèle à accorder autant d'importance aux 214 cas "non-substituable" qu'aux 1295 cas "substituable".

---

## Cellule 3 — Distributions des features numériques

```python
ax.hist(data, bins=40, color=color, alpha=0.8)
# hist() trace un histogramme avec 40 barres

ax.axvline(data.mean(), color='black', linestyle='--')
# axvline() trace une ligne verticale à la moyenne

ax.axvline(data.median(), color='orange', linestyle='--')
# deuxième ligne pour la médiane
```

**Ce que ça fait :** Pour chaque variable numérique, on trace la distribution de ses valeurs avec la moyenne (noir) et la médiane (orange) marquées.

**Résultats :**

### `distance_km`
| Stat | Valeur |
|---|---|
| Minimum | 100 km |
| Maximum | 1154 km |
| Moyenne | 192 km |
| Médiane | **132 km** |

→ 75% des corridors font moins de 173 km. Distribution très asymétrique vers la droite (beaucoup de courts trajets, quelques très longs).

### `co2_train_kg`
| Stat | Valeur |
|---|---|
| Minimum | 0.09 kg |
| Maximum | 89.59 kg |
| Moyenne | 4.35 kg |
| Médiane | **1.33 kg** |

→ Très faible grâce au nucléaire français (0.09 kg CO2/kWh). Le max à 89.59 kg = long trajet en pays à fort mix charbon (Pologne, Allemagne).

### `co2_avion_kg`
| Stat | Valeur |
|---|---|
| Minimum | 64.20 kg |
| Maximum | 240.82 kg |
| Moyenne | 94.21 kg |
| Médiane | **85.45 kg** |

→ Beaucoup plus élevé que le train. Varie selon la distance (kérosène proportionnel au trajet).

### `co2_saved_kg`
| Stat | Valeur |
|---|---|
| Minimum | 58.45 kg |
| Maximum | 235.39 kg |
| Moyenne | 89.57 kg |
| Médiane | **84.31 kg** |

→ En prenant le train au lieu de l'avion, un passager économise **en moyenne 90 kg de CO2** par trajet. C'est le signal principal du Modèle 2.

---

## Cellule 4 — CO2 Train vs Avion par corridor

```python
df_both = df.dropna(subset=['co2_avion_kg'])
# dropna() supprime les lignes où co2_avion_kg est vide
# Résultat : 1377 corridors avec les deux modes comparés

for val, grp in df_both.groupby('is_substitutable'):
    axes[0].scatter(grp['distance_km'], grp['co2_saved_kg'], c=colors_map[val])
# scatter() trace un nuage de points : distance en X, CO2 économisé en Y
# couleur verte = substituable, rouge = non-substituable

sns.boxplot(data=df_both, x='vehicule_type', y='co2_saved_kg')
# boxplot() montre la distribution du CO2 économisé par type de train
```

**Ce que ça fait :** On compare visuellement les deux modes de transport.

**Résultats :**

| Stat | Valeur |
|---|---|
| Corridors avec train ET avion comparés | **1377** |
| Facteur CO2 avion/train (médiane) | **×73** |
| Gain CO2 moyen | **89.6 kg/passager** |

**CO2 économisé médian par type de train :**

| Type | CO2 économisé médian |
|---|---|
| Nightjet | **141.6 kg** (longs trajets → avion très polluant) |
| InterCity | **117.1 kg** |
| EuroNight | **90.3 kg** |
| Train Longue Distance | **83.8 kg** |
| Train Longue Distance Nuit | **78.2 kg** |

→ Le Nightjet économise le plus de CO2 car il couvre de longues distances où l'avion émet beaucoup.

---

## Cellule 5 — Répartition par type de véhicule

```python
vt_counts = df['vehicule_type'].value_counts()
# Compte les corridors par type de train

sub_rate = df.groupby('vehicule_type')['is_substitutable'].mean()
# groupby() regroupe par type, mean() calcule la proportion de 1
# → donne le % de corridors substituables par type
```

**Ce que ça fait :** On analyse si le type de train influence la substituabilité.

**Résultats :**

| Type de train | Nb corridors | % substituables |
|---|---|---|
| Train Longue Distance | **706** | **92.5%** |
| Train Longue Distance Nuit | **424** | **86.8%** |
| EuroNight | 266 | 79.3% |
| InterCity | 95 | 61.1% |
| Nightjet | 18 | **27.8%** |

→ Les trains régionaux français (courts trajets) sont très substituables. Les Nightjet internationaux (Paris-Vienne, Hambourg-Rome) dépassent souvent les 600 km → rarement substituables selon la loi française.

---

## Cellule 6 — Matrice de corrélations

```python
corr = df[num_cols].corr()
# corr() calcule le coefficient de Pearson entre chaque paire de colonnes
# Valeur entre -1 et 1 : 1 = corrélation parfaite, -1 = inverse parfait, 0 = aucun lien

mask = np.triu(np.ones_like(corr, dtype=bool))
# On masque le triangle supérieur pour éviter les doublons

sns.heatmap(corr, annot=True, cmap='RdYlGn', center=0)
# heatmap() affiche la matrice en couleurs : vert = corrélation positive, rouge = négative
```

**Ce que ça fait :** On mesure les liens entre toutes les variables numériques.

**Matrice complète :**

| | distance_km | co2_train_kg | co2_avion_kg | co2_saved_kg | is_substitutable |
|---|---|---|---|---|---|
| `distance_km` | 1.00 | 0.84 | 0.92 | 0.81 | **-0.40** |
| `co2_train_kg` | 0.84 | 1.00 | 0.76 | 0.53 | **-0.39** |
| `co2_avion_kg` | 0.92 | 0.76 | 1.00 | 0.95 | **-0.77** |
| `co2_saved_kg` | 0.81 | 0.53 | 0.95 | 1.00 | **-0.68** |
| `is_substitutable` | -0.40 | -0.39 | -0.77 | -0.68 | 1.00 |

**Lecture :**
- `co2_avion_kg` est la plus corrélée avec `is_substitutable` (-0.77) → plus l'avion émet, moins le corridor est substituable (car distance longue > 600 km)
- `distance_km` et `co2_avion_kg` sont très liées (0.92) → plus c'est loin, plus l'avion pollue (logique)
- `co2_saved_kg` et `co2_avion_kg` sont quasi identiques (0.95) → le CO2 économisé dépend surtout du CO2 de l'avion

---

## Cellule 7 — Valeurs manquantes

```python
nulls = df.isnull().sum()
# isnull() retourne True pour chaque valeur vide, sum() les compte

null_df = pd.DataFrame({'count': nulls, '%': nulls_pct}).query('count > 0')
# On garde seulement les colonnes qui ont des valeurs manquantes
```

**Ce que ça fait :** On liste uniquement les colonnes qui ont des données manquantes et on explique quoi en faire.

**Résultat :**

| Colonne | Valeurs manquantes | % | Décision |
|---|---|---|---|
| `co2_avion_kg` | 132 | 8.7% | Garder pour Modèle 1, exclure du Modèle 2 |
| `co2_saved_kg` | 132 | 8.7% | Même logique que co2_avion |
| `traffic_share_pct` | 1509 | **100%** | **Supprimée des features** |

**Détail sur les 132 nulls :**
Les 132 corridors sans `co2_avion_kg` sont exactement les corridors où `is_substitutable = 0` — aucun ne vaut 1. Cela confirme la logique : si aucun aéroport n'est proche, il n'y a pas de vol, donc le corridor ne peut pas être substitué (pas de comparaison possible).

---

## Cellule 8 — Visualisation géographique

```python
import folium

m = folium.Map(location=[46.5, 2.5], zoom_start=5)
# Crée une carte centrée sur la France

folium.PolyLine(
    locations=[[lat1, lon1], [lat2, lon2]],
    color='#2ecc71',   # vert = substituable
    tooltip="Paris → Lyon (132km, CO2 saved: 83kg)"
).add_to(m)
# PolyLine trace une ligne entre deux points GPS sur la carte

m.save('../docs/carte_corridors.html')
# Sauvegarde la carte en fichier HTML interactif
```

**Ce que ça fait :** On trace une carte interactive avec tous les corridors. Chaque ligne relie deux gares, en vert si substituable, en rouge sinon. En survolant une ligne, on voit les détails du corridor (distance, CO2 économisé).

**Résultat :** Carte sauvegardée dans `docs/carte_corridors.html` — livrable visuel pour la soutenance.

---

## Cellule 9 — Tableau des variables retenues

```python
variables = pd.DataFrame([
    {'Variable': 'distance_km', 'Rôle': 'Feature', 'Justification': '...'},
    ...
])
```

**Ce que ça fait :** On crée le tableau officiel des variables retenues, livrable obligatoire du sujet MSPR.

**Tableau final :**

| Variable | Gardée ? | Rôle | Justification |
|---|---|---|---|
| `distance_km` | ✅ | Feature | Corrélation -0.40 avec cible, détermine si < 600km |
| `co2_train_kg` | ✅ | Feature | Varie selon pays (mix électrique) et type de train |
| `co2_avion_kg` | ✅ | Feature | Présence = vol existant. NULL → is_substitutable=0 |
| `co2_saved_kg` | ✅ | **Cible Modèle 2** | co2_avion - co2_train, aucun biais circulaire |
| `vehicule_type` | ✅ | Feature | TGV vs Train Nuit = profils CO2 et distances très différents |
| `is_substitutable` | ✅ | **Cible Modèle 1** | Basé loi FR 2023 : distance ≤ 600km ET vol existant |
| `traffic_share_pct` | ❌ | Supprimée | 100% NULL, aucune donnée disponible |
| `station_lat/lon` | ✅ | Carte uniquement | Coordonnées GPS pour visualisation, pas pour le ML |

---

## Synthèse EDA

| Point clé | Valeur | Impact ML |
|---|---|---|
| Taille dataset | 1509 corridors | Suffisant pour modèles simples |
| Déséquilibre classes | 85.8% / 14.2% | `class_weight='balanced'` obligatoire |
| Feature la plus corrélée | `co2_avion_kg` (-0.77) | Feature principale Modèle 1 |
| Gain CO2 moyen | 89.6 kg/passager | Signal fort pour Modèle 2 |
| Facteur CO2 avion/train | ×73 (médiane) | Argument fort pour la soutenance |
| Corridors < 200km | 1205/1509 (80%) | Explique le fort taux de substituabilité |
| `traffic_share_pct` | 100% NULL | Supprimée |
| `co2_avion_kg` manquant | 132 lignes = 132 is_sub=0 | Logique confirmée |
