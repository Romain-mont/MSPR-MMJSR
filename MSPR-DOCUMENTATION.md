# Documentation — EDA & Préparation ML
## Source : `notebooks/01_EDA.ipynb` — `donnee/staging_fact_route_analysis.csv`

---

## Cellule 0 — Imports et chargement

```python
import pandas as pd, numpy as np, matplotlib.pyplot as plt
import seaborn as sns, folium, os
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split

df = pd.read_csv('../donnee/staging_fact_route_analysis.csv')
```

**Ce que ça fait :** Charge toutes les bibliothèques nécessaires (visualisation, ML, carte) puis lit le fichier CSV enrichi. Chaque ligne = un corridor ferroviaire, chaque colonne = une information sur ce corridor.

**Résultat :** Dataset chargé — **2486 corridors, 18 colonnes**

| Colonne | Contenu |
|---|---|
| `origin` / `destination` | Noms des gares |
| `vehicule_type` | Type de train (TGV, InterCity, EuroNight...) |
| `distance_km` | Distance du corridor en km |
| `co2_train_kg` | CO2 émis par le train (EcoPassenger) |
| `co2_avion_kg` | CO2 émis par l'avion (si vol existant) |
| `co2_saved_kg` | co2_avion - co2_train |
| `is_substitutable` | **Cible M1** : 1 si distance ≤ 600km ET vol existant |
| `origin_station_traffic` | Fréquentation annuelle gare départ (SNCF) |
| `origin_city_population` | Population ville départ (INSEE/GeoNames) |
| `dest_station_traffic` | Fréquentation annuelle gare arrivée |
| `dest_city_population` | Population ville arrivée |
| `station_lat/lon` | Coordonnées GPS (carte uniquement) |

---

## Cellule 1.1 — Vue d'ensemble du dataset

```python
info = pd.DataFrame({
    'Type': df.dtypes,
    'Non-null': df.count(),
    'Null': df.isnull().sum(),
    'Null %': (df.isnull().sum() / len(df) * 100).round(1)
})
```

**Ce que ça fait :** Tableau récapitulatif des types de colonnes et valeurs manquantes.

**Résultat — Valeurs manquantes :**

| Colonne | Nulls | % | Décision |
|---|---|---|---|
| `co2_train_kg` | 135 | 5.4% | Filtrer (pas de coords GPS → distance incalculable) |
| `co2_avion_kg` | 339 | 13.6% | Garder M1, exclure M2. NULL → 0 |
| `co2_saved_kg` | 339 | 13.6% | Idem |
| `station_lat/lon` | 111 | 4.5% | Exclure de la carte Folium |
| `origin_station_traffic` | 1173 | 47.2% | Gares non-françaises → 0 |
| `origin_city_population` | 714 | 28.7% | Villes non-reconnues → 0 |
| `dest_station_traffic` | 1164 | 46.8% | Idem |
| `dest_city_population` | 711 | 28.6% | Idem |

---

## Cellule 1.2 — Distribution de la cible `is_substitutable`

```python
counts = df['is_substitutable'].value_counts()
axes[0].bar(labels, [counts[0], counts[1]], color=colors)
axes[1].pie([counts[0], counts[1]], autopct='%1.1f%%')
```

**Ce que ça fait :** Compte et visualise le nombre de corridors substituables vs non-substituables.

**Résultat :**

| Classe | Nombre | Pourcentage |
|---|---|---|
| Substituable (1) | **2001** | **80.5%** |
| Non-substituable (0) | **485** | **19.5%** |

**Pourquoi c'est important :** Un modèle qui prédit toujours "1" aurait 80.5% de précision sans rien apprendre. On utilise `class_weight='balanced'` pour forcer le modèle à prendre en compte les 485 cas "non-substituable".

---

## Cellule 1.3 — Distributions CO2 et distance

```python
ax.hist(data, bins=40, color=color, alpha=0.8)     # histogramme
ax.axvline(data.mean(), color='black', linestyle='--')   # ligne moyenne
ax.axvline(data.median(), color='orange', linestyle='--') # ligne médiane
```

**Ce que ça fait :** Histogrammes avec moyenne (noir) et médiane (orange) pour chaque feature numérique.

**Résultats :**

| Feature | Min | Médiane | Moyenne | Max |
|---|---|---|---|---|
| `distance_km` | 100 km | 135 km | 204 km | 1154 km |
| `co2_train_kg` | 0.09 kg | 1.37 kg | 4.70 kg | 89.59 kg |
| `co2_avion_kg` | 64.20 kg | 86.33 kg | 96.45 kg | 240.82 kg |
| `co2_saved_kg` | 53.33 kg | 85.02 kg | 91.43 kg | 237.16 kg |

**Observations :**
- Distance médiane 135 km : 75% des corridors font moins de 195 km → beaucoup de courts trajets
- CO2 train médiane 1.37 kg : très faible grâce au nucléaire français
- CO2 économisé médiane 85 kg : signal très fort pour la régression

---

## Cellule 1.4 — Distribution fréquentation et population

```python
axes[0].hist(traffic / 1e6, bins=30)  # fréquentation en millions
axes[1].hist(pop / 1e3, bins=30)      # population en milliers
```

**Ce que ça fait :** Visualise les données démographiques pour comprendre leur distribution et couverture.

**Résultats :**

| Feature | Couverture | Médiane |
|---|---|---|
| `origin_station_traffic` | 52.8% | 230 185 voyageurs/an |
| `origin_city_population` | 71.3% | 48 825 habitants |
| `dest_station_traffic` | 53.2% | 230 185 voyageurs/an |
| `dest_city_population` | 71.4% | 54 849 habitants |

→ Les NULL correspondent aux gares non-françaises (pas de données SNCF) et aux villes non reconnues dans INSEE/GeoNames. Stratégie : remplacer par 0.

---

## Cellule 1.5 — CO2 économisé vs Distance

```python
axes[0].scatter(grp['distance_km'], grp['co2_saved_kg'], c=colors_map[val])
axes[0].axvline(600, color='black', linestyle='--')  # seuil loi FR
sns.boxplot(data=df_both, x='vehicule_type', y='co2_saved_kg')
```

**Ce que ça fait :** Nuage de points coloré par classe + boxplot par type de train.

**Résultats :**

- 2147 corridors avec les deux modes comparés
- Facteur CO2 avion/train (médiane) : **×63**
- Gain CO2 moyen : **91.4 kg/passager**

| Type de train | CO2 économisé médian |
|---|---|
| Nightjet | ~141 kg |
| InterCity | ~117 kg |
| EuroNight | ~90 kg |
| Train Longue Distance | ~84 kg |
| Train Longue Distance Nuit | ~78 kg |

---

## Cellule 1.6 — Matrice de corrélations

```python
corr = df[corr_cols].corr()
sns.heatmap(corr, annot=True, cmap='RdYlGn', center=0)
```

**Ce que ça fait :** Coefficient de Pearson entre toutes les variables numériques. Vert = corrélation positive, rouge = négative.

**Corrélations avec `is_substitutable` (par ordre croissant) :**

| Feature | Corrélation | Interprétation |
|---|---|---|
| `co2_avion_kg` | **-0.782** | Plus l'avion émet = trajet long > 600km = non-substituable |
| `co2_saved_kg` | -0.710 | Lié à co2_avion |
| `distance_km` | -0.413 | Plus c'est loin, moins c'est substituable |
| `co2_train_kg` | -0.353 | Lié à la distance |
| `origin_city_population` | -0.170 | Les grandes villes ont plus de vols longs |
| `origin_station_traffic` | -0.012 | Quasi pas de lien direct |

---

## Cellule 1.7 — Valeurs manquantes (stratégie)

```python
null_df = pd.DataFrame({'Nulls': nulls, '%': nulls_pct}).query('Nulls > 0')
```

**Stratégie finale :**
- `co2_train_kg` NULL (135) → **filtrer** ces lignes (coords GPS absentes)
- `co2_avion_kg` NULL (339) → **0** pour M1, **exclure** pour M2
- `*_station_traffic` NULL → **0** (gares non-françaises sans données SNCF)
- `*_city_population` NULL → **0** (villes non reconnues dans les référentiels)

---

## Cellule 1.8 — Carte géographique Folium

```python
m = folium.Map(location=[46.5, 2.5], zoom_start=5)
folium.PolyLine(locations=[[lat1,lon1],[lat2,lon2]], color='#2ecc71').add_to(m)
m.save('../docs/carte_corridors.html')
```

**Ce que ça fait :** Carte interactive HTML. Vert = substituable, rouge = non-substituable. Survol = détails du corridor.

---

## Cellule 1.9 — Tableau des variables retenues (livrable obligatoire)

| Variable | Gardée ? | Rôle | Justification |
|---|---|---|---|
| `distance_km` | ✅ | Feature | Corrélation -0.41 avec cible |
| `co2_train_kg` | ✅ | Feature | Varie selon mix électrique pays |
| `co2_avion_kg` | ✅ | Feature | NULL = pas de vol = non-substituable |
| `co2_saved_kg` | ✅ | **Cible M2** | co2_avion - co2_train, pas de biais |
| `vehicule_type` | ✅ | Feature | Encodé LabelEncoder |
| `origin_station_traffic` | ✅ | Feature | Proxy demande, NULL → 0 |
| `origin_city_population` | ✅ | Feature | Proxy densité, NULL → 0 |
| `dest_station_traffic` | ✅ | Feature | Idem destination |
| `dest_city_population` | ✅ | Feature | Idem destination |
| `is_substitutable` | ✅ | **Cible M1** | Loi FR 2023 : ≤ 600km ET vol existant |
| `station_lat/lon` | ✅ | Carte seule | Pas dans le ML |
| `traffic_share_pct` | ❌ | Supprimée | 100% NULL |

---

## Section 2 — Préparation ML

### Cellule 2.1 — Sélection et nettoyage

```python
df_ml['co2_avion_kg'] = df_ml['co2_avion_kg'].fillna(0)
for col in ['origin_station_traffic', ...]:
    df_ml[col] = df_ml[col].fillna(0)
```

→ NULL remplacés par 0, `traffic_share_pct` supprimée.

### Cellule 2.2 — Encodage `vehicule_type`

```python
le = LabelEncoder()
df_ml['vehicule_type'] = le.fit_transform(df_ml['vehicule_type'])
joblib.dump(le, '../models/label_encoder_vehicule.joblib')
```

→ EuroNight=0, InterCity=1, Nightjet=2, Train Longue Distance=3, Train Longue Distance Nuit=4

### Cellule 2.3 — Split train/val/test Modèle 1

```python
X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.30, stratify=y, random_state=42)
X_val, X_test, y_val, y_test     = train_test_split(X_temp, y_temp, test_size=0.50, random_state=42)
```

**Stratifié** = même proportion 80/20 dans chaque split.

| Split | Lignes |
|---|---|
| Train (70%) | ~1484 |
| Validation (15%) | ~318 |
| Test (15%) | ~318 |

### Cellule 2.4 — Dataset Modèle 2

Filtre les 339 lignes sans `co2_saved_kg` → **2147 corridors** utilisables.

### Cellule 2.5 — Normalisation

```python
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)  # fit sur le train uniquement
X_test_scaled  = scaler.transform(X_test)        # applique sur test
joblib.dump(scaler, '../models/scaler.joblib')
```

**Pourquoi normaliser :** Les algorithmes comme la Régression Logistique et le MLP sont sensibles à l'échelle. `distance_km` (100-1154) et `origin_station_traffic` (22-257M) ont des ordres de grandeur très différents → StandardScaler ramène tout à moyenne=0, écart-type=1.

### Cellule 2.6 — Sauvegarde

```python
X_train_m1.to_csv('../data/train_m1.csv')  # prêt pour train_model1.py
X_test_m1.to_csv('../data/test_m1.csv')
joblib.dump(scaler, '../models/scaler.joblib')
```

---

## Synthèse

| Point clé | Valeur | Impact ML |
|---|---|---|
| Dataset | 2486 corridors, 18 colonnes | Source unique enrichie |
| Après filtre NULL co2_train | 2351 corridors | Base pour M1 |
| Déséquilibre classes | 80.5% / 19.5% | `class_weight='balanced'` |
| Feature la plus corrélée | `co2_avion_kg` (-0.782) | Feature principale M1 |
| Gain CO2 moyen | 91.4 kg/passager | Signal fort M2 |
| Couverture fréquentation | ~53% | NULL → 0 |
| Couverture population | ~71% | NULL → 0 |
| Split | 70/15/15 stratifié, seed=42 | Reproductible |
| Livrables sauvegardés | scaler.joblib, label_encoder.joblib, train/test CSV | Réutilisables |
