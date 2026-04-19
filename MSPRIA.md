# MSPR IA — ObRail Europe
## Idée projet : Continuité et lisibilité du réseau ferroviaire européen

---

## Contexte

ObRail Europe souhaite évaluer la continuité du réseau ferroviaire européen et identifier les gaps entre **infrastructure physique existante** et **services commerciaux réels**. Le projet répond à trois besoins concrets :

**Besoin 1 — Reconstruire les trajets réels avec leurs arrêts intermédiaires**
Les données sources (GTFS, Back-on-Track) contiennent les arrêts de chaque train. L'objectif est de reconstituer les routes complètes (origine → stops intermédiaires → destination) pour avoir une vision fidèle du réseau tel qu'il fonctionne réellement.

**Besoin 2 — Proposer le meilleur trajet quand il n'existe pas de direct**
Pour une paire origine-destination avec plusieurs options de correspondances, le système choisit automatiquement le trajet le plus pertinent selon deux critères combinables :
- Minimum de correspondances (confort voyageur)
- Minimum de CO2 (impact environnemental)

> Exemple : Montélimar → Bordeaux — plusieurs chemins via correspondances existent.
> Le système propose les options et recommande le meilleur compromis correspondances/CO2.

**Besoin 3 — Détecter les gaps entre infrastructure physique et service commercial**
Certaines liaisons disposent d'une voie ferrée physique existante mais aucun opérateur ne propose de service direct. Le système identifie ces gaps et évalue le bénéfice environnemental qu'un service direct apporterait.

> Exemple : Limoges → Montpellier — une voie ferrée physique existe, aucun train direct n'est commercialisé. Notre système détecte ce gap et calcule le CO2 économisé si un service direct était créé.

---

## Architecture du projet

```
Données brutes (GTFS + OpenRailwayMap)
        ↓
  ETL / Reconstruction des routes avec stops
        ↓
  Graphe ferroviaire européen (NetworkX)
        ↓
  Pathfinding optimal (Dijkstra)
        ↓
  ┌─────────────────┬──────────────────────┐
  │   ML 1          │   ML 2               │
  │   Clustering    │   Régression CO2     │
  │   des gares     │   trajet reconstruit │
  └─────────────────┴──────────────────────┘
        ↓
  API REST /predict
        ↓
  Visualisation carte Europe interactive
```

---

## Étapes détaillées

### Étape 1 — ETL : Reconstruction des routes avec stops

**Sources de données :**
- `data/raw/backontrack_csv/back_on_track_trip_stop.csv` — séquences d'arrêts avec horaires
- `data/raw/backontrack_csv/back_on_track_trips.csv` — colonne `via` avec tous les arrêts intermédiaires
- `data/raw/backontrack_csv/back_on_track_stops.csv` — coordonnées GPS des gares
- `data/raw/mobility_gtfs/*/stop_times.txt` — horaires détaillés par provider (86K+ lignes)

**Ce qu'on produit :**
- Un dataset enrichi avec tous les arrêts intermédiaires par trajet
- Les horaires d'arrivée/départ à chaque gare
- Les coordonnées GPS de chaque gare

**Livrable :** `data/processed/routes_with_stops.csv`

---

### Étape 2 — Graphe ferroviaire européen

**Outil :** NetworkX

```python
import networkx as nx

G = nx.DiGraph()
# Nœuds = gares (avec coordonnées, pays, nom)
G.add_node(station_id, lat=lat, lon=lon, name=name, country=country)
# Arêtes = segments directs entre deux gares consécutives
G.add_edge(origin, destination,
           co2_kg=co2, distance_km=dist,
           duration_min=duration, train_type=type)
```

**Métriques calculées sur le graphe :**
- `degree_centrality` — nombre de liaisons directes par gare
- `betweenness_centrality` — nb de trajets optimaux passant par la gare
- `closeness_centrality` — proximité moyenne à toutes les autres gares

**Livrable :** graphe NetworkX sérialisé + tableau des métriques par gare

---

### Étape 3 — Pathfinding optimal (Dijkstra multi-objectif)

**Cas d'usage :** Montélimar → Bordeaux — plusieurs options de correspondances existent, choisir le meilleur trajet.

Le problème est **multi-objectif** : minimiser les correspondances ET le CO2 peut être contradictoire.

```
Option A : 1 correspondance (Lyon)              → CO2 = 4.2 kg  ← moins de changements
Option B : 2 correspondances (Nîmes, Toulouse)  → CO2 = 2.8 kg  ← moins polluant
```

**Approche retenue : score pondéré paramétrable**

```python
def compute_score(path, alpha=0.5):
    """
    alpha = 0   → priorité CO2
    alpha = 1   → priorité minimum de correspondances
    alpha = 0.5 → équilibre (défaut)
    """
    co2_norm = path.co2_total / co2_max_reseau
    stops_norm = path.nb_correspondances / stops_max_reseau
    return alpha * stops_norm + (1 - alpha) * co2_norm

# Trois trajets candidats retournés à l'utilisateur :
path_min_co2   = nx.shortest_path(G, origin, destination, weight='co2_kg')
path_min_stops = nx.shortest_path(G, origin, destination, weight='nb_correspondances')
path_balanced  = best_path_by_score(G, origin, destination, alpha=0.5)
```

**Ce que retourne l'API pour Montélimar → Bordeaux :**

| Option | Correspondances | CO2 | Durée | Score équilibré |
|---|---|---|---|---|
| Moins de changements | 1 (Lyon) | 4.2 kg | 3h10 | 0.61 |
| Moins polluant | 2 (Nîmes, Toulouse) | 2.8 kg | 4h05 | 0.58 |
| **Équilibré (recommandé)** | **1 (Lyon)** | **3.9 kg** | **3h20** | **0.48** |

L'utilisateur peut ajuster le paramètre `alpha` selon ses priorités.

**Livrable :** fonction `find_optimal_route(origin, destination, alpha=0.5)` → retourne les 3 options avec comparatif CO2 / correspondances / durée

---

### Étape 4 — Détection des gaps infrastructure vs service

**Source :** OpenRailwayMap via API Overpass (OSM)

```python
import requests

query = """
[out:json];
way["railway"="rail"]["usage"="main"]({{bbox}});
out geom;
"""
response = requests.get("https://overpass-api.de/api/interpreter", params={"data": query})
```

**Logique de détection :**
1. Récupérer les voies ferrées physiques d'Europe (OpenRailwayMap)
2. Comparer avec les services commerciaux du graphe
3. Identifier les segments où une voie existe **sans service direct**
4. Calculer le CO2 économisé si un service direct était créé (vs correspondance actuelle)

**Visualisation :**
- Trait gris = voie ferrée physique
- Trait coloré = service commercial actif
- Trait rouge pointillé = voie sans service direct → candidat

**Livrable :** carte Folium/Plotly interactive + liste des liaisons candidates

---

### Étape 5 — ML 1 : Clustering des gares (non supervisé)

**Objectif :** classifier automatiquement chaque gare en type de nœud réseau

**Features :**

| Feature | Description |
|---|---|
| `degree` | Nombre de liaisons directes |
| `betweenness_centrality` | Importance dans les chemins optimaux |
| `closeness_centrality` | Proximité au reste du réseau |
| `lat`, `lon` | Position géographique |
| `co2_moyen_depart` | CO2 moyen des routes au départ |
| `nb_pays_accessibles` | Diversité internationale |

**Modèles à comparer :**
- K-means (k=3 à 6, évalué par silhouette score + elbow method)
- DBSCAN (détecte naturellement les gares isolées/anomalies sans forcer un k)

**Résultat attendu :**

| Cluster | Profil type |
|---|---|
| 0 | Hubs européens majeurs (Paris, Frankfurt, Vienne) |
| 1 | Connecteurs régionaux |
| 2 | Gares périphériques / isolées |
| 3 | Nœuds de transit (fort betweenness, faible degree) |

**Métriques d'évaluation :** Silhouette Score, Davies-Bouldin Index

**Livrable :** modèle sauvegardé (joblib) + carte des clusters + tableau comparatif K-means vs DBSCAN

---

### Étape 6 — ML 2 : Régression CO2 d'un trajet reconstruit

**Objectif :** prédire le CO2 total d'un trajet multi-stops à partir de ses caractéristiques

**Pourquoi la régression ?**
La target `co2_total_kg` est calculable directement en sommant les segments → pas de target artificielle, pas de biais circulaire.

**Features :**

| Feature | Description |
|---|---|
| `distance_totale_km` | Distance cumulée du trajet |
| `nb_correspondances` | Nombre de changements |
| `nb_pays_traverses` | Diversité des pays |
| `types_trains_encoded` | Encodage des types de trains utilisés |
| `heure_depart` | Heure de départ (proxy jour/nuit) |
| `cluster_gare_depart` | Cluster ML1 de la gare de départ |
| `cluster_gare_arrivee` | Cluster ML1 de la gare d'arrivée |

**Modèles à comparer :**

| Modèle | Justification |
|---|---|
| Régression linéaire | Baseline interprétable |
| RandomForest Regressor | Capture les non-linéarités |
| XGBoost / LightGBM | Performances sur données tabulaires |
| MLP Regressor | Réseau neuronal simple |

**Optimisation :** GridSearchCV / RandomizedSearchCV + cross-validation 5-fold

**Métriques :** MAE, RMSE, R²

**Livrable :** modèle final sauvegardé (joblib) + rapport d'évaluation + visualisations résidus

---

### Étape 7 — API REST /predict

**Technologie :** FastAPI

**Routes exposées :**

```
POST /predict/route
  Input  : { "origin": "Limoges", "destination": "Montpellier", "criteria": "co2" }
  Output : { "stops": [...], "co2_kg": 2.4, "nb_correspondances": 1, "duree_min": 210 }

POST /predict/co2
  Input  : { "distance_km": 380, "nb_correspondances": 1, "train_types": ["TGV", "TER"] }
  Output : { "co2_predicted_kg": 3.1 }

GET /clusters/station/{station_name}
  Output : { "cluster": 0, "profil": "Hub européen majeur", "centrality": 0.87 }
```

**Métriques de monitoring :** latence /predict, distribution des CO2 prédits, drift détection

**Livrable :** API fonctionnelle + documentation OpenAPI automatique (Swagger)

---

### Étape 8 — Benchmark des services IA cloud

Comparer notre approche custom avec :

| Service | Capacités | Prix | Contraintes | Pertinence |
|---|---|---|---|---|
| Azure Machine Learning | AutoML, pipelines MLOps | Payant | RGPD EU | Bonne |
| AWS SageMaker AutoPilot | AutoML, déploiement | Payant | Données hors EU | Moyenne |
| Google Vertex AI | AutoML Tables | Payant | RGPD à vérifier | Bonne |
| HuggingFace AutoTrain | Fine-tuning, tabular | Freemium | Open source | Bonne |

**Livrable :** tableau comparatif + justification du choix modèle custom vs cloud

---

### Étape 9 — Visualisation finale

**Carte Europe interactive (Folium ou Plotly)** :
- Réseau ferroviaire physique (OpenRailwayMap)
- Services commerciaux actifs (GTFS)
- Clusters de gares (couleurs par type)
- Gaps détectés (liaisons candidates)
- Trajets optimaux calculés

**Livrable :** dashboard HTML interactif exportable

---

## Livrables finaux

| # | Livrable | Format |
|---|---|---|
| 1 | Notebook EDA + reconstruction des stops | `.ipynb` |
| 2 | Dossier projet structuré | `requirements.txt` |
| 3 | Scripts entraînement ML1 + ML2 | `.py` |
| 4 | Tableau comparatif des modèles | `.md` / `.csv` |
| 5 | Benchmark services IA cloud | `.md` |
| 6 | Modèles sauvegardés | `.joblib` |
| 7 | Script `predict.py` | `.py` |
| 8 | API FastAPI fonctionnelle | `.py` |
| 9 | Carte Europe interactive | `.html` |
| 10 | Rapport technique complet | `.md` / `.pdf` |
| 11 | Support de soutenance | `.pdf` |

---

## Points de vigilance

| Risque | Mitigation |
|---|---|
| OpenRailwayMap = données OSM potentiellement incomplètes | Documenter comme limite, croiser avec données GTFS |
| Clustering sans validation métier | Faire valider les clusters par des critères ferroviaires connus |
| GTFS mobility_db couvre surtout l'Europe centrale | Documenter la couverture géographique réelle |
| Complexité ETL élevée (multi-sources) | Prioriser backontrack pour les stops, GTFS en complément |

---

## Limites à documenter dans le rapport

- Absence de données de fréquentation (nb passagers/jour par liaison)
- OpenRailwayMap peut être incomplet sur certains pays (Europe de l'Est)
- Les gaps détectés peuvent avoir des raisons légitimes non visibles dans les données (topographie, rentabilité, politique)
- Le modèle CO2 prédit une valeur calculée, pas une émission mesurée en conditions réelles
