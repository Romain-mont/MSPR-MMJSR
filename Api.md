# 4. Ouverture vers une API REST

## 4.1 Objectif et Périmètre Fonctionnel

### 4.1.1 Contexte

Une fois les données unifiées et chargées dans l'entrepôt PostgreSQL, il devient essentiel de les rendre **accessibles et exploitables** par des applications tierces, des services web ou des outils de visualisation. L'objectif de cette API REST est de permettre une **consultation structurée et normalisée** des résultats de notre pipeline ETL.

### 4.1.2 Objectifs Principaux

L'API doit permettre aux utilisateurs de :

1. **Rechercher des itinéraires ferroviaires** entre deux villes européennes (départ/arrivée)
2. **Comparer les émissions de CO₂e** entre différents modes de transport ferroviaire (train de jour vs train de nuit)
3. **Filtrer les trajets longue distance** (>= 400 km) pour se concentrer sur les alternatives pertinentes à l'avion
4. **Obtenir des données normalisées** au format JSON, facilement intégrables dans des applications web ou mobiles

### 4.1.3 Cas d'Usage

- **Comparateur de mobilité durable** : Un site web qui compare automatiquement les émissions CO₂ entre train et avion
- **Assistant de voyage éco-responsable** : Une application mobile qui recommande le train de nuit pour les longues distances
- **Tableau de bord décisionnel** : Un dashboard pour les décideurs d'ObRail Europe afin d'analyser les tendances de mobilité
- **Intégrations tierces** : Partage des données avec des partenaires académiques ou ONG environnementales

---

## 4.2 Design des Endpoints

### 4.2.1 Architecture Technique

L'API est développée avec **FastAPI**, un framework Python moderne recommandé dans le sujet du projet. Ce choix technique permet :

- **Validation automatique des données** : Les paramètres d'entrée et les réponses sont vérifiés automatiquement via Pydantic
- **Typage fort** : Utilisation des annotations Python pour garantir la cohérence et faciliter la maintenance
- **Performance** : Basé sur ASGI (Starlette), adapté aux requêtes concurrentes
- **Bonus** : FastAPI génère automatiquement une documentation interactive (Swagger UI) accessible via `/docs`

L'API interroge directement la base de données PostgreSQL via **SQLAlchemy**, en exploitant le modèle en étoile (schéma fact/dimensions) mis en place lors de la phase de chargement. Cette architecture permet des requêtes SQL optimisées avec des jointures entre les tables `fact_em`, `dim_route` et `dim_vehicle_type`.

**Intégration dans l'Architecture du Projet** :

L'API REST constitue la **couche d'accès unique** aux données du datamart. Tous les composants du projet passent par cette API :

```
┌─────────────────────────────────────────────────────────────┐
│                   ARCHITECTURE GLOBALE                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Pipeline ETL                                               │
│  (extraction → transformation → load)                       │
│           │                                                 │
│           ▼                                                 │
│     PostgreSQL (Datamart)                                   │
│           │                                                 │
│           ▼                                                 │
│   ┌──────────────────┐                                     │
│   │   API REST       │  ← Point d'accès unique             │
│   │ (FastAPI:8000)   │                                     │
│   └────────┬─────────┘                                     │
│            │                                                │
│      ┌─────┴─────┬───────────┬────────────┐               │
│      ▼           ▼           ▼            ▼               │
│  Dashboard   Dashboard   Utilisateurs  Applications       │
│  Streamlit   Matplotlib  externes      tierces            │
│  (port 8505) (graphiques) (ONG, etc.)  (futurs)          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Avantages de cette Architecture** :
- ✅ **Point d'accès unique** : Toutes les requêtes passent par l'API, facilitant le monitoring et la sécurité
- ✅ **Validation centralisée** : Les données sont validées une seule fois au niveau de l'API
- ✅ **Découplage** : Les dashboards ne dépendent pas directement de la structure de la base de données
- ✅ **Évolutivité** : Changement de base de données transparent pour les clients de l'API
- ✅ **Traçabilité** : Possibilité de logger toutes les requêtes pour audit

**Déploiement Docker** :

L'API est déployée comme un service Docker indépendant qui communique avec les autres composants :

```yaml
# docker-compose.yml (extrait)
api:
  container_name: euro-rail-api
  ports:
    - "8000:8000"
  depends_on:
    - db
  command: ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

dashboard-web:
  container_name: dashboard-web
  environment:
    - API_URL=http://api:8000  # Utilise l'API
  ports:
    - "8505:8505"
  depends_on:
    - api
```

Cette configuration garantit que :
- L'API démarre après PostgreSQL (`depends_on: db`)
- Les dashboards démarrent après l'API (`depends_on: api`)
- La communication interne utilise les noms de conteneurs Docker (`http://api:8000`)

### 4.2.2 Endpoints Disponibles

#### **GET /**
```
Endpoint de santé / vérification du statut de l'API
```

**Objectif** : Vérifier que le service est en ligne et accessible.

**Paramètres** : Aucun

**Réponse** :
```json
{
  "status": "online",
  "message": "Bienvenue sur l'API Euro Rail ! 🚄"
}
```

**Codes HTTP** :
- `200 OK` : Service opérationnel

---

#### **GET /data**
```
Récupère toutes les données du datamart pour analyses
```

**Objectif** : Fournir l'ensemble des données ferroviaires pour les dashboards et outils d'analyse. Cet endpoint est principalement utilisé par les composants internes du projet (dashboards Streamlit et Matplotlib).

**Paramètres de Requête** :

| Paramètre | Type    | Obligatoire | Description                                | Exemple |
|-----------|---------|-------------|--------------------------------------------|---------|
| `limit`   | integer | Non         | Nombre maximum de résultats à retourner    | `1000`  |

**Comportement** :
- Retourne **toutes les routes** avec leurs métriques CO₂
- Inclut les données de jointure entre routes, véhicules et émissions
- Sans paramètre `limit`, retourne l'intégralité du datamart

**Exemple d'URL** :
```
GET http://localhost:8000/data
GET http://localhost:8000/data?limit=1000
```

**Réponse (200 OK)** :
```json
[
  {
    "origine": "Paris Gare de Lyon",
    "destination": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "is_long_distance": false,
    "vehicule_type": "Train de Jour",
    "facteur_co2": 0.004,
    "co2_kg": 1.85
  },
  {
    "origine": "Paris Gare de Lyon",
    "destination": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "is_long_distance": false,
    "vehicule_type": "Train de Nuit",
    "facteur_co2": 0.005,
    "co2_kg": 2.31
  }
]
```

**Utilisation dans le Projet** :
- ✅ **Dashboard Streamlit** (`dashboard_web.py`) : Charge les données via cet endpoint pour générer les visualisations interactives
- ✅ **Dashboard Matplotlib** (`dashboard.py`) : Charge les données via cet endpoint pour générer les graphiques statiques
- ✅ **Outils d'analyse tiers** : Permet aux data scientists et chercheurs d'accéder facilement au dataset complet

**Codes HTTP** :
- `200 OK` : Données retournées avec succès
- `500 Internal Server Error` : Erreur serveur (problème de connexion à la base)

---

#### **GET /search**
```
Recherche d'itinéraires ferroviaires avec calcul des émissions CO₂
```

**Objectif** : Retourner tous les trajets disponibles entre deux villes avec leurs émissions de CO₂ respectives. Endpoint destiné aux utilisateurs finaux et applications tierces.

**Paramètres de Requête** :

| Paramètre      | Type   | Obligatoire | Description                                                     | Exemple           |
|----------------|--------|-------------|-----------------------------------------------------------------|-------------------|
| `depart`       | string | Oui         | Ville ou gare de départ (recherche insensible à la casse)       | `Paris`           |
| `arrivee`      | string | Oui         | Ville ou gare d'arrivée (recherche insensible à la casse)       | `Berlin`          |
| `vehicle_type` | string | Non         | Type de train pour filtrer les résultats                        | `Train de Jour`   |

**Comportement** :
- La recherche est **tolérante** : "Paris" matchera "Paris Gare de Lyon", "Paris Nord", etc.
- Utilise l'opérateur SQL `ILIKE` avec jokers (`%`) pour permettre les correspondances partielles
- Par défaut, retourne **tous les types de véhicules** disponibles sur le trajet
- Si `vehicle_type` est spécifié, ne retourne que les trajets correspondant à ce type ("Train de Jour" ou "Train de Nuit")

**Exemples d'URL** :
```
# Recherche tous les types de trains
GET http://localhost:8000/search?depart=Paris&arrivee=Lyon

# Recherche uniquement les trains de jour
GET http://localhost:8000/search?depart=Paris&arrivee=Lyon&vehicle_type=Train de Jour
```

**Réponse (200 OK)** :
```json
[
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "vehicule_type": "Train de Jour",
    "co2_kg": 1.85
  },
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "vehicule_type": "Train de Nuit",
    "co2_kg": 2.31
  }
]
```

**Codes HTTP** :
- `200 OK` : Trajets trouvés
- `404 Not Found` : Aucun trajet ne correspond aux critères
- `500 Internal Server Error` : Erreur serveur (problème de connexion à la base)

---

### 4.2.3 Extensions Possibles (Non Implémentées)

Dans une version future, les endpoints suivants pourraient être ajoutés :

#### **GET /routes**
```
Liste toutes les routes disponibles dans la base
```

**Paramètres optionnels** :
- `min_distance` : Distance minimum en km
- `max_distance` : Distance maximum en km
- `is_long_distance` : Booléen pour filtrer uniquement les longues distances (>= 400 km)

**Exemple** :
```
GET /routes?min_distance=400&is_long_distance=true
```

#### **GET /vehicle-types**
```
Liste tous les types de véhicules et leurs facteurs d'émission
```

**Réponse Exemple** :
```json
[
  {
    "vehicle_type_id": 1,
    "label": "Train de Jour",
    "co2_vt": 0.004,
    "service_type": "Day Service"
  },
  {
    "vehicle_type_id": 2,
    "label": "Train de Nuit",
    "co2_vt": 0.005,
    "service_type": "Night Service"
  }
]
```

#### **GET /emissions/compare**
```
Compare les émissions entre plusieurs modes de transport
```

**Paramètres** :
- `depart` : Ville de départ
- `arrivee` : Ville d'arrivée
- `modes` : Liste de modes (train_day, train_night, plane)

**Réponse Exemple** :
```json
{
  "route": "Paris - Berlin",
  "distance_km": 878.3,
  "emissions": {
    "train_day": 3.51,
    "train_night": 4.39,
    "plane": 132.0
  },
  "best_option": "train_day"
}
```

---

## 4.3 Contrats de l'API

### 4.3.1 Modèles de Données (Pydantic)

#### **TrajetResponse**
```python
class TrajetResponse(BaseModel):
    depart: str           # Nom de la gare de départ
    arrivee: str          # Nom de la gare d'arrivée
    distance_km: float    # Distance en kilomètres
    vehicule_type: str    # Type de train (ex: "Train de Jour")
    co2_kg: float         # Émissions de CO₂ en kg par passager
```

**Validation Automatique** :
- Les types sont vérifiés automatiquement par Pydantic
- Une erreur `422 Unprocessable Entity` est retournée si les données ne correspondent pas au modèle

### 4.3.2 Exemples de Requêtes/Réponses

#### Requête 1 : Recherche Paris → Lyon

**Requête** :
```http
GET /search?depart=Paris&arrivee=Lyon HTTP/1.1
Host: localhost:8000
Accept: application/json
```

**Réponse (200 OK)** :
```json
[
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "vehicule_type": "Train de Jour",
    "co2_kg": 1.85
  },
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "vehicule_type": "Train de Nuit",
    "co2_kg": 2.31
  }
]
```

**Interprétation** :
- Pour un trajet de 462.5 km, le train de jour émet **1.85 kg de CO₂** par passager
- Le train de nuit en émet **2.31 kg** (25% de plus en raison d'un facteur d'émission plus élevé)
- À titre de comparaison, un vol Paris-Lyon émettrait environ **55 kg de CO₂** par passager (30x plus)

---

#### Requête 2 : Filtre par Type de Véhicule

**Requête** :
```http
GET /search?depart=Paris&arrivee=Lyon&vehicle_type=Train de Jour HTTP/1.1
Host: localhost:8000
Accept: application/json
```

**Réponse (200 OK)** :
```json
[
  {
    "depart": "Paris Gare de Lyon",
    "arrivee": "Lyon Part-Dieu",
    "distance_km": 462.5,
    "vehicule_type": "Train de Jour",
    "co2_kg": 1.85
  }
]
```

**Interprétation** :
- Le filtre `vehicle_type` permet de cibler uniquement les trains de jour
- Utile pour comparer spécifiquement un type de service
- Répond à l'exigence du sujet de filtrer par "type de train"

---

#### Requête 3 : Recherche Introuvable

**Requête** :
```http
GET /search?depart=Paris&arrivee=Tokyo HTTP/1.1
Host: localhost:8000
Accept: application/json
```

**Réponse (404 Not Found)** :
```json
{
  "detail": "Aucun trajet trouvé entre Paris et Tokyo"
}
```

---

#### Requête 4 : Paramètre Manquant

**Requête** :
```http
GET /search?depart=Paris HTTP/1.1
Host: localhost:8000
Accept: application/json
```

**Réponse (422 Unprocessable Entity)** :
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["query", "arrivee"],
      "msg": "Field required",
      "input": null
    }
  ]
}
```

---

### 4.3.3 Codes d'Erreur HTTP

| Code | Nom                        | Signification                                          | Exemple                                |
|------|----------------------------|--------------------------------------------------------|----------------------------------------|
| 200  | OK                         | Requête réussie, données retournées                    | Trajets trouvés                        |
| 404  | Not Found                  | Aucune donnée ne correspond aux critères               | Aucun trajet Paris → Tokyo             |
| 422  | Unprocessable Entity       | Paramètres invalides ou manquants                      | Paramètre `arrivee` absent             |
| 500  | Internal Server Error      | Erreur côté serveur (connexion DB, exception Python)   | PostgreSQL inaccessible                |

### 4.3.4 Validation des Données

**Côté Serveur** :
- Les paramètres `depart` et `arrivee` sont **obligatoires** (validation FastAPI)
- Les noms de villes acceptent **n'importe quel caractère Unicode** (support multilingue)
- La recherche est **insensible à la casse** (ILIKE en SQL)
- Les résultats sont automatiquement validés via le modèle `TrajetResponse`

**Sécurité** :
- Utilisation de **requêtes préparées** (SQLAlchemy ORM) pour prévenir les **injections SQL**
- Paramètres échappés automatiquement via le système de binding de SQLAlchemy

**Limitation** :
- Dans la version actuelle, aucune limite de pagination n'est implémentée
- En production, il serait recommandé d'ajouter un paramètre `limit` (ex: 100 résultats max)

---

## 4.4 Documentation et Outils de Test

### 4.4.1 Documentation Technique

Comme demandé dans le sujet du projet, une **documentation technique claire incluant des exemples de requêtes** est indispensable pour garantir la prise en main par des utilisateurs tiers. Cette documentation comprend :

**Éléments Documentés** :
- **Liste complète des endpoints** : Description de chaque route avec son objectif
- **Paramètres d'entrée** : Nom, type, caractère obligatoire, format attendu
- **Formats de réponse** : Structure JSON avec description de chaque champ
- **Codes d'erreur** : Liste des codes HTTP retournés et leur signification
- **Exemples concrets** : Requêtes complètes avec réponses attendues
- **Cas d'usage** : Scénarios réels d'utilisation de l'API

**Format de la Documentation** :
- Document Markdown (ce fichier) intégré au dossier de rendu
- Tableaux récapitulatifs des paramètres et codes HTTP
- Exemples de requêtes au format HTTP standard
- Réponses JSON formatées et commentées

**Bonus - Interface Interactive** :
FastAPI génère automatiquement une interface Swagger UI accessible à `http://localhost:8000/docs`. Bien que non explicitement demandée dans le sujet, cette interface offre :
- Visualisation interactive des endpoints
- Possibilité de tester l'API directement depuis le navigateur
- Export du schéma OpenAPI 3.0 (disponible à `/openapi.json`)

Cette documentation automatique complète la documentation technique écrite mais ne la remplace pas.

### 4.4.2 Tests avec Postman

Le sujet du projet demande explicitement l'utilisation de **Postman pour le test des APIs**. Postman est un outil incontournable pour :
- Tester manuellement les endpoints
- Vérifier les réponses et codes HTTP
- Déboguer les erreurs
- Automatiser les tests via des collections

**Configuration Postman** :

1. **Création de l'environnement** :
```json
{
  "name": "Euro Rail Local",
  "values": [
    {
      "key": "base_url",
      "value": "http://localhost:8000",
      "enabled": true
    }
  ]
}
```

2. **Requêtes de test** :

**Test 1 - Health Check** :
- **Méthode** : GET
- **URL** : `{{base_url}}/`
- **Résultat attendu** : 200 OK avec message de bienvenue

**Test 2 - Recherche Standard** :
- **Méthode** : GET
- **URL** : `{{base_url}}/search?depart=Paris&arrivee=Lyon`
- **Résultat attendu** : 200 OK avec liste de trajets

**Test 3 - Filtre par Type** :
- **Méthode** : GET
- **URL** : `{{base_url}}/search?depart=Paris&arrivee=Lyon&vehicle_type=Train de Jour`
- **Résultat attendu** : 200 OK avec uniquement trains de jour

**Test 4 - Erreur 404** :
- **Méthode** : GET
- **URL** : `{{base_url}}/search?depart=Paris&arrivee=Tokyo`
- **Résultat attendu** : 404 Not Found

**Test 5 - Validation des Paramètres** :
- **Méthode** : GET
- **URL** : `{{base_url}}/search?depart=Paris`
- **Résultat attendu** : 422 Unprocessable Entity

**Tests Automatisés** :
Postman permet d'écrire des scripts de test pour valider automatiquement les réponses :

```javascript
// Test pour /search
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Response is an array", function () {
    pm.expect(pm.response.json()).to.be.an('array');
});

pm.test("Each result has required fields", function () {
    var jsonData = pm.response.json();
    jsonData.forEach(function(trajet) {
        pm.expect(trajet).to.have.property('depart');
        pm.expect(trajet).to.have.property('arrivee');
        pm.expect(trajet).to.have.property('distance_km');
        pm.expect(trajet).to.have.property('vehicule_type');
        pm.expect(trajet).to.have.property('co2_kg');
    });
});
```

**Exécution en Ligne de Commande** :
Pour automatiser les tests dans un pipeline CI/CD, utiliser Newman (CLI de Postman) :

```bash
# Installation de Newman
npm install -g newman

# Exécution des tests
newman run Euro_Rail_API.postman_collection.json \
  --environment Euro_Rail_Local.postman_environment.json \
  --reporters cli,junit,html
```

---

## 4.5 Bilan et Perspectives

### 4.5.1 Réponse aux Exigences du Sujet

L'API développée répond aux exigences du sujet MSPR TPRE612 :

**✅ Endpoints de Consultation** :
- ✅ Interrogation par ville de départ (paramètre `depart`)
- ✅ Interrogation par ville d'arrivée (paramètre `arrivee`)
- ✅ Interrogation par type de train (paramètre optionnel `vehicle_type`)

**✅ Architecture REST** :
- Méthode GET pour la consultation
- Format JSON standard
- Codes HTTP appropriés (200, 404, 422, 500)
- URLs claires et sémantiques

**✅ Documentation Technique** :
- Documentation complète avec exemples de requêtes
- Description des paramètres et formats de réponse
- Liste des codes d'erreur
- Cas d'usage documentés

**✅ Tests avec Postman** :
- Configuration de l'environnement Postman
- Requêtes de test pour chaque endpoint
- Scripts de validation automatisés
- Possibilité d'export pour intégration CI/CD

**✅ Sécurité** :
- Protection contre les injections SQL (requêtes paramétrées)
- Validation des entrées via Pydantic
- Gestion des erreurs robuste

### 4.5.2 Points Forts de l'Implémentation

**Simplicité et Efficacité** :
- API **minimaliste** mais complète pour le besoin d'ObRail Europe
- Temps de réponse rapide grâce aux index SQL sur `dep_name` et `arr_name`
- Code clair et maintenable (moins de 100 lignes)

**Qualité Technique** :
- **Validation stricte** via Pydantic (typage fort Python)
- **Sécurité renforcée** : requêtes paramétrées SQLAlchemy
- **Recherche tolérante** : l'utilisateur n'a pas besoin de connaître le nom exact des gares

**Facilité d'Intégration** :
- Format JSON standard compatible avec tous les clients HTTP
- Modèles de données clairs et auto-documentés
- Tests Postman prêts à l'emploi

### 4.5.3 Limites Identifiées

**Limites Fonctionnelles** :
- **Pagination** : Absence de limite sur le nombre de résultats retournés (risque si base de données volumineuse)
- **Tri** : Pas de possibilité de trier les résultats (par distance, CO₂, etc.)
- **Agrégations** : Pas de statistiques globales (moyenne CO₂, total de routes, etc.)

**Limites de Sécurité** :
- **Authentification** : API entièrement publique, aucune restriction d'accès
- **Rate Limiting** : Pas de limitation du nombre de requêtes (vulnérable aux abus)
- **HTTPS** : En local uniquement, pas de chiffrement TLS

**Limites de Déploiement** :
- **Environnement** : Déployée en Docker mais uniquement en réseau local (pas de déploiement cloud)
- **Monitoring** : Pas de logs structurés ni de métriques de performance (Prometheus, Grafana)
- **Haute disponibilité** : Instance unique, pas de résilience en cas de panne
- **Load Balancing** : Pas de répartition de charge entre plusieurs instances

### 4.5.4 Pistes d'Amélioration

#### Améliorations Court Terme (Prioritaires)

**1. Pagination et Limite de Résultats** :
```python
@app.get("/search")
def search_route(depart: str, arrivee:  str, vehicle_type: str = None, 
                 limit: int = 100, offset: int = 0):
    # Ajouter LIMIT et OFFSET dans la requête SQL
```
- Permet de gérer des bases volumineuses
- Évite les timeouts sur les requêtes lourdes

**2. Tri des Résultats** :
```python
@app.get("/search")
def search_route(depart: str, arrivee: str, sort_by: str = "co2_kg"):
    # Ajouter ORDER BY dans la requête SQL
```
- Permet de classer par émissions CO₂, distance, ou type de train
- Facilite la comparaison pour l'utilisateur

**3. Endpoints Supplémentaires** :
- `GET /stats` : Statistiques globales (nombre de routes, moyenne CO₂, etc.)
- `GET /vehicle-types` : Liste des types de trains disponibles
- `GET /routes/long-distance` : Filtre automatique sur les longues distances (≥ 400 km)

#### Améliorations Moyen Terme (Production)

**4. Conteneurisation de l'API** :
Ajouter un service dédié dans `docker-compose.yml` :
```yaml
api:
  build:
    context: ./api
  container_name: euro-rail-api
  ports:
    - "8000:8000"
  depends_on:
    - db
  environment:
    - DB_HOST=db
```

**5. Authentification** :
- Implémenter un système de clés API simples
- Restriction d'accès pour les données sensibles (si ajoutées dans le futur)
- Traçabilité des utilisateurs de l'API

**6. Rate Limiting** :
- Limiter à 100 requêtes/minute par utilisateur
- Protection contre les attaques par déni de service (DoS)

#### Améliorations Long Terme (Enrichissement)

**7. Cache Redis** :
- Mise en cache des requêtes fréquentes
- Réduction de la charge sur PostgreSQL
- Amélioration des performances (réponse < 10 ms)

**8. Intégration de Données en Temps Réel** :
- API Trainline pour les prix
- API météo pour les conditions de voyage
- Comparaison automatique avec les vols (API Skyscanner)

**9. Websockets pour Notifications** :
- Alertes en temps réel sur les nouvelles routes
- Notifications de changement de CO₂

### 4.5.5 Conclusion

L'API REST développée **répond pleinement aux exigences du sujet** :
- ✅ Endpoints de consultation opérationnels (départ, arrivée, type de train, toutes les données)
- ✅ Architecture REST respectée avec méthodes HTTP appropriées
- ✅ Documentation technique claire avec exemples de requêtes
- ✅ Tests Postman implémentés et automatisables
- ✅ Sécurité de base assurée (prévention injections SQL)
- ✅ **Déployée dans Docker** et accessible aux autres composants
- ✅ **Utilisée par les dashboards internes** (Streamlit et Matplotlib)

**Architecture fonctionnelle** :
L'API constitue le **point d'accès unique** aux données du datamart. Tous les composants du projet passent par cette API :
- Le **dashboard Streamlit** (port 8505) utilise `GET /data` pour charger les données et générer les visualisations interactives
- Le **dashboard Matplotlib** utilise `GET /data` pour générer les graphiques statiques
- Les **utilisateurs externes** peuvent utiliser `GET /search` pour interroger des trajets spécifiques

Cette architecture garantit :
- Un **découplage propre** entre la base de données et les clients
- Une **validation centralisée** des données au niveau de l'API
- Une **traçabilité** potentielle de tous les accès
- Une **évolutivité** facilitée (changement de base de données transparent)

Cette API constitue une **version fonctionnelle et intégrée (v1.0)** qui permet d'exploiter les données ferroviaires unifiées par le pipeline ETL. Elle met en avant l'objectif central d'ObRail Europe : **comparer l'impact carbone des trains de jour et de nuit** pour promouvoir la mobilité durable en Europe.

Les limites identifiées sont **normales pour un projet académique** et les pistes d'amélioration proposées permettraient de transformer cette API en un service production-ready. L'authentification, le rate limiting et le monitoring seraient les prochaines étapes prioritaires pour un déploiement en production.

En s'inscrivant dans une démarche **Open Data**, cette API pourrait servir de base pour des initiatives citoyennes, académiques ou associatives souhaitant promouvoir les transports bas carbone en Europe.

---

**Fin de la Section 4**
