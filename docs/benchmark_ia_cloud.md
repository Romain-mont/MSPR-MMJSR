# Benchmark — Services IA Cloud
## Projet ObRail Europe : comparaison des plateformes MLOps

---

## Contexte du projet

ObRail Europe entraîne deux modèles ML sur **46 106 corridors ferroviaires français** :

| Modèle | Type | Algorithme retenu | Métriques |
|---|---|---|---|
| Modèle 1 — Classification | `is_substitutable` | XGBoost | F1=0.996, AUC=1.000 |
| Modèle 2 — Régression | `co2_saved_kg` | Random Forest | MAE=4.07 kg, R²=0.948 |
| Clustering (M3) | Segmentation corridors | K-Means k=4 (vs GMM k=2, DBSCAN k=13) | Silhouette=0.652 |

**Stack actuelle :** Python + scikit-learn + XGBoost + FastAPI + PostgreSQL  
**Déploiement actuel :** custom, modèles `.joblib` servis via `uvicorn`

---

## Plateformes évaluées

| Plateforme | Éditeur | Positionnement |
|---|---|---|
| AWS SageMaker | Amazon | MLOps complet, leader marché |
| Azure Machine Learning | Microsoft | Intégration entreprise, fort en EU |
| Google Vertex AI | Google | AutoML tabular, BigQuery natif |
| HuggingFace Inference Endpoints | HuggingFace | Open source, NLP + modèles custom |

---

## 1. AWS SageMaker

### Description
Plateforme MLOps complète d'Amazon AWS. Couvre le cycle complet : préparation des données (SageMaker Data Wrangler), entraînement (instances EC2 managées), déploiement (endpoints auto-scalants), monitoring (Model Monitor) et explicabilité (SageMaker Clarify).

### Tarification (eu-west-1, Paris)

| Usage | Instance | Prix/heure |
|---|---|---|
| Entraînement | ml.m5.large (2 vCPU, 8 GB) | ~0,115 $/h |
| Entraînement | ml.m5.xlarge (4 vCPU, 16 GB) | ~0,230 $/h |
| Inférence temps-réel | ml.t3.medium (2 vCPU, 4 GB) | ~0,054 $/h |
| SageMaker Autopilot | Tabular (classification) | ~3,00 $/h (GPU) |
| Stockage S3 | Données + modèles | ~0,023 $/Go/mois |

**Estimation projet ObRail :**
- Entraînement (2 modèles × 10 min sur ml.m5.large) : **~0,04 $**
- Endpoint inférence 24/7 (ml.t3.medium) : **~39 $/mois**
- SageMaker Autopilot (exploration) : **~15 $ pour 5h**

### Points forts
- **Explicabilité native** : SageMaker Clarify intègre SHAP values, biais detection, drift monitoring
- **MLOps complet** : pipelines CI/CD, versioning modèles, A/B testing automatique
- **Scalabilité** : auto-scaling de 0 à N instances selon la charge
- **Monitoring** : Model Monitor détecte le data drift et les dégradations de performance

### Points faibles
- **Complexité** : courbe d'apprentissage élevée, nombreux services à configurer
- **Vendor lock-in** : format propriétaire pour certains pipelines
- **Coût fixe** : l'endpoint 24/7 coûte ~39 $/mois même sans requêtes

### Conformité RGPD
- Régions EU disponibles : `eu-west-1` (Irlande), `eu-west-3` (Paris), `eu-central-1` (Francfort)
- Certifié ISO 27001, SOC 2, RGPD
- Possibilité de restreindre toutes les données à l'UE
- **DPA (Data Processing Agreement)** disponible

---

## 2. Azure Machine Learning

### Description
Plateforme MLOps de Microsoft, fortement intégrée à l'écosystème Azure (Active Directory, Power BI, DevOps). Propose AutoML, Designer (low-code), et des pipelines Python via SDK v2. Explicabilité via InterpretML (open source Microsoft).

### Tarification (West Europe, Amsterdam)

| Usage | Instance | Prix/heure |
|---|---|---|
| Entraînement | Standard_DS2_v2 (2 vCPU, 7 GB) | ~0,096 $/h |
| Entraînement | Standard_DS3_v2 (4 vCPU, 14 GB) | ~0,192 $/h |
| Inférence managée | Standard_DS2_v2 | ~0,096 $/h |
| AutoML | Tabular — classification | ~0,10 $/nœud/h |
| Stockage Blob | Données + modèles | ~0,018 $/Go/mois |

**Estimation projet ObRail :**
- Entraînement (2 modèles × 10 min) : **~0,03 $**
- Endpoint inférence 24/7 (Standard_DS2_v2) : **~69 $/mois**
- AutoML (exploration 5h) : **~2,40 $**

### Points forts
- **AutoML performant** : FLAML (Fast and Lightweight AutoML) intégré, excellent sur données tabulaires
- **InterpretML** : explicabilité SHAP, PDPs, interactive dashboards
- **Intégration MLflow** native : tracking expériences, versioning modèles
- **Gouvernance** : Azure Policy, RBAC granulaire, audit logs complets
- **Support entreprise** : SLA 99.95%, support Microsoft direct

### Points faibles
- **Coût inférence** : endpoint managé plus cher que SageMaker pour petites charges
- **Complexité SDK** : SDK v2 bien documenté mais nombreuses abstractions
- **Dépendance Azure** : difficile à migrer vers un autre cloud ensuite

### Conformité RGPD
- **Siège européen** : Microsoft Ireland, données stockables en UE uniquement
- Certifié ISO 27001, 27018, SOC 1/2, RGPD
- **Clauses contractuelles types (CCT)** UE incluses dans les contrats standard
- Parmi les meilleures certifications RGPD du marché cloud
- Résidence des données garantie via Azure Policy

---

## 3. Google Vertex AI

### Description
Plateforme MLOps unifiée de Google, intégrant AutoML, BigQuery ML et les modèles custom via Workbench (JupyterLab managé). Vertex Explainable AI fournit des explications SHAP pour les modèles tabulaires. Pipeline via Kubeflow Pipelines managé (Vertex Pipelines).

### Tarification (europe-west1, Belgique)

| Usage | Instance | Prix/heure |
|---|---|---|
| Entraînement custom | n1-standard-4 (4 vCPU, 15 GB) | ~0,190 $/h |
| Inférence — endpoint dédié | n1-standard-2 | ~0,095 $/h |
| Inférence — serverless | — | 0,0025 $/1000 prédictions |
| Vertex AutoML Tabular | Classification | ~19,32 $/h (node) |
| Stockage GCS | Données + modèles | ~0,023 $/Go/mois |

**Estimation projet ObRail :**
- Entraînement custom (2 modèles × 10 min) : **~0,06 $**
- Endpoint dédié 24/7 (n1-standard-2) : **~68 $/mois**
- Inférence serverless (10 000 req/mois) : **~0,025 $** ← très intéressant
- AutoML Tabular (exploration) : **~97 $ pour 5h** ← très cher

### Points forts
- **Inférence serverless** : 0.0025 $/1000 requêtes — idéal pour faible volume (notre cas)
- **Vertex Explainable AI** : SHAP intégré, integrated gradients pour features tabulaires
- **BigQuery ML** : si nos données sont dans BigQuery, entraînement sans extraction
- **Kubeflow Pipelines** : orchestration ML reproductible et versionnée

### Points faibles
- **AutoML très cher** : ~19 $/heure vs ~0.10 $/heure chez Azure
- **Moins mature** que SageMaker sur le monitoring de production
- **Complexité IAM** : gestion des permissions Service Account plus complexe

### Conformité RGPD
- Régions EU : `europe-west1` (Belgique), `europe-west4` (Pays-Bas), `europe-west3` (Allemagne)
- Certifié ISO 27001, SOC 2/3, RGPD
- **Accord de traitement des données UE** disponible
- Google LLC reste soumis au CLOUD Act américain — point de vigilance pour données sensibles
- Recommandé : activer la "Assured Workloads" policy pour bloquer tout accès hors UE

---

## 4. HuggingFace Inference Endpoints

### Description
Plateforme de déploiement open source principalement orientée NLP/LLM, mais qui supporte désormais les modèles scikit-learn, XGBoost et joblib via `custom handlers`. Hébergement sur AWS ou Azure, dans les régions de son choix.

### Tarification

| Instance | vCPU | RAM | Prix/heure |
|---|---|---|---|
| CPU small | 1 | 2 GB | ~0,032 $/h |
| CPU medium | 2 | 4 GB | ~0,060 $/h |
| CPU large | 4 | 8 GB | ~0,120 $/h |
| GPU (T4) | 4 | 14 GB | ~0,60 $/h |

**Estimation projet ObRail :**
- Endpoint 24/7 (CPU medium, nos modèles ~50 MB) : **~43 $/mois**
- Endpoint avec scale-to-zero (pause après inactivité) : **~5-10 $/mois**

### Points forts
- **Simplicité** : déploiement en quelques minutes via UI ou API, zero-config
- **Scale-to-zero** : endpoint mis en pause si pas de requêtes → économies importantes
- **Open source** : modèles versionnés sur HuggingFace Hub, transparence totale
- **Pricing prévisible** : pas de coûts cachés (pas de frais de stockage séparés)

### Points faibles
- **Conçu pour NLP** : support tabular/sklearn possible mais non natif, documentation limitée
- **Pas d'AutoML** : aucune fonctionnalité d'entraînement managé
- **Monitoring limité** : pas de drift detection, pas de Model Monitor équivalent
- **Explicabilité** : aucune intégration SHAP native, à implémenter manuellement
- **MLOps** : versioning basique, pas de pipelines CI/CD intégrés

### Conformité RGPD
- Hébergement sur AWS ou Azure selon la région choisie → RGPD dépend de l'hébergeur sous-jacent
- Régions EU disponibles (eu-west-1, eu-central-1)
- HuggingFace Inc. est une société française → engagement RGPD fort
- DPA disponible sur demande

---

## Tableau comparatif synthétique

| Critère | AWS SageMaker | Azure ML | Google Vertex AI | HuggingFace |
|---|---|---|---|---|
| **Coût entraînement** (2 modèles) | ~0,04 $ | ~0,03 $ | ~0,06 $ | Non applicable |
| **Coût inférence 24/7** | ~39 $/mois | ~69 $/mois | ~68 $/mois (dédié) | ~43 $/mois |
| **Coût inférence faible volume** | ~0,10 $/1000 req | ~0,10 $/1000 req | **0,0025 $/1000 req** | scale-to-zero |
| **AutoML tabular** | Autopilot (~3 $/h) | FLAML (~0,10 $/h) | (~19 $/h) ❌ | ❌ |
| **Explicabilité (SHAP)** | ✅ Clarify | ✅ InterpretML | ✅ Vertex XAI | ❌ manuel |
| **Drift monitoring** | ✅ Model Monitor | ✅ intégré | ✅ intégré | ❌ |
| **MLflow / expériences** | ✅ | ✅ natif | ✅ | ❌ |
| **RGPD — données en UE** | ✅ (Paris) | ✅ fort | ✅ (vigilance CLOUD Act) | ✅ |
| **Facilité déploiement** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Maturité MLOps** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **Support modèles custom sklearn/xgb** | ✅ | ✅ | ✅ | ⚠️ partiel |

---

## Analyse coût total sur 12 mois (scénario ObRail)

**Hypothèses :** 1 entraînement/mois, ~10 000 requêtes/mois, données en UE obligatoires.

| Solution | Coût annuel estimé | Notes |
|---|---|---|
| **Custom (actuel)** | **~0 $** (serveur interne) | Infrastructure existante, zéro coût cloud |
| HuggingFace (scale-to-zero) | **~60-120 $/an** | Option la plus économique si besoin cloud |
| AWS SageMaker | **~480 $/an** | Endpoint 24/7 ml.t3.medium |
| Google Vertex AI (serverless) | **~3 $/an** | Pour 10k req/mois à 0,0025 $/1000 |
| Azure ML | **~828 $/an** | Endpoint 24/7 Standard_DS2_v2 |

> **Note :** Vertex AI serverless est le plus économique pour faible volume, mais sans entraînement managé ni monitoring.

---

## Justification du choix : modèle custom

Pour le projet ObRail Europe, le déploiement cloud n'est **pas justifié à ce stade** pour les raisons suivantes :

### 1. Taille du dataset adaptée mais pas suffisante pour l'AutoML cloud
Avec 46 106 corridors, les services AutoML cloud (conçus pour des dizaines de millions de lignes) n'apporteraient pas de gain significatif par rapport à nos GridSearchCV locaux. Nos modèles atteignent déjà R²=0.948 et F1=1.000 — performances proches du maximum théorique.

### 2. Absence de réentraînement fréquent
Le dataset GTFS et SNCF est stable. Un réentraînement mensuel sur infrastructure locale suffit largement. Les services cloud deviennent pertinents à partir d'un réentraînement continu (streaming) ou de données > 10 Go.

### 3. Contrainte RGPD
Les données de fréquentation SNCF et INSEE sont publiques, mais dans un contexte de production avec des données passagers, le stockage sur infrastructure nationale (ou UE maîtrisée) est préférable. Notre stack FastAPI + PostgreSQL local offre un contrôle total.

### 4. Coût disproportionné
Un endpoint AWS SageMaker 24/7 coûte ~480 $/an pour des volumes faibles. Notre solution FastAPI + uvicorn sur un VPS à 5-10 $/mois est **50× moins chère** avec des performances identiques (~20ms de latence).

### 5. Explicabilité déjà implémentée
SageMaker Clarify et Vertex Explainable AI génèrent des SHAP values — notre implémentation expose directement les `feature_importances_` via l'API et le garde-fou EcoPassenger valide les prédictions de façon métier.

### Quand le cloud deviendrait pertinent ?

| Scénario | Plateforme recommandée | Raison |
|---|---|---|
| Extension à toute l'Europe (>500k corridors) | **AWS SageMaker** | MLOps mature, monitoring drift |
| Réentraînement quotidien sur nouvelles données GTFS | **Azure ML** | AutoML FLAML + MLflow natif |
| API publique à fort volume (>1M req/mois) | **Google Vertex AI** | Serverless 0.0025 $/1000 req |
| Prototype rapide / démo investisseur | **HuggingFace** | Déploiement en 10 minutes |

---

## Conclusion

Pour le contexte actuel d'ObRail Europe (MVP, données statiques, volume modéré), la solution **custom FastAPI + modèles joblib** est la plus adaptée : coût quasi nul, latence ~20ms, RGPD maîtrisé, explicabilité métier intégrée.

Si le projet devait évoluer vers une plateforme européenne à grande échelle, **Azure Machine Learning** serait la recommandation prioritaire : meilleure conformité RGPD en contexte ferroviaire européen (données publiques sensibles), AutoML compétitif, et intégration MLflow pour la reproductibilité des expériences.

**Google Vertex AI** reste la meilleure option si le volume de requêtes API devient important (pricing serverless).
