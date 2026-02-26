# ETL Docker Project

## Description
Ce projet est une application ETL (Extraction, Transformation, Chargement) qui permet de gérer le flux de données entre différentes sources et destinations. Il est conçu pour être exécuté dans un conteneur Docker, facilitant ainsi son déploiement et sa gestion.

## Structure du projet
```
etl-docker-project
├── src
│   ├── etl.py          # Logique principale de l'ETL
│   └── utils
│       └── helpers.py  # Fonctions utilitaires
├── Dockerfile           # Instructions pour construire l'image Docker
├── requirements.txt     # Liste des dépendances Python
└── README.md            # Documentation du projet
```

## Prérequis
- Docker installé sur votre machine.
- Python 3.x (pour le développement local).

## Installation
1. Clonez le dépôt :
   ```
   git clone <URL_DU_DEPOT>
   cd etl-docker-project
   ```

2. Construisez l'image Docker :
   ```
   docker build -t etl-docker-project .
   ```

## Exécution
Pour exécuter l'application ETL dans Docker, utilisez la commande suivante :
```
docker run --rm etl-docker-project
```

## Utilisation
- Modifiez le fichier `src/etl.py` pour adapter la logique ETL selon vos besoins.
- Ajoutez des fonctions utilitaires dans `src/utils/helpers.py` si nécessaire.
- Mettez à jour `requirements.txt` pour inclure toute nouvelle dépendance.

## Contribuer
Les contributions sont les bienvenues ! Veuillez soumettre une demande de tirage pour toute amélioration ou correction.