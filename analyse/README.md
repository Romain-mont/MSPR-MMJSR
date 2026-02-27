# 📊 Analyse des Résultats ETL

Ce dossier contient le module d'analyse automatique des résultats du pipeline ETL.

## Fichiers

- **`analyse_resultat.py`** : Script principal qui génère un rapport d'analyse complet
- **`__init__.py`** : Package marker

## Fonctionnalité

À la fin de chaque exécution du pipeline ETL, un rapport détaillé est automatiquement généré dans :

```
data/staging/rapport_analyse.txt
```

## Contenu du Rapport

Le rapport comprend :

1. **Vue d'ensemble** : Nombre total de routes, sources de données
2. **Répartition par type de véhicule** : Statistiques par type (Train, Avion, etc.)
3. **Statistiques détaillées** : Distance moyenne, CO2 moyen par type
4. **Top 10 routes** : Les trajets les plus longs
5. **Comparaison environnementale** : Train vs Avion avec facteur multiplicateur
6. **Classification des trains** : Détail des différents types de trains
7. **Trains de nuit** : Analyse spécifique des trains de nuit détectés
8. **Couverture géographique** : Nombre de gares/aéroports, top connexions
9. **Qualité des données** : Taux de complétude des champs

## Utilisation Standalone

Vous pouvez aussi exécuter l'analyse manuellement :

```bash
cd /path/to/MSPR
python analyse/analyse_resultat.py
```

## Variables d'Environnement

- `OUTPUT_DIR` : Dossier des fichiers de sortie (défaut: `./data/staging`)
- `OUTPUT_FINAL_FILE` : Nom du fichier CSV final (défaut: `final_routes.csv`)

## Intégration au Pipeline

L'analyse est appelée automatiquement dans `main.py` comme Phase 4 du pipeline :

1. Extraction
2. Transformation  
3. Chargement
4. **Analyse** ← Génération du rapport

Si l'analyse échoue, le pipeline continue (non bloquant).
