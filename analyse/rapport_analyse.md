================================================================================
RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-03-10 19:41:52
Source: ./data/staging/final_routes.csv
================================================================================

1) VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 44687
   Routes valides (avec distance): 44554
   Routes invalides              : 133

   Sources de données:
      • mobility_db         : 40368 routes
      • airports            : 3946 routes
      • back_on_track       :  373 routes

2) RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • Train Jour                         : 39064 trajets ( 87.4%)
   • Avion                              : 3946 trajets (  8.8%)
   • Train Nuit                         : 1677 trajets (  3.8%)

3) STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
Avion                                 3946          292.93           65.82
Train Jour                           38931           75.60            0.68
Train Nuit                            1677          195.21            1.76

4) TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Hannover Hbf              → Hengelo                  
       Train Jour                  5845.8 km   52.61 kg CO2
    2. Amsterdam Centraal        → Osnabrück Hbf            
       Train Jour                  5842.2 km   52.58 kg CO2
    3. 6415                      → Mainz, Hauptbahnhof Bstg 
       Train Jour                  5615.2 km   50.54 kg CO2
    4. Mainz, Hauptbahnhof       → 6415                     
       Train Jour                  5615.0 km   50.54 kg CO2
    5. 6415                      → Nackenheim, Tankstelle   
       Train Jour                  5607.2 km   50.46 kg CO2
    6. 6415                      → Oppenheim, Bahnhof       
       Train Jour                  5600.7 km   50.41 kg CO2
    7. Frankenthal, Hauptbahnhof → 6415                     
       Train Jour                  5565.6 km   50.09 kg CO2
    8. LU-Oggersheim, Bahnhof    → 6415                     
       Train Jour                  5561.3 km   50.05 kg CO2
    9. 6415                      → Mannheim, Hauptbahnhof   
       Train Jour                  5561.1 km   50.05 kg CO2
   10. 6408                      → Mannheim, Hauptbahnhof   
       Train Jour                  5561.1 km   50.05 kg CO2

5) COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   TRAINS (40741 routes)
      Distance moyenne    :    80.5 km
      CO2 moyen           :    0.72 kg
      CO2 par km          :  0.0090 kg/km
      Distance min/max    : 0.0 / 5845.8 km

   AVIONS (3946 routes)
      Distance moyenne    :   292.9 km
      CO2 moyen           :   65.82 kg
      CO2 par km          :  0.2247 kg/km
      Distance min/max    : 100.0 / 1072.0 km

   CONCLUSION: L'avion émet 25.0x plus de CO2 par km que le train
   Économie moyenne en prenant le train: 65.10 kg CO2

6) CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • Train Jour                         : 39064 trajets ( 95.9% des trains)
   • Train Nuit                         : 1677 trajets (  4.1% des trains)

7) ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 1677
   Distance moyenne              : 195.2 km
   CO2 moyen                     : 1.76 kg

      • Train Nuit                    : 1677 trajets

8) COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 8318
   Gares/Aéroports de destination: 15460
   Total unique                  : 16555

   Top 10 gares/aéroports les plus connectés:
       1. Zürich HB                               : 469 connexions
       2. München Hbf                             : 346 connexions
       3. Basel SBB                               : 275 connexions
       4. Köln Hbf                                : 270 connexions
       5. Bern                                    : 254 connexions
       6. Nürnberg Hbf                            : 248 connexions
       7. Düsseldorf Hbf                          : 240 connexions
       8. Dortmund Hbf                            : 240 connexions
       9. Frankfurt (Main) Hauptbahnhof           : 224 connexions
      10. Frankfurt(Main)Hbf                      : 218 connexions

9) QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 44687
   Données complètes (distance)  : 44554 (99.7%)
   Données complètes (CO2)       : 44554 (99.7%)
   Horaires de départ renseignés : 40737 (91.2%)
   Horaires d'arrivée renseignés : 40737 (91.2%)

================================================================================
10) FOOTER
================================================================================

Fichier de sortie: ./analyse/rapport_analyse.md
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
