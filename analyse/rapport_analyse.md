================================================================================
RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-04-28 12:33:44
Source: ./data/staging/final_routes.csv
================================================================================

1) VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 5000
   Routes valides (avec distance): 4857
   Routes invalides              : 143

   Sources de données:
      • mobility_db         : 2307 routes
      • airports            : 2307 routes
      • back_on_track       :  386 routes

2) RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • Avion                              : 2307 trajets ( 46.1%)
   • Train Longue Distance              : 1193 trajets ( 23.9%)
   • Train Longue Distance Nuit         :  572 trajets ( 11.4%)
   • EuroNight                          :  570 trajets ( 11.4%)
   • InterCity                          :  326 trajets (  6.5%)
   • Nightjet                           :   32 trajets (  0.6%)

3) STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
Avion                                 2307          218.65           96.83
EuroNight                              570          248.12            6.66
InterCity                              184          598.66           28.46
Nightjet                                31          671.70           23.74
Train Longue Distance                 1193          155.24            1.98
Train Longue Distance Nuit             572          129.05            1.26

4) TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Edirne                    → Villach Hbf              
       InterCity                   1154.3 km   89.59 kg CO2
    2. Villach Hbf               → Edirne                   
       InterCity                   1154.3 km   43.78 kg CO2
    3. Konya                     → İzmir Basmane            
       InterCity                   1127.1 km   47.72 kg CO2
    4. İzmir Basmane             → Konya                    
       InterCity                   1127.1 km   74.56 kg CO2
    5. Edirne                    → Villach Hbf              
       Avion                       1123.9 km  188.68 kg CO2
    6. Villach Hbf               → Edirne                   
       Avion                       1123.9 km  188.68 kg CO2
    7. Konya                     → İzmir Basmane            
       Avion                       1119.0 km  187.85 kg CO2
    8. İzmir Basmane             → Konya                    
       Avion                       1119.0 km  187.85 kg CO2
    9. Milano Centrale           → Siracusa                 
       Avion                       1072.0 km  179.96 kg CO2
   10. Siracusa                  → Milano Centrale          
       Avion                       1072.0 km  179.96 kg CO2

5) COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   TRAINS (2693 routes)
      Distance moyenne    :   208.4 km
      CO2 moyen           :    5.04 kg
      CO2 par km          :  0.0242 kg/km
      Distance min/max    : 100.1 / 1154.3 km

   AVIONS (2307 routes)
      Distance moyenne    :   218.7 km
      CO2 moyen           :   96.83 kg
      CO2 par km          :  0.4428 kg/km
      Distance min/max    : 100.1 / 1123.9 km

   CONCLUSION: L'avion émet 18.3x plus de CO2 par km que le train
   Économie moyenne en prenant le train: 91.79 kg CO2

6) CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • Train Longue Distance              : 1193 trajets ( 44.3% des trains)
   • Train Longue Distance Nuit         :  572 trajets ( 21.2% des trains)
   • EuroNight                          :  570 trajets ( 21.2% des trains)
   • InterCity                          :  326 trajets ( 12.1% des trains)
   • Nightjet                           :   32 trajets (  1.2% des trains)

7) ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 1174
   Distance moyenne              : 201.3 km
   CO2 moyen                     : 4.48 kg

      • Train Longue Distance Nuit    : 572 trajets
      • EuroNight                     : 570 trajets
      • Nightjet                      :  32 trajets

8) COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 482
   Gares/Aéroports de destination: 467
   Total unique                  : 521

   Top 10 gares/aéroports les plus connectés:
       1. Friedrichshafen Stadtbahnhof            : 120 connexions
       2. Gare de Nantes                          : 119 connexions
       3. Gare de Le Mans                         : 116 connexions
       4. Ulm Hbf                                 : 108 connexions
       5. Gare de Rennes                          : 102 connexions
       6. Gare de Tours                           : 100 connexions
       7. Lindau-Reutin                           :  94 connexions
       8. Gare de Savenay                         :  93 connexions
       9. München Hbf Gleis 14                    :  92 connexions
      10. Gare de Paris Saint-Lazare              :  90 connexions

9) QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 5000
   Données complètes (distance)  : 4857 (97.1%)
   Données complètes (CO2)       : 4857 (97.1%)
   Horaires de départ renseignés : 2685 (53.7%)
   Horaires d'arrivée renseignés : 2685 (53.7%)

================================================================================
10) FOOTER
================================================================================

Fichier de sortie: ./analyse/rapport_analyse.md
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
