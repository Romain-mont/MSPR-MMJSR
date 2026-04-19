================================================================================
RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-03-16 09:48:44
Source: ./data/staging/final_routes.csv
================================================================================

1) VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 9554
   Routes valides (avec distance): 9419
   Routes invalides              : 135

   Sources de données:
      • mobility_db         : 5961 routes
      • airports            : 3210 routes
      • back_on_track       :  383 routes

2) RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • Avion                              : 3210 trajets ( 33.6%)
   • Train Longue Distance              : 2642 trajets ( 27.7%)
   • Train Longue Distance Nuit         : 1193 trajets ( 12.5%)
   • ICE                                :  757 trajets (  7.9%)
   • InterCity                          :  435 trajets (  4.6%)
   • ICE Nuit                           :  428 trajets (  4.5%)
   • Train Nuit                         :  426 trajets (  4.5%)
   • EuroNight                          :  186 trajets (  1.9%)
   • Nightjet                           :  128 trajets (  1.3%)
   • EuroCity                           :   55 trajets (  0.6%)
   • InterCity Nuit                     :   50 trajets (  0.5%)
   • AVE                                :   17 trajets (  0.2%)
   • EuroCity Nuit                      :   12 trajets (  0.1%)
   • TGV                                :   11 trajets (  0.1%)
   • TGV Nuit                           :    3 trajets (  0.0%)
   • AVE Nuit                           :    1 trajets (  0.0%)

3) STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
AVE                                     17          136.87            1.23
AVE Nuit                                 1          103.07            0.93
Avion                                 3210          309.87           69.54
EuroCity                                55          209.32            1.88
EuroCity Nuit                           12          218.36            1.97
EuroNight                              186          355.38            3.20
ICE                                    757          382.33            3.44
ICE Nuit                               428          367.08            3.30
InterCity                              302          448.10            4.03
InterCity Nuit                          50          205.05            1.85
Nightjet                               126          492.61            4.43
TGV                                     11          476.56            4.29
TGV Nuit                                 3          555.68            5.00
Train Longue Distance                 2642          304.45            2.74
Train Longue Distance Nuit            1193          363.79            3.27
Train Nuit                             426          234.60            2.11

4) TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Hannover Hbf              → Hengelo                  
       ICE                         5845.8 km   52.61 kg CO2
    2. Hamburg-Harburg           → Hengelo                  
       Train Longue Distance       5845.8 km   52.61 kg CO2
    3. Hannover Hbf              → Hengelo                  
       ICE Nuit                    5845.8 km   52.61 kg CO2
    4. Münster (W) Zentrum Nord  → Ochtrup                  
       Train Longue Distance Nuit   5843.3 km   52.59 kg CO2
    5. Amsterdam Centraal        → Osnabrück Hbf            
       ICE                         5842.2 km   52.58 kg CO2
    6. Amsterdam Centraal        → Hamburg-Harburg          
       Train Longue Distance       5842.2 km   52.58 kg CO2
    7. Münster (Westf) Hbf       → Metelen Land             
       Train Longue Distance Nuit   5839.7 km   52.56 kg CO2
    8. Münster (Westf) Hbf       → Metelen Land             
       Train Longue Distance       5839.7 km   52.56 kg CO2
    9. Dortmund Hbf              → Lüdinghausen             
       Train Longue Distance       5797.7 km   52.18 kg CO2
   10. Dortmund Hbf              → Lüdinghausen             
       Train Longue Distance Nuit   5797.7 km   52.18 kg CO2

5) COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   TRAINS (5127 routes)
      Distance moyenne    :   325.8 km
      CO2 moyen           :    2.93 kg
      CO2 par km          :  0.0090 kg/km
      Distance min/max    : 100.1 / 5845.8 km

   AVIONS (3210 routes)
      Distance moyenne    :   309.9 km
      CO2 moyen           :   69.54 kg
      CO2 par km          :  0.2244 kg/km
      Distance min/max    : 100.0 / 1390.3 km

   CONCLUSION: L'avion émet 24.9x plus de CO2 par km que le train
   Économie moyenne en prenant le train: 66.61 kg CO2

6) CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • Train Longue Distance              : 2642 trajets ( 51.5% des trains)
   • Train Longue Distance Nuit         : 1193 trajets ( 23.3% des trains)
   • InterCity                          :  435 trajets (  8.5% des trains)
   • Train Nuit                         :  426 trajets (  8.3% des trains)
   • EuroNight                          :  186 trajets (  3.6% des trains)
   • Nightjet                           :  128 trajets (  2.5% des trains)
   • EuroCity                           :   55 trajets (  1.1% des trains)
   • InterCity Nuit                     :   50 trajets (  1.0% des trains)
   • EuroCity Nuit                      :   12 trajets (  0.2% des trains)

7) ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 2427
   Distance moyenne              : 343.9 km
   CO2 moyen                     : 3.09 kg

      • Train Longue Distance Nuit    : 1193 trajets
      • ICE Nuit                      : 428 trajets
      • Train Nuit                    : 426 trajets
      • EuroNight                     : 186 trajets
      • Nightjet                      : 128 trajets
      • InterCity Nuit                :  50 trajets
      • EuroCity Nuit                 :  12 trajets
      • TGV Nuit                      :   3 trajets
      • AVE Nuit                      :   1 trajets

8) COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 1541
   Gares/Aéroports de destination: 2607
   Total unique                  : 2779

   Top 10 gares/aéroports les plus connectés:
       1. München Hbf                             : 238 connexions
       2. Frankfurt(Main)Hbf                      : 203 connexions
       3. Köln Hbf                                : 174 connexions
       4. Berlin Brandenburg Airport              : 155 connexions
       5. Stuttgart Hbf                           : 152 connexions
       6. Stockholm Centralstation                : 149 connexions
       7. Schleissheim Airfield                   : 146 connexions
       8. Basel SBB                               : 140 connexions
       9. Zürich HB                               : 139 connexions
      10. Paris-Le Bourget International Airport  : 133 connexions

9) QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 9554
   Données complètes (distance)  : 9419 (98.6%)
   Données complètes (CO2)       : 9419 (98.6%)
   Horaires de départ renseignés : 6342 (66.4%)
   Horaires d'arrivée renseignés : 6342 (66.4%)

================================================================================
10) FOOTER
================================================================================

Fichier de sortie: ./analyse/rapport_analyse.md
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
