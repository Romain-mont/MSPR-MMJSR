================================================================================
RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-06-09 15:36:20
Source: donnee/staging_fact_route_analysis.csv (46k corridors français)
================================================================================

1) VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total corridors (train)        : 46106
   Corridors avec vol existant    : 43782
   Corridors substituables        : 41268 (89.5%)
   Corridors non substituables    : 4838 (10.5%)

2) RÉPARTITION PAR TYPE DE VÉHICULE (train uniquement)
--------------------------------------------------------------------------------
   • Train Longue Distance              :  26121 corridors ( 56.7%)
   • Train Longue Distance Nuit         :   6036 corridors ( 13.1%)
   • InterCity                          :   4797 corridors ( 10.4%)
   • TGV                                :   2435 corridors (  5.3%)
   • EuroNight                          :   1390 corridors (  3.0%)
   • Train Nuit                         :   1386 corridors (  3.0%)
   • InterCity Nuit                     :   1277 corridors (  2.8%)
   • ICE                                :    857 corridors (  1.9%)
   • Nightjet                           :    599 corridors (  1.3%)
   • ICE Nuit                           :    469 corridors (  1.0%)
   • TGV Nuit                           :    364 corridors (  0.8%)
   • EuroCity                           :    250 corridors (  0.5%)
   • AVE                                :    100 corridors (  0.2%)
   • AVE Nuit                           :     13 corridors (  0.0%)
   • EuroCity Nuit                      :     12 corridors (  0.0%)

3) STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                   Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
AVE                                      100           166.6            1.32
AVE Nuit                                  13           177.5            1.41
EuroCity                                 250           174.0            2.64
EuroCity Nuit                             12           163.8            0.14
EuroNight                               1390           140.0            1.50
ICE                                      857           212.6            8.36
ICE Nuit                                 469           183.3            7.08
InterCity                               4797           141.0            2.68
InterCity Nuit                          1277           135.1            2.30
Nightjet                                 599           179.6            6.30
TGV                                     2435           462.2            6.91
TGV Nuit                                 364           522.9            7.75
Train Longue Distance                  26121           209.7            4.03
Train Longue Distance Nuit              6036           185.6            3.61
Train Nuit                              1386           202.4            2.89

4) TOP 10 CORRIDORS LES PLUS LONGS
--------------------------------------------------------------------------------
    1. Modane                    → Brest (F)                
       TGV                          918.3 km   41.31 kg CO2
    2. Brest (F)                 → Modane                   
       TGV Nuit                     918.3 km    7.29 kg CO2
    3. Modane                    → Brest (F)                
       Train Longue Distance        918.3 km   41.31 kg CO2
    4. Brest (F)                 → Modane                   
       Train Longue Distance Nuit    918.3 km    7.29 kg CO2
    5. Bruxelles Midi            → Perpignan                
       Train Longue Distance        911.8 km   17.69 kg CO2
    6. Perpignan                 → Bruxelles Midi           
       Train Longue Distance        911.8 km   27.34 kg CO2
    7. Brest (F)                 → Bourg-Saint-Maurice      
       TGV Nuit                     906.0 km    7.19 kg CO2
    8. Bourg-Saint-Maurice       → Brest (F)                
       TGV                          906.0 km   40.75 kg CO2
    9. Brest (F)                 → Bourg-Saint-Maurice      
       Train Longue Distance Nuit    906.0 km    7.19 kg CO2
   10. Bourg-Saint-Maurice       → Brest (F)                
       Train Longue Distance        906.0 km   40.75 kg CO2

5) COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   TRAINS (corridors avec vol existant : 43782)
      CO2 train moyen     : 4.08 kg/passager
      CO2 avion moyen     : 96.85 kg/passager
      CO2 économisé moyen : 92.77 kg/passager
      CO2 économisé médian: 85.99 kg/passager
   CONCLUSION: L'avion émet 23.7x plus de CO2 que le train sur ces corridors

6) COUVERTURE DES DONNÉES ENRICHIES
--------------------------------------------------------------------------------
   Gares uniques (origine)        : 3242
   Gares uniques (destination)    : 3299
   Total gares uniques            : 3537
   Couverture fréquentation SNCF  : 62.5%
   Couverture population ville    : 72.7%
   Couverture service_share GTFS  : 100.0%

7) TOP 10 GARES LES PLUS CONNECTÉES
--------------------------------------------------------------------------------
    1. Paris Gare de Lyon                           :  672 connexions
    2. Lyon Part Dieu                               :  649 connexions
    3. Lyon Perrache                                :  583 connexions
    4. Köln Hbf                                     :  503 connexions
    5. Nantes                                       :  448 connexions
    6. Strasbourg                                   :  394 connexions
    7. Dijon                                        :  383 connexions
    8. Basel SBB                                    :  346 connexions
    9. Rennes                                       :  336 connexions
   10. Karlsruhe Hbf                                :  329 connexions

================================================================================
Pipeline ETL: ObRail Europe — 46 106 corridors ferroviaires français
Données: GTFS SNCF + Back on Track + SNCF Fréquentation + INSEE + GeoNames
Généré par: analyse/analyse_resultat.py
