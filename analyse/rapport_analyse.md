================================================================================
📊 RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-02-27 11:40:44
Source: ./data/staging/final_routes.csv
================================================================================

1️⃣  VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 508
   Routes valides (avec distance): 375
   Routes invalides              : 133

   Sources de données:
      • back_on_track       :  373 routes
      • mobility_db         :   76 routes
      • airports            :   59 routes

2️⃣  RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • Train Jour                         :  373 trajets ( 73.4%)
   • Avion                              :   59 trajets ( 11.6%)
   • EuroNight                          :   33 trajets (  6.5%)
   • Train Longue Distance              :   27 trajets (  5.3%)
   • Train Longue Distance Nuit         :   16 trajets (  3.1%)

3️⃣  STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
Avion                                   59          261.51           58.84
EuroNight                               33          193.39            1.74
Train Jour                             240          605.44            5.45
Train Longue Distance                   27          384.77            3.46
Train Longue Distance Nuit              16          543.99            4.90

4️⃣  TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Gare de Saint-Nazaire     → Gare de Donges           
       Train Longue Distance       5262.4 km   47.36 kg CO2
    2. Gare de Saint-Nazaire     → Gare de Donges           
       Train Longue Distance Nuit   5262.4 km   47.36 kg CO2
    3. Edirne                    → Villach Hbf              
       Train Jour                  1154.3 km   10.39 kg CO2
    4. Villach Hbf               → Edirne                   
       Train Jour                  1154.3 km   10.39 kg CO2
    5. Konya                     → İzmir Basmane            
       Train Jour                  1127.1 km   10.14 kg CO2
    6. İzmir Basmane             → Konya                    
       Train Jour                  1127.1 km   10.14 kg CO2
    7. Milano Centrale           → Siracusa                 
       Train Jour                  1064.2 km    9.58 kg CO2
    8. Siracusa                  → Milano Centrale          
       Train Jour                  1064.2 km    9.58 kg CO2
    9. Reggio Calabria Centrale  → Torino Lingotto          
       Train Jour                  1015.4 km    9.14 kg CO2
   10. Torino Lingotto           → Reggio Calabria Centrale 
       Train Jour                  1015.4 km    9.14 kg CO2

5️⃣  COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   🚆 TRAINS (449 routes)
      Distance moyenne    :   540.4 km
      CO2 moyen           :    4.86 kg
      CO2 par km          :  0.0090 kg/km
      Distance min/max    : 100.2 / 5262.4 km

   ✈️  AVIONS (59 routes)
      Distance moyenne    :   261.5 km
      CO2 moyen           :   58.84 kg
      CO2 par km          :  0.2250 kg/km
      Distance min/max    : 101.5 / 688.1 km

   ⚠️  CONCLUSION: L'avion émet 25.0x plus de CO2 par km que le train
   💚 Économie moyenne en prenant le train: 53.97 kg CO2

6️⃣  CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • Train Jour                         :  373 trajets ( 83.1% des trains)
   • EuroNight                          :   33 trajets (  7.3% des trains)
   • Train Longue Distance              :   27 trajets (  6.0% des trains)
   • Train Longue Distance Nuit         :   16 trajets (  3.6% des trains)

7️⃣  ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 49
   Distance moyenne              : 307.9 km
   CO2 moyen                     : 2.77 kg

      • EuroNight                     :  33 trajets
      • Train Longue Distance Nuit    :  16 trajets

8️⃣  COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 207
   Gares/Aéroports de destination: 236
   Total unique                  : 262

   Top 10 gares/aéroports les plus connectés:
       1. Kyiv Pasazhyrskyi                       :  42 connexions
       2. București Nord                          :  27 connexions
       3. Paris Austerlitz                        :  18 connexions
       4. Odesa Holovna                           :  18 connexions
       5. Gare de Nantes                          :  17 connexions
       6. Paris-Orly Airport                      :  16 connexions
       7. Kharkiv                                 :  16 connexions
       8. Praha hl.n.                             :  15 connexions
       9. Zürich HB                               :  14 connexions
      10. Lviv                                    :  14 connexions

9️⃣  QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 508
   Données complètes (distance)  : 375 (73.8%)
   Données complètes (CO2)       : 375 (73.8%)
   Horaires de départ renseignés : 445 (87.6%)
   Horaires d'arrivée renseignés : 445 (87.6%)

================================================================================
✅ RAPPORT GÉNÉRÉ AVEC SUCCÈS
================================================================================

Fichier de sortie: ./data/staging/rapport_analyse.txt
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
