================================================================================
📊 RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-03-03 08:25:34
Source: ./data/staging/final_routes.csv
================================================================================

1️⃣  VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 513
   Routes valides (avec distance): 380
   Routes invalides              : 133

   Sources de données:
      • back_on_track       :  377 routes
      • mobility_db         :   77 routes
      • airports            :   59 routes

2️⃣  RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • InterCity                          :  317 trajets ( 61.8%)
   • EuroNight                          :   61 trajets ( 11.9%)
   • Avion                              :   59 trajets ( 11.5%)
   • Nightjet                           :   32 trajets (  6.2%)
   • Train Longue Distance              :   28 trajets (  5.5%)
   • Train Longue Distance Nuit         :   16 trajets (  3.1%)

3️⃣  STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
Avion                                   59          261.51           58.84
EuroNight                               61          401.97            3.62
InterCity                              186          592.95            5.34
Nightjet                                30          663.77            5.97
Train Longue Distance                   28          375.35            3.38
Train Longue Distance Nuit              16          543.99            4.90

4️⃣  TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Gare de Saint-Nazaire     → Gare de Donges           
       Train Longue Distance       5262.4 km   47.36 kg CO2
    2. Gare de Saint-Nazaire     → Gare de Donges           
       Train Longue Distance Nuit   5262.4 km   47.36 kg CO2
    3. Edirne                    → Villach Hbf              
       InterCity                   1154.3 km   10.39 kg CO2
    4. Villach Hbf               → Edirne                   
       InterCity                   1154.3 km   10.39 kg CO2
    5. Konya                     → İzmir Basmane            
       InterCity                   1127.1 km   10.14 kg CO2
    6. İzmir Basmane             → Konya                    
       InterCity                   1127.1 km   10.14 kg CO2
    7. Milano Centrale           → Siracusa                 
       InterCity                   1064.2 km    9.58 kg CO2
    8. Siracusa                  → Milano Centrale          
       InterCity                   1064.2 km    9.58 kg CO2
    9. Reggio Calabria Centrale  → Torino Lingotto          
       InterCity                   1015.4 km    9.14 kg CO2
   10. Torino Lingotto           → Reggio Calabria Centrale 
       InterCity                   1015.4 km    9.14 kg CO2

5️⃣  COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   🚆 TRAINS (454 routes)
      Distance moyenne    :   541.9 km
      CO2 moyen           :    4.88 kg
      CO2 par km          :  0.0090 kg/km
      Distance min/max    : 100.2 / 5262.4 km

   ✈️  AVIONS (59 routes)
      Distance moyenne    :   261.5 km
      CO2 moyen           :   58.84 kg
      CO2 par km          :  0.2250 kg/km
      Distance min/max    : 101.5 / 688.1 km

   ⚠️  CONCLUSION: L'avion émet 25.0x plus de CO2 par km que le train
   💚 Économie moyenne en prenant le train: 53.96 kg CO2

6️⃣  CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • InterCity                          :  317 trajets ( 69.8% des trains)
   • EuroNight                          :   61 trajets ( 13.4% des trains)
   • Nightjet                           :   32 trajets (  7.0% des trains)
   • Train Longue Distance              :   28 trajets (  6.2% des trains)
   • Train Longue Distance Nuit         :   16 trajets (  3.5% des trains)

7️⃣  ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 109
   Distance moyenne              : 496.6 km
   CO2 moyen                     : 4.47 kg

      • EuroNight                     :  61 trajets
      • Nightjet                      :  32 trajets
      • Train Longue Distance Nuit    :  16 trajets

8️⃣  COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 208
   Gares/Aéroports de destination: 237
   Total unique                  : 263

   Top 10 gares/aéroports les plus connectés:
       1. Kyiv Pasazhyrskyi                       :  42 connexions
       2. București Nord                          :  27 connexions
       3. Paris Austerlitz                        :  18 connexions
       4. Odesa Holovna                           :  18 connexions
       5. Gare de Nantes                          :  17 connexions
       6. Paris-Orly Airport                      :  16 connexions
       7. Kharkiv                                 :  16 connexions
       8. Praha hl.n.                             :  15 connexions
       9. Przemyśl Gł.                            :  14 connexions
      10. Berlin Hbf                              :  14 connexions

9️⃣  QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 513
   Données complètes (distance)  : 380 (74.1%)
   Données complètes (CO2)       : 380 (74.1%)
   Horaires de départ renseignés : 450 (87.7%)
   Horaires d'arrivée renseignés : 450 (87.7%)

================================================================================
✅ RAPPORT GÉNÉRÉ AVEC SUCCÈS
================================================================================

Fichier de sortie: ./analyse/rapport_analyse.md
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
