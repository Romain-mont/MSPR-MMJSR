================================================================================
📊 RAPPORT D'ANALYSE AUTOMATIQUE - PIPELINE ETL ObRail Europe
================================================================================
Date de génération: 2026-03-01 15:51:42
Source: ./data/staging/final_routes.csv
================================================================================

1️⃣  VUE D'ENSEMBLE
--------------------------------------------------------------------------------
   Total routes extraites        : 393
   Routes valides (avec distance): 260
   Routes invalides              : 133

   Sources de données:
      • back_on_track       :  377 routes
      • airports            :   16 routes

2️⃣  RÉPARTITION PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
   • InterCity                          :  317 trajets ( 80.7%)
   • Nightjet                           :   32 trajets (  8.1%)
   • EuroNight                          :   28 trajets (  7.1%)
   • Avion                              :   16 trajets (  4.1%)

3️⃣  STATISTIQUES PAR TYPE DE VÉHICULE
--------------------------------------------------------------------------------
Type                                 Count   Dist.Moy (km)    CO2 Moy (kg)
--------------------------------------------------------------------------------
Avion                                   16          570.96          128.47
EuroNight                               28          647.81            5.83
InterCity                              186          592.95            5.34
Nightjet                                30          663.77            5.97

4️⃣  TOP 10 ROUTES LES PLUS LONGUES
--------------------------------------------------------------------------------
    1. Edirne                    → Villach Hbf              
       InterCity                   1154.3 km   10.39 kg CO2
    2. Villach Hbf               → Edirne                   
       InterCity                   1154.3 km   10.39 kg CO2
    3. Konya                     → İzmir Basmane            
       InterCity                   1127.1 km   10.14 kg CO2
    4. İzmir Basmane             → Konya                    
       InterCity                   1127.1 km   10.14 kg CO2
    5. Milano Centrale           → Siracusa                 
       InterCity                   1064.2 km    9.58 kg CO2
    6. Siracusa                  → Milano Centrale          
       InterCity                   1064.2 km    9.58 kg CO2
    7. Reggio Calabria Centrale  → Torino Lingotto          
       InterCity                   1015.4 km    9.14 kg CO2
    8. Torino Lingotto           → Reggio Calabria Centrale 
       InterCity                   1015.4 km    9.14 kg CO2
    9. Narvik                    → Stockholm C              
       InterCity                   1013.6 km    9.12 kg CO2
   10. Stockholm C               → Narvik                   
       InterCity                   1013.6 km    9.12 kg CO2

5️⃣  COMPARAISON ENVIRONNEMENTALE TRAIN vs AVION
--------------------------------------------------------------------------------
   🚆 TRAINS (377 routes)
      Distance moyenne    :   608.0 km
      CO2 moyen           :    5.47 kg
      CO2 par km          :  0.0090 kg/km
      Distance min/max    : 258.4 / 1154.3 km

   ✈️  AVIONS (16 routes)
      Distance moyenne    :   571.0 km
      CO2 moyen           :  128.47 kg
      CO2 par km          :  0.2250 kg/km
      Distance min/max    : 426.1 / 688.1 km

   ⚠️  CONCLUSION: L'avion émet 25.0x plus de CO2 par km que le train
   💚 Économie moyenne en prenant le train: 122.99 kg CO2

6️⃣  CLASSIFICATION DÉTAILLÉE DES TRAINS
--------------------------------------------------------------------------------
   • InterCity                          :  317 trajets ( 84.1% des trains)
   • Nightjet                           :   32 trajets (  8.5% des trains)
   • EuroNight                          :   28 trajets (  7.4% des trains)

7️⃣  ANALYSE DES TRAINS DE NUIT
--------------------------------------------------------------------------------
   Total trains de nuit détectés : 60
   Distance moyenne              : 656.1 km
   CO2 moyen                     : 5.90 kg

      • Nightjet                      :  32 trajets
      • EuroNight                     :  28 trajets

8️⃣  COUVERTURE GÉOGRAPHIQUE
--------------------------------------------------------------------------------
   Gares/Aéroports d'origine     : 161
   Gares/Aéroports de destination: 160
   Total unique                  : 164

   Top 10 gares/aéroports les plus connectés:
       1. Kyiv Pasazhyrskyi                       :  42 connexions
       2. București Nord                          :  27 connexions
       3. Odesa Holovna                           :  18 connexions
       4. Paris Austerlitz                        :  18 connexions
       5. Paris-Orly Airport                      :  16 connexions
       6. Kharkiv                                 :  16 connexions
       7. Praha hl.n.                             :  15 connexions
       8. Hamburg Altona                          :  14 connexions
       9. Zürich HB                               :  14 connexions
      10. Przemyśl Gł.                            :  14 connexions

9️⃣  QUALITÉ DES DONNÉES
--------------------------------------------------------------------------------
   Total lignes                  : 393
   Données complètes (distance)  : 260 (66.2%)
   Données complètes (CO2)       : 260 (66.2%)
   Horaires de départ renseignés : 373 (94.9%)
   Horaires d'arrivée renseignés : 373 (94.9%)

================================================================================
✅ RAPPORT GÉNÉRÉ AVEC SUCCÈS
================================================================================

Fichier de sortie: ./analyse/rapport_analyse.md
Généré par: analyse/analyse_resultat.py
Pipeline ETL: ObRail Europe - Comparatif Train vs Avion
