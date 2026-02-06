"""
TRANSFORMATION ETL - PySpark (Étape 1 : Filtre Grossier)
=========================================================
Pipeline de nettoyage initial des données brutes.

OBJECTIF : Nettoyer les données en ne gardant que les datasets
           qui possèdent des données dans les colonnes minimales exploitables.

Colonnes minimales obligatoires :
    - origin           : Nom gare/aéroport départ
    - destination      : Nom gare/aéroport arrivée
    - vehicule_type    : Type de transport (train jour/nuit)
    - station_lat/long : Coordonnées des gares
    - aero_lat/long    : Coordonnées des aéroports (pour intermodalité)
    - category         : Catégorie d'aéroport
    - departure_time   : Heure de départ
    - arrival_time     : Heure d'arrivée

ÉTAPES DU PIPELINE :
    1. Rassembler les DataFrames de toutes les sources (déjà extraites)
    2. Nettoyer les noms de gares
    3. Filtre grossier : supprimer datasets sans données dans les colonnes minimales
    4. Dédoublonner
    5. Export staging

DONNÉES EN ENTRÉE (depuis extraction.py) :
    - ./data/raw/mobility_gtfs/   → Dossiers GTFS par provider
    - ./data/raw/backontrack_csv/ → Fichiers CSV Back on Track
    - ./data/raw/airports/        → Fichiers CSV OurAirports

À VENIR (étapes suivantes) :
    - Calcul distance_km via Haversine
    - Calcul co2_kg avec facteurs fixes
"""

import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, regexp_replace, when, 
    first, last, coalesce, lower
)
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
from functools import reduce

# === CONFIGURATION ===
RAW_MOBILITY_DIR = "./data/raw/mobility_gtfs"
RAW_BACKONTRACK_DIR = "./data/raw/backontrack_csv"
RAW_AIRPORTS_DIR = "./data/raw/airports"
OUTPUT_DIR = "./data/staging"
OUTPUT_FILE = "staging_routes.csv"
OUTPUT_AIRPORTS_FILE = "staging_airports.csv"

# Colonnes MINIMALES obligatoires pour les trajets
REQUIRED_COLUMNS_ROUTES = ['origin', 'destination', 'vehicule_type', 'station_lat', 'station_long', 'departure_time', 'arrival_time']

# Colonnes pour les aéroports
REQUIRED_COLUMNS_AIRPORTS = ['airport_name', 'aero_lat', 'aero_long', 'category', 'iata_code', 'country_code']


# ===========================
# INITIALISATION SPARK
# ===========================

def get_spark_session():
    """Crée ou récupère la session Spark."""
    return SparkSession.builder \
        .appName("CO2_ETL_Stage1_Filter") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


# ===========================
# FONCTIONS DE NETTOYAGE
# ===========================

def clean_station_name(df, column_name):
    """
    Nettoie les noms de gares/aéroports dans une colonne.
    - Trim des espaces
    - Suppression des codes type "(ABC)" ou "[XYZ]"
    - Suppression des espaces multiples
    """
    return df.withColumn(
        column_name,
        trim(
            regexp_replace(
                regexp_replace(col(column_name), r'\s*[\(\[][A-Z0-9]{2,5}[\)\]]\s*$', ''),
                r'\s+', ' '
            )
        )
    )


def validate_required_columns(df):
    """
    Filtre les lignes qui n'ont PAS les colonnes minimales remplies.
    Retourne uniquement les lignes exploitables.
    """
    # Filtrer : origin et destination non nulls, non vides, et différents
    df_valid = df.filter(
        (col('origin').isNotNull()) & 
        (col('destination').isNotNull()) &
        (trim(col('origin')) != '') &
        (trim(col('destination')) != '') &
        (col('origin') != col('destination'))
    )
    
    return df_valid


# ===========================
# LECTURE MOBILITY DATABASE (GTFS)
# ===========================

def read_mobility_gtfs(spark, provider_dir):
    """
    Lit les données GTFS d'un provider déjà extrait.
    Retourne un DataFrame avec origin, destination, vehicule_type, coordonnées et horaires.
    """
    provider_name = os.path.basename(provider_dir)
    print(f"   📂 {provider_name}...", end=" ", flush=True)
    
    try:
        # Vérifier les fichiers requis
        required = ['stops.txt', 'stop_times.txt', 'trips.txt', 'routes.txt']
        for f in required:
            if not os.path.exists(os.path.join(provider_dir, f)):
                print(f"⚠️ (manque {f})")
                return None
        
        # === LECTURE DES FICHIERS GTFS ===
        stops = spark.read.option("header", "true").csv(f"{provider_dir}/stops.txt")
        stop_times = spark.read.option("header", "true").csv(f"{provider_dir}/stop_times.txt")
        trips = spark.read.option("header", "true").csv(f"{provider_dir}/trips.txt")
        routes = spark.read.option("header", "true").csv(f"{provider_dir}/routes.txt")
        
        # === FILTRE : TRAINS UNIQUEMENT ===
        # route_type : 2 = Rail, 100-117 = Railway Service
        routes = routes.withColumn('route_type', col('route_type').cast('int'))
        rail_routes = routes.filter(
            (col('route_type') == 2) | 
            ((col('route_type') >= 100) & (col('route_type') <= 117))
        )
        
        if rail_routes.count() == 0:
            print("⚠️ (pas de trains)")
            return None
        
        # Récupérer le route_type pour déterminer vehicule_type plus tard
        rail_trips = trips.join(
            rail_routes.select('route_id', 'route_type'), 
            'route_id', 
            'inner'
        )
        
        rail_stop_times = stop_times.join(
            rail_trips.select('trip_id', 'route_type'), 
            'trip_id', 
            'inner'
        )
        
        # === AGRÉGATION : ORIGINE & DESTINATION PAR TRIP ===
        rail_stop_times = rail_stop_times.withColumn(
            'stop_sequence', col('stop_sequence').cast('int')
        )
        
        window_asc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').asc())
        window_desc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').desc())
        
        trip_bounds = rail_stop_times \
            .withColumn('origin_stop_id', first('stop_id').over(window_asc)) \
            .withColumn('dest_stop_id', first('stop_id').over(window_desc)) \
            .withColumn('route_type_val', first('route_type').over(window_asc)) \
            .withColumn('departure_time', first('departure_time').over(window_asc)) \
            .withColumn('arrival_time', first('arrival_time').over(window_desc)) \
            .select('trip_id', 'origin_stop_id', 'dest_stop_id', 'route_type_val', 'departure_time', 'arrival_time') \
            .distinct()
        
        # === ENRICHISSEMENT : NOMS ET COORDONNÉES DES GARES ===
        # Jointure origine avec coordonnées
        df = trip_bounds.join(
            stops.select(
                col('stop_id').alias('origin_stop_id'),
                col('stop_name').alias('origin'),
                col('stop_lat').cast(DoubleType()).alias('origin_lat'),
                col('stop_lon').cast(DoubleType()).alias('origin_long')
            ),
            'origin_stop_id'
        )
        
        # Jointure destination avec coordonnées
        df = df.join(
            stops.select(
                col('stop_id').alias('dest_stop_id'),
                col('stop_name').alias('destination'),
                col('stop_lat').cast(DoubleType()).alias('dest_lat'),
                col('stop_lon').cast(DoubleType()).alias('dest_long')
            ),
            'dest_stop_id'
        )
        
        # === DÉTERMINATION DU TYPE DE VÉHICULE ===
        # route_type 102 = Train de nuit, sinon Jour
        df = df.withColumn(
            'vehicule_type',
            when(col('route_type_val') == 102, 'Train Nuit').otherwise('Train Jour')
        )
        
        # === SÉLECTION COLONNES ===
        df = df.select(
            'origin', 'destination', 'vehicule_type',
            'origin_lat', 'origin_long', 'dest_lat', 'dest_long',
            'departure_time', 'arrival_time'
        )
        
        # Renommer pour correspondre aux colonnes standardisées
        df = df.withColumnRenamed('origin_lat', 'station_lat') \
               .withColumnRenamed('origin_long', 'station_long') \
               .withColumnRenamed('dest_lat', 'station_lat_dest') \
               .withColumnRenamed('dest_long', 'station_long_dest')
        
        # Ajouter métadonnées source
        df = df.withColumn('source', lit('mobility_db')) \
               .withColumn('provider', lit(provider_name))
        
        count = df.count()
        print(f"✅ ({count} trajets)")
        return df
        
    except Exception as e:
        print(f"❌ ({e})")
        return None


def read_all_mobility(spark):
    """
    Lit tous les providers GTFS de Mobility Database (déjà extraits).
    """
    print("\n🌐 Lecture Mobility Database...")
    
    if not os.path.exists(RAW_MOBILITY_DIR):
        print(f"   ❌ Dossier introuvable : {RAW_MOBILITY_DIR}")
        return None
    
    all_dfs = []
    
    for provider_name in os.listdir(RAW_MOBILITY_DIR):
        provider_path = os.path.join(RAW_MOBILITY_DIR, provider_name)
        if os.path.isdir(provider_path):
            df = read_mobility_gtfs(spark, provider_path)
            if df is not None:
                all_dfs.append(df)
    
    if all_dfs:
        result = reduce(lambda a, b: a.union(b), all_dfs)
        total = result.count()
        print(f"   📊 Mobility Database : {total} trajets lus")
        return result
    
    return None


# ===========================
# LECTURE BACK ON TRACK
# ===========================

def read_backontrack(spark):
    """
    Lit les données Back on Track (déjà extraites avec colonnes mappées).
    Retourne un DataFrame avec origin, destination, vehicule_type, coordonnées et horaires.
    """
    print("\n📊 Lecture Back on Track...")
    
    try:
        files = {
            'routes': 'back_on_track_routes.csv',
            'trips': 'back_on_track_trips.csv',
            'stops': 'back_on_track_stops.csv',
            'trip_stop': 'back_on_track_trip_stop.csv'
        }
        
        # Vérifier existence des fichiers
        for key, filename in files.items():
            filepath = os.path.join(RAW_BACKONTRACK_DIR, filename)
            if not os.path.exists(filepath):
                print(f"   ⚠️ Fichier manquant : {filename}")
                return None
        
        # === LECTURE (colonnes déjà mappées par extraction.py) ===
        routes = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_routes.csv")
        trips = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_trips.csv")
        stops = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_stops.csv")
        trip_stop = spark.read.option("header", "true").csv(f"{RAW_BACKONTRACK_DIR}/back_on_track_trip_stop.csv")
        
        print(f"   ✅ Fichiers chargés")
        
        # Déterminer les noms de colonnes (mappées ou originales)
        stops_cols = stops.columns
        lat_col = 'station_lat' if 'station_lat' in stops_cols else 'stop_lat'
        lon_col = 'station_long' if 'station_long' in stops_cols else 'stop_lon'
        name_col = 'station_name' if 'station_name' in stops_cols else 'stop_name'
        
        route_cols = routes.columns
        type_col = 'vehicule_type' if 'vehicule_type' in route_cols else 'route_type'
        
        trip_stop_cols = trip_stop.columns
        dep_col = 'departure_time' if 'departure_time' in trip_stop_cols else 'departure_time'
        arr_col = 'arrival_time' if 'arrival_time' in trip_stop_cols else 'arrival_time'
        
        # === FUSIONS avec coordonnées ===
        merged = trip_stop.join(trips, 'trip_id', 'left') \
                          .join(routes.select('route_id', col(type_col).alias('route_type')), 'route_id', 'left') \
                          .join(stops.select(
                              'stop_id', 
                              col(name_col).alias('stop_name'),
                              col(lat_col).cast(DoubleType()).alias('stop_lat'),
                              col(lon_col).cast(DoubleType()).alias('stop_lon')
                          ), 'stop_id', 'left')
        
        # === AGRÉGATION O-D avec coordonnées et horaires ===
        merged = merged.withColumn('stop_sequence', col('stop_sequence').cast('int'))
        
        window_asc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').asc())
        window_desc = Window.partitionBy('trip_id').orderBy(col('stop_sequence').desc())
        
        df = merged \
            .withColumn('origin', first('stop_name').over(window_asc)) \
            .withColumn('destination', first('stop_name').over(window_desc)) \
            .withColumn('route_type_val', first('route_type').over(window_asc)) \
            .withColumn('origin_lat', first('stop_lat').over(window_asc)) \
            .withColumn('origin_long', first('stop_lon').over(window_asc)) \
            .withColumn('dest_lat', first('stop_lat').over(window_desc)) \
            .withColumn('dest_long', first('stop_lon').over(window_desc)) \
            .withColumn('departure_time', first(dep_col).over(window_asc)) \
            .withColumn('arrival_time', first(arr_col).over(window_desc)) \
            .select('trip_id', 'origin', 'destination', 'route_type_val', 
                    'origin_lat', 'origin_long', 'dest_lat', 'dest_long',
                    'departure_time', 'arrival_time') \
            .distinct()
        
        # === TYPE DE VÉHICULE ===
        # route_type 102 = Train de nuit
        df = df.withColumn(
            'vehicule_type',
            when(col('route_type_val') == '102', 'Train Nuit').otherwise('Train Jour')
        )
        
        # === SÉLECTION COLONNES ===
        df = df.select(
            'origin', 'destination', 'vehicule_type',
            'origin_lat', 'origin_long', 'dest_lat', 'dest_long',
            'departure_time', 'arrival_time'
        )
        
        # Renommer pour correspondre aux colonnes standardisées
        df = df.withColumnRenamed('origin_lat', 'station_lat') \
               .withColumnRenamed('origin_long', 'station_long') \
               .withColumnRenamed('dest_lat', 'station_lat_dest') \
               .withColumnRenamed('dest_long', 'station_long_dest')
        
        # Ajouter métadonnées
        df = df.withColumn('source', lit('back_on_track')) \
               .withColumn('provider', lit('BackOnTrack'))
        
        count = df.count()
        print(f"   📊 Back on Track : {count} trajets lus")
        return df
        
    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return None


# ===========================
# LECTURE OURAIRPORTS
# ===========================

def read_airports(spark):
    """
    Lit les données aéroports OurAirports (déjà extraites avec colonnes mappées).
    Retourne un DataFrame avec les colonnes standardisées.
    """
    print("\n✈️  Lecture OurAirports...")
    
    try:
        if not os.path.exists(RAW_AIRPORTS_DIR):
            print(f"   ❌ Dossier introuvable : {RAW_AIRPORTS_DIR}")
            return None
        
        # Trouver le fichier le plus récent
        airport_files = glob.glob(f"{RAW_AIRPORTS_DIR}/airports_*.csv")
        if not airport_files:
            print("   ❌ Aucun fichier airports trouvé")
            return None
        
        # Prendre le fichier le plus récent
        latest_file = sorted(airport_files)[-1]
        print(f"   📂 Fichier : {os.path.basename(latest_file)}")
        
        # === LECTURE (colonnes déjà mappées par extraction.py) ===
        df = spark.read.option("header", "true").csv(latest_file)
        
        # Vérifier les colonnes disponibles
        cols = df.columns
        
        # Mapper les colonnes si nécessaire (backup si mapping pas appliqué à l'extraction)
        col_mappings = {
            'latitude_deg': 'aero_lat',
            'longitude_deg': 'aero_long',
            'type': 'category',
            'name': 'airport_name',
            'iso_country': 'country_code'
        }
        
        for old_col, new_col in col_mappings.items():
            if old_col in cols and new_col not in cols:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Filtrer les colonnes utiles
        available_cols = df.columns
        select_cols = []
        
        for col_name in REQUIRED_COLUMNS_AIRPORTS:
            if col_name in available_cols:
                select_cols.append(col_name)
        
        # Ajouter ident si disponible (identifiant unique)
        if 'ident' in available_cols:
            select_cols.append('ident')
        
        if not select_cols:
            print("   ❌ Aucune colonne requise trouvée")
            return None
        
        df = df.select(*select_cols)
        
        # Convertir les types
        if 'aero_lat' in df.columns:
            df = df.withColumn('aero_lat', col('aero_lat').cast(DoubleType()))
        if 'aero_long' in df.columns:
            df = df.withColumn('aero_long', col('aero_long').cast(DoubleType()))
        
        # Filtrer les aéroports valides (avec coordonnées et catégorie pertinente)
        df = df.filter(
            (col('aero_lat').isNotNull()) & 
            (col('aero_long').isNotNull()) &
            (col('category').isin('large_airport', 'medium_airport', 'small_airport'))
        )
        
        # Filtrer les pays européens pertinents
        if 'country_code' in df.columns:
            european_countries = ['FR', 'DE', 'CH', 'IT', 'ES', 'BE', 'NL', 'AT', 'GB', 'PT', 'PL', 'CZ', 'DK', 'SE', 'NO', 'FI']
            df = df.filter(col('country_code').isin(european_countries))
        
        count = df.count()
        print(f"   ✅ {count} aéroports chargés (Europe)")
        return df
        
    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return None


# ===========================
# ORCHESTRATION ETL
# ===========================

def run_transform():
    """
    Pipeline de transformation (Filtre Grossier).
    
    Prérequis : extraction.py doit avoir été exécuté avant !
    Les données brutes doivent être dans :
        - ./data/raw/mobility_gtfs/
        - ./data/raw/backontrack_csv/
        - ./data/raw/airports/
    
    ÉTAPES :
        1. Rassembler les DataFrames trajets + aéroports
        2. Nettoyer les noms de gares
        3. Filtre grossier : supprimer les datasets sans données dans les colonnes minimales
        4. Dédoublonner
        5. Export staging (trajets + aéroports séparément)
    """
    print("=" * 60)
    print("🚀 TRANSFORMATION - Filtre Grossier")
    print("=" * 60)
    print(f"📋 Colonnes trajets requises : {REQUIRED_COLUMNS_ROUTES}")
    print(f"📋 Colonnes aéroports requises : {REQUIRED_COLUMNS_AIRPORTS}")
    print(f"📂 Données en entrée :")
    print(f"   - {RAW_MOBILITY_DIR}")
    print(f"   - {RAW_BACKONTRACK_DIR}")
    print(f"   - {RAW_AIRPORTS_DIR}")
    print("=" * 60)
    
    # Initialiser Spark
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # =========================================
    # ÉTAPE 1 : RASSEMBLER LES DATAFRAMES
    # =========================================
    print("\n" + "=" * 40)
    print("📥 ÉTAPE 1 : Rassemblement des DataFrames")
    print("=" * 40)
    
    df_mobility = read_all_mobility(spark)
    df_backontrack = read_backontrack(spark)
    df_airports = read_airports(spark)
    
    all_route_dfs = [df for df in [df_mobility, df_backontrack] if df is not None]
    
    if not all_route_dfs:
        print("\n❌ Aucune donnée de trajets trouvée. As-tu exécuté extraction.py ?")
        spark.stop()
        return None
    
    # Union de tous les DataFrames trajets
    df_combined = reduce(lambda a, b: a.union(b), all_route_dfs)
    total_brut = df_combined.count()
    print(f"\n✅ Total trajets rassemblés : {total_brut}")
    
    if df_airports is not None:
        print(f"✅ Aéroports chargés : {df_airports.count()}")
    
    # =========================================
    # ÉTAPE 2 : NETTOYAGE DES NOMS
    # =========================================
    print("\n" + "=" * 40)
    print("🧹 ÉTAPE 2 : Nettoyage des noms de gares")
    print("=" * 40)
    
    df_cleaned = clean_station_name(df_combined, 'origin')
    df_cleaned = clean_station_name(df_cleaned, 'destination')
    print("   ✅ Noms nettoyés (espaces, codes supprimés)")
    
    # =========================================
    # ÉTAPE 3 : FILTRE GROSSIER
    # =========================================
    print("\n" + "=" * 40)
    print("🔍 ÉTAPE 3 : Filtre grossier (colonnes minimales)")
    print("=" * 40)
    
    df_valid = validate_required_columns(df_cleaned)
    total_valid = df_valid.count()
    
    dropped = total_brut - total_valid
    print(f"   ✅ Lignes valides : {total_valid}")
    print(f"   🗑️ Lignes supprimées (données manquantes) : {dropped}")
    
    # =========================================
    # ÉTAPE 4 : DÉDOUBLONNAGE
    # =========================================
    print("\n" + "=" * 40)
    print("🔄 ÉTAPE 4 : Dédoublonnage")
    print("=" * 40)
    
    df_dedup = df_valid.dropDuplicates(['origin', 'destination', 'vehicule_type', 'source'])
    total_dedup = df_dedup.count()
    
    duplicates = total_valid - total_dedup
    print(f"   🔄 Doublons supprimés : {duplicates}")
    print(f"   📊 Total final : {total_dedup} trajets uniques")
    
    # =========================================
    # ÉTAPE 5 : EXPORT STAGING
    # =========================================
    print("\n" + "=" * 40)
    print("💾 ÉTAPE 5 : Export staging")
    print("=" * 40)
    
    import shutil
    
    # Export trajets
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df_dedup.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f"{OUTPUT_DIR}/temp_output")
    
    temp_files = glob.glob(f"{OUTPUT_DIR}/temp_output/part-*.csv")
    if temp_files:
        shutil.move(temp_files[0], output_path)
        shutil.rmtree(f"{OUTPUT_DIR}/temp_output")
    
    print(f"   📂 Trajets exportés : {output_path}")
    
    # Export aéroports
    if df_airports is not None:
        airports_path = os.path.join(OUTPUT_DIR, OUTPUT_AIRPORTS_FILE)
        df_airports.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f"{OUTPUT_DIR}/temp_airports")
        
        temp_files = glob.glob(f"{OUTPUT_DIR}/temp_airports/part-*.csv")
        if temp_files:
            shutil.move(temp_files[0], airports_path)
            shutil.rmtree(f"{OUTPUT_DIR}/temp_airports")
        
        print(f"   📂 Aéroports exportés : {airports_path}")
    
    # =========================================
    # RÉSUMÉ
    # =========================================
    print("\n" + "=" * 60)
    print("🎉 TRANSFORMATION TERMINÉE")
    print("=" * 60)
    
    print("\n📈 Répartition par source :")
    df_dedup.groupBy('source').count().show()
    
    print("📈 Répartition par type de véhicule :")
    df_dedup.groupBy('vehicule_type').count().show()
    
    print("\n📋 Colonnes des trajets :")
    print(f"   {df_dedup.columns}")
    
    if df_airports is not None:
        print("\n📋 Colonnes des aéroports :")
        print(f"   {df_airports.columns}")
        
        print("\n📈 Répartition des aéroports par catégorie :")
        df_airports.groupBy('category').count().show()
    
    print("\n📋 Prochaines étapes :")
    print("   1. Calculer distance_km via Haversine")
    print("   2. Calculer co2_kg avec facteurs fixes")
    print("   3. Joindre aéroports proches des gares (intermodalité)")
    print("   4. Préparer pour le load dans le datamart")
    
    spark.stop()
    return df_dedup, df_airports


if __name__ == "__main__":
    run_transform()
