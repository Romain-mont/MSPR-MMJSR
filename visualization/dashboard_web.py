import os, sys, glob, shutil
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


RAW_MOBILITY_DIR    = "./data/raw/mobility_gtfs"
RAW_BACKONTRACK_DIR = "./data/raw/backontrack_csv"
RAW_AIRPORTS_DIR    = "./data/raw/airports"
OUTPUT_DIR          = "./data/staging"

OUTPUT_AIRPORTS_FILE   = "staging_airports.csv"
OUTPUT_INTERMODAL_FILE = "staging_intermodal.csv"
OUTPUT_FINAL_FILE      = "final_routes.csv"

REQUIRED_COLUMNS_AIRPORTS = ["airport_name", "aero_lat", "aero_long", "category", "iata_code", "country_code"]

CO2_FACTORS_PER_100KM = {
    "TGV": 0.29, "Train Jour": 0.29,
    "Intercité": 0.9, "Train Nuit": 0.9,
    "Avion Court": 22.5, "Avion Moyen": 18.45, "Avion Moyen-Long": 16.68, "Avion Long": 17.79,
    "default": 0.9
}
EARTH_RADIUS_KM = 6371.0

INTERMODAL_RADIUS_KM  = 50
BBOX_MARGIN_DEG       = 1.0
MIN_PLANE_DISTANCE_KM = 100

# 👉 Taille de grille (degrés). 0.5 = plus précis mais + de bins
# 1.0 = moins de bins, encore très correct vu ton BBOX_MARGIN_DEG=1.0
GRID_BIN_DEG = 1.0


# ===========================
# 1) SPARK SESSION (Windows-safe + RAM-friendly)
# ===========================
def get_spark_session():
    py = sys.executable

    # ⚠️ Sur 16 Go RAM, local[*] peut te tuer (trop de tasks/threads/shuffles).
    # 4 est un bon compromis. Ajuste si besoin.
    local_cores = os.environ.get("SPARK_LOCAL_CORES", "4")

    spark = (
        SparkSession.builder
        .appName("CO2_ETL_Europe")
        .master(f"local[{local_cores}]")
        .config("spark.pyspark.driver.python", py)
        .config("spark.pyspark.python", py)
        .config("spark.executorEnv.PYSPARK_PYTHON", py)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", py)

        # Stabilité mémoire/IO
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "1g")

        # ✅ Le nerf de la guerre: moins de partitions shuffle = moins de temp_shuffle
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "16")

        # ✅ Regrouper les petits fichiers → moins de tasks (souvent la cause du 1105 tasks)
        .config("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))  # 256MB
        .config("spark.sql.files.openCostInBytes", str(16 * 1024 * 1024))     # 16MB

        # AQE ok mais on veut qu'il coalesce
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.local.dir", "./data/spark_temp")
        .config("spark.python.worker.reuse", "true")
        .config("spark.ui.enabled", "false")

        # ✅ FIX CRITIQUE Windows / Hadoop NativeIO
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.hadoop.io.native.lib.available", "false")

        .getOrCreate()
    )
    return spark


# ===========================
# 2) HELPERS
# ===========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def read_csv(spark, path: str):
    return spark.read.option("header", "true").csv(path)

def export_single_csv(df, out_csv_path: str, tmp_dir: str):
    """
    Export un DF en CSV unique.
    On write dans un dossier temp, puis on déplace le part-*.csv.
    """
    shutil.rmtree(tmp_dir, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)

    part = next((f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")), None)
    if not part:
        raise RuntimeError("Aucun fichier part-*.csv généré par Spark.")

    if os.path.exists(out_csv_path):
        os.remove(out_csv_path)

    shutil.move(os.path.join(tmp_dir, part), out_csv_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)

def normalize_station_col(df, colname: str):
    return df.withColumn(
        colname,
        F.trim(
            F.regexp_replace(
                F.regexp_replace(F.col(colname), r"\s*[\(\[][A-Z0-9]{2,5}[\)\]]\s*$", ""),
                r"\s+", " "
            )
        )
    )

def validate_routes(df):
    return df.filter(
        F.col("origin").isNotNull() &
        F.col("destination").isNotNull() &
        (F.trim("origin") != "") &
        (F.trim("destination") != "") &
        (F.col("origin") != F.col("destination"))
    )


# ===========================
# 3) DISTANCE + CO2
# ===========================
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1r, lat2r = F.radians(lat1), F.radians(lat2)
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2) + (F.cos(lat1r) * F.cos(lat2r) * (F.sin(dlon / 2) ** 2))
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    dist = F.lit(EARTH_RADIUS_KM) * c

    return F.when(
        lat1.isNotNull() & lon1.isNotNull() & lat2.isNotNull() & lon2.isNotNull(),
        dist
    ).otherwise(F.lit(None).cast(DoubleType()))

def co2_kg(distance_km, vehicule_type):
    factor = (
        F.when(vehicule_type.contains("Avion") & (distance_km <= 1000), F.lit(CO2_FACTORS_PER_100KM["Avion Court"]))
         .when(vehicule_type.contains("Avion") & (distance_km <= 2000), F.lit(CO2_FACTORS_PER_100KM["Avion Moyen"]))
         .when(vehicule_type.contains("Avion") & (distance_km <= 5000), F.lit(CO2_FACTORS_PER_100KM["Avion Moyen-Long"]))
         .when(vehicule_type.contains("Avion"), F.lit(CO2_FACTORS_PER_100KM["Avion Long"]))
         .when(vehicule_type.isin("Train Jour", "Train Nuit"), F.lit(CO2_FACTORS_PER_100KM["default"]))
         .otherwise(F.lit(CO2_FACTORS_PER_100KM["default"]))
    )
    return F.when(
        distance_km.isNotNull() & vehicule_type.isNotNull(),
        (distance_km * factor / 100.0)
    ).otherwise(F.lit(None).cast(DoubleType()))


# ===========================
# 4) MOBILITY (GTFS)
# ===========================
def read_mobility_provider(spark, provider_dir: str):
    provider_name = os.path.basename(provider_dir)
    required = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"]
    if any(not os.path.exists(os.path.join(provider_dir, f)) for f in required):
        return None

    stops      = read_csv(spark, f"{provider_dir}/stops.txt")
    stop_times = read_csv(spark, f"{provider_dir}/stop_times.txt")
    trips      = read_csv(spark, f"{provider_dir}/trips.txt")
    routes     = read_csv(spark, f"{provider_dir}/routes.txt")

    routes = routes.withColumn("route_type", F.col("route_type").cast("int"))
    rail_routes = routes.filter((F.col("route_type") == 2) | ((F.col("route_type") >= 100) & (F.col("route_type") <= 117)))

    if rail_routes.selectExpr("1").limit(1).count() == 0:
        return None

    trip_sel = ["trip_id", "route_id"] + (["shape_id"] if "shape_id" in trips.columns else [])
    rail_trips = trips.select(*trip_sel).join(rail_routes.select("route_id", "route_type"), "route_id", "inner")

    rail_stop_times = (
        stop_times.join(rail_trips.select("trip_id", "route_type"), "trip_id", "inner")
        .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
    )

    w_asc  = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").asc())
    w_desc = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").desc())

    bounds = (
        rail_stop_times
        .withColumn("origin_stop_id", F.first("stop_id").over(w_asc))
        .withColumn("dest_stop_id",   F.first("stop_id").over(w_desc))
        .withColumn("route_type_val", F.first("route_type").over(w_asc))
        .withColumn("departure_time", F.first("departure_time").over(w_asc))
        .withColumn("arrival_time",   F.first("arrival_time").over(w_desc))
        .select("trip_id", "origin_stop_id", "dest_stop_id", "route_type_val", "departure_time", "arrival_time")
        .distinct()
    )

    bounds = bounds.withColumn("shape_distance_km", F.lit(None).cast(DoubleType()))

    origin_stops = stops.select(
        F.col("stop_id").alias("origin_stop_id"),
        F.col("stop_name").alias("origin"),
        F.col("stop_lat").cast(DoubleType()).alias("station_lat"),
        F.col("stop_lon").cast(DoubleType()).alias("station_long"),
    )
    dest_stops = stops.select(
        F.col("stop_id").alias("dest_stop_id"),
        F.col("stop_name").alias("destination"),
        F.col("stop_lat").cast(DoubleType()).alias("station_lat_dest"),
        F.col("stop_lon").cast(DoubleType()).alias("station_long_dest"),
    )

    df = bounds.join(origin_stops, "origin_stop_id").join(dest_stops, "dest_stop_id")
    df = df.withColumn("vehicule_type", F.when(F.col("route_type_val") == 102, F.lit("Train Nuit")).otherwise(F.lit("Train Jour")))

    return (
        df.select(
            "origin", "destination", "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            "departure_time", "arrival_time", "shape_distance_km"
        )
        .withColumn("source", F.lit("mobility_db"))
        .withColumn("provider", F.lit(provider_name))
    )

def read_all_mobility(spark, country_filter=None):
    EU = ['FR','DE','CH','BE','NL','AT','IT','ES','PT','PL','CZ','SK','HU','SI','HR','DK','SE','NO','FI','IE','GB','LU','RO','BG','GR','EE','LV','LT']
    countries = EU if country_filter is None else ([country_filter] if isinstance(country_filter, str) else country_filter)

    if not os.path.exists(RAW_MOBILITY_DIR):
        print(f"❌ Dossier introuvable : {RAW_MOBILITY_DIR}")
        return None

    dfs = []
    for provider_name in os.listdir(RAW_MOBILITY_DIR):
        if not any(provider_name.startswith(f"{c}_") for c in countries):
            continue
        p = os.path.join(RAW_MOBILITY_DIR, provider_name)
        if not os.path.isdir(p):
            continue
        df = read_mobility_provider(spark, p)
        if df is not None:
            dfs.append(df)

    if not dfs:
        return None

    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


# ===========================
# 5) BACK ON TRACK
# ===========================
def read_backontrack(spark):
    files = {
        "routes": f"{RAW_BACKONTRACK_DIR}/back_on_track_routes.csv",
        "trips":  f"{RAW_BACKONTRACK_DIR}/back_on_track_trips.csv",
        "stops":  f"{RAW_BACKONTRACK_DIR}/back_on_track_stops.csv",
    }
    if any(not os.path.exists(p) for p in files.values()):
        return None

    routes = read_csv(spark, files["routes"])
    trips  = read_csv(spark, files["trips"])
    stops  = read_csv(spark, files["stops"])

    origin_col = "trip_origin"   if "trip_origin" in trips.columns else None
    dest_col   = "trip_headsign" if "trip_headsign" in trips.columns else None
    if not origin_col or not dest_col:
        return None

    stops_coords = (
        stops.select(
            F.col("station_name").alias("stop_name_match"),
            F.col("station_lat").cast(DoubleType()).alias("lat"),
            F.col("station_long").cast(DoubleType()).alias("lon"),
        )
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    )

    df = trips.join(routes.select("route_id", "vehicule_type"), "route_id", "left")

    df = df.join(
        stops_coords.withColumnRenamed("lat", "station_lat").withColumnRenamed("lon", "station_long"),
        F.col(origin_col) == F.col("stop_name_match"),
        "left",
    ).drop("stop_name_match")

    df = df.join(
        stops_coords.withColumnRenamed("lat", "station_lat_dest").withColumnRenamed("lon", "station_long_dest"),
        F.col(dest_col) == F.col("stop_name_match"),
        "left",
    ).drop("stop_name_match")

    df = df.withColumn(
        "vehicule_type",
        F.when(F.col("vehicule_type") == F.lit("102"), F.lit("Train Nuit"))
         .when(F.col("vehicule_type") == F.lit("2"),   F.lit("Train Jour"))
         .otherwise(F.lit("Train Jour"))
    )

    return (
        df.select(
            F.col(origin_col).alias("origin"),
            F.col(dest_col).alias("destination"),
            "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            F.col("origin_departure_time").alias("departure_time"),
            F.col("destination_arrival_time").alias("arrival_time"),
        )
        .filter(
            F.col("origin").isNotNull() & F.col("destination").isNotNull() &
            (F.col("origin") != "#N/A") & (F.col("destination") != "#N/A") &
            (F.trim("origin") != "") & (F.trim("destination") != "")
        )
        .withColumn("shape_distance_km", F.lit(None).cast(DoubleType()))
        .withColumn("source", F.lit("back_on_track"))
        .withColumn("provider", F.lit("BackOnTrack"))
    )


# ===========================
# 6) AIRPORTS (OurAirports)
# ===========================
def read_airports(spark):
    if not os.path.exists(RAW_AIRPORTS_DIR):
        return None

    airport_files = glob.glob(f"{RAW_AIRPORTS_DIR}/airports_*.csv")
    if not airport_files:
        return None

    latest = sorted(airport_files)[-1]
    df = read_csv(spark, latest)

    mapping = {
        "latitude_deg": "aero_lat",
        "longitude_deg": "aero_long",
        "type": "category",
        "name": "airport_name",
        "iso_country": "country_code",
    }
    for old, new in mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    keep = [c for c in REQUIRED_COLUMNS_AIRPORTS if c in df.columns]
    if "ident" in df.columns and "ident" not in keep:
        keep.append("ident")
    if not keep:
        return None

    df = df.select(*keep)
    df = df.withColumn("aero_lat", F.col("aero_lat").cast(DoubleType())) \
           .withColumn("aero_long", F.col("aero_long").cast(DoubleType()))

    df = df.filter(
        F.col("aero_lat").isNotNull() & F.col("aero_long").isNotNull() &
        F.col("category").isin("large_airport", "medium_airport", "small_airport")
    )

    if "country_code" in df.columns:
        eu = ['FR','DE','CH','IT','ES','BE','NL','AT','GB','PT','PL','CZ','DK','SE','NO','FI']
        df = df.filter(F.col("country_code").isin(eu))

    return df


# ===========================
# 7) INTERMODALITÉ + AVION (RAM-friendly: GRID JOIN)
# ===========================
def _add_geo_bins(df, lat_col: str, lon_col: str, bin_deg: float, prefix: str):
    """
    Convertit un join non-équi (bbox) en join équi via des bins lat/lon.
    """
    return (
        df.withColumn(f"{prefix}_lat_bin", F.floor(F.col(lat_col) / F.lit(bin_deg)).cast(IntegerType()))
          .withColumn(f"{prefix}_lon_bin", F.floor(F.col(lon_col) / F.lit(bin_deg)).cast(IntegerType()))
    )

def build_intermodal_links(df_routes, df_airports, radius_km=INTERMODAL_RADIUS_KM):
    """
    Intermodalité Windows-friendly :
    - stations uniques
    - JOIN EQUI via bins (évite BroadcastNestedLoopJoin)
    - airports broadcast (petit dataset)
    - filtre bbox + distance
    """
    if df_airports is None:
        return None

    # 1) Stations uniques (origin + destination)
    stations = (
        df_routes.select(
            F.col("origin").alias("station_name"),
            F.col("station_lat").alias("station_lat"),
            F.col("station_long").alias("station_long"),
        )
        .unionByName(
            df_routes.select(
                F.col("destination").alias("station_name"),
                F.col("station_lat_dest").alias("station_lat"),
                F.col("station_long_dest").alias("station_long"),
            ),
            allowMissingColumns=True
        )
        .filter(
            F.col("station_name").isNotNull() &
            F.col("station_lat").isNotNull() &
            F.col("station_long").isNotNull()
        )
        .dropDuplicates(["station_name", "station_lat", "station_long"])
    )

    # 2) Airports (petit) + bins
    airports = (
        df_airports.select(
            "airport_name",
            F.col("aero_lat").alias("aero_lat"),
            F.col("aero_long").alias("aero_long"),
            "iata_code",
            "country_code"
        )
        .filter(F.col("aero_lat").isNotNull() & F.col("aero_long").isNotNull())
        .dropDuplicates(["iata_code", "aero_lat", "aero_long"])
    )

    stations_binned = _add_geo_bins(stations, "station_lat", "station_long", GRID_BIN_DEG, "st")
    airports_binned = _add_geo_bins(airports, "aero_lat", "aero_long", GRID_BIN_DEG, "ap")

    # 3) Expander airports sur les bins voisins (dx,dy) ∈ {-1,0,1}
    offsets = [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)]
    offsets_df = df_routes.sparkSession.createDataFrame(offsets, ["dx", "dy"])

    airports_expanded = (
        airports_binned.crossJoin(F.broadcast(offsets_df))
        .withColumn("ap_lat_bin2", (F.col("ap_lat_bin") + F.col("dx")).cast(IntegerType()))
        .withColumn("ap_lon_bin2", (F.col("ap_lon_bin") + F.col("dy")).cast(IntegerType()))
        .drop("dx", "dy")
    )

    # 4) JOIN EQUI sur bins (hash join) + filtre bbox exact
    joined = stations_binned.join(
        F.broadcast(airports_expanded),
        (F.col("st_lat_bin") == F.col("ap_lat_bin2")) & (F.col("st_lon_bin") == F.col("ap_lon_bin2")),
        "inner"
    )

    joined = joined.filter(
        (F.abs(F.col("station_lat") - F.col("aero_lat")) <= F.lit(BBOX_MARGIN_DEG)) &
        (F.abs(F.col("station_long") - F.col("aero_long")) <= F.lit(BBOX_MARGIN_DEG))
    )

    # 5) Distance + radius
    joined = joined.withColumn(
        "station_airport_km",
        haversine_distance(
            F.col("station_lat"), F.col("station_long"),
            F.col("aero_lat"), F.col("aero_long")
        )
    ).filter(
        F.col("station_airport_km").isNotNull() &
        (F.col("station_airport_km") <= F.lit(radius_km))
    )

    # 6) Best airport par station
    w = Window.partitionBy("station_name").orderBy(F.col("station_airport_km").asc())
    best = joined.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    return best.select(
        "station_name", "station_lat", "station_long",
        "airport_name", "aero_lat", "aero_long",
        "iata_code", "country_code",
        "station_airport_km",
        "st_lat_bin", "st_lon_bin"
    )

def generate_plane_routes(df_routes, df_intermodal, min_plane_km=MIN_PLANE_DISTANCE_KM):
    if df_intermodal is None:
        return None

    # pairs = shuffle -> ok (shuffle partitions faibles)
    pairs = df_routes.select("origin", "destination").dropDuplicates()

    inter_o = df_intermodal.select(
        F.col("station_name").alias("origin"),
        F.col("airport_name").alias("airport_origin"),
        F.col("aero_lat").alias("airport_origin_lat"),
        F.col("aero_long").alias("airport_origin_long"),
        F.col("iata_code").alias("iata_origin"),
    )
    inter_d = df_intermodal.select(
        F.col("station_name").alias("destination"),
        F.col("airport_name").alias("airport_dest"),
        F.col("aero_lat").alias("airport_dest_lat"),
        F.col("aero_long").alias("airport_dest_long"),
        F.col("iata_code").alias("iata_dest"),
    )

    dfp = pairs.join(inter_o, "origin", "inner").join(inter_d, "destination", "inner")

    dfp = dfp.withColumn(
        "distance_km",
        haversine_distance(
            F.col("airport_origin_lat"), F.col("airport_origin_long"),
            F.col("airport_dest_lat"), F.col("airport_dest_long")
        )
    ).filter(F.col("distance_km").isNotNull() & (F.col("distance_km") >= F.lit(min_plane_km)))

    dfp = dfp.withColumn("vehicule_type", F.lit("Avion")) \
             .withColumn("co2_kg", co2_kg(F.col("distance_km"), F.col("vehicule_type")))

    return dfp.select(
        F.col("airport_origin").alias("origin"),
        F.col("airport_dest").alias("destination"),
        "vehicule_type",
        F.col("airport_origin_lat").alias("station_lat"),
        F.col("airport_origin_long").alias("station_long"),
        F.col("airport_dest_lat").alias("station_lat_dest"),
        F.col("airport_dest_long").alias("station_long_dest"),
        "distance_km", "co2_kg",
        F.lit(None).cast("string").alias("departure_time"),
        F.lit(None).cast("string").alias("arrival_time"),
        F.lit("airports").alias("source"),
        F.lit("OurAirports").alias("provider"),
    )


# ===========================
# 8) PIPELINE ORCHESTRATION
# ===========================
def run_transform():
    print("=" * 60)
    print("🚀 TRANSFORMATION - Pipeline datamart (train + avion)")
    print("=" * 60)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_dir(OUTPUT_DIR)

    try:
        # 1) LOAD sources
        print("\n[1] Chargement des sources...")
        df_mob = read_all_mobility(spark)
        df_bot = read_backontrack(spark)
        df_air = read_airports(spark)

        routes_sources = [df for df in (df_mob, df_bot) if df is not None]
        if not routes_sources:
            print("❌ Aucune donnée trajets (Mobility/BackOnTrack vides).")
            return

        df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), routes_sources)

        # 2) CLEAN
        print("\n[2] Nettoyage + filtre grossier...")
        df = validate_routes(df)
        df = normalize_station_col(df, "origin")
        df = normalize_station_col(df, "destination")

        # 3) DISTANCE + CO2
        print("\n[3] Calcul distance + CO2...")
        has_coords = all(c in df.columns for c in ("station_lat", "station_long", "station_lat_dest", "station_long_dest"))
        if has_coords:
            df = df.withColumn(
                "distance_km",
                haversine_distance(
                    F.col("station_lat"), F.col("station_long"),
                    F.col("station_lat_dest"), F.col("station_long_dest")
                )
            )
        else:
            df = df.withColumn("distance_km", F.lit(None).cast(DoubleType()))

        df = df.withColumn("co2_kg", co2_kg(F.col("distance_km"), F.col("vehicule_type")))

        # 4) INTERMODAL
        print("\n[4] Intermodalité (gare -> aéroport proche)...")
        df_intermodal = None
        if df_air is not None and has_coords:
            df_intermodal = build_intermodal_links(df, df_air, radius_km=INTERMODAL_RADIUS_KM)

            if df_intermodal is not None and df_intermodal.selectExpr("1").limit(1).count() > 0:
                out_inter = os.path.join(OUTPUT_DIR, OUTPUT_INTERMODAL_FILE)
                export_single_csv(df_intermodal, out_inter, os.path.join(OUTPUT_DIR, "_tmp_intermodal"))
                print(f"   ✅ Intermodal exporté : {out_inter}")
            else:
                df_intermodal = None
                print("   ⚠️ Intermodal : aucun lien généré")
        else:
            print("   ⚠️ Intermodal : airports ou coords manquants")

        # 5) PLANES
        print("\n[5] Génération trajets avion...")
        df_planes = generate_plane_routes(df, df_intermodal) if df_intermodal is not None else None
        if df_planes is not None and df_planes.selectExpr("1").limit(1).count() > 0:
            df = df.unionByName(df_planes, allowMissingColumns=True)
            print("   ✅ Trajets avion ajoutés")
        else:
            print("   ⚠️ Aucun trajet avion généré")

        # 6) DEDUP + EXPORT
        print("\n[6] Dédup + exports...")
        df = df.dropDuplicates(["origin", "destination", "vehicule_type", "source"])

        final_cols = [
            "origin", "destination", "vehicule_type",
            "station_lat", "station_long", "station_lat_dest", "station_long_dest",
            "distance_km", "co2_kg", "departure_time", "arrival_time",
            "source", "provider"
        ]
        df = df.select(*[c for c in final_cols if c in df.columns])

        out_routes = os.path.join(OUTPUT_DIR, OUTPUT_FINAL_FILE)
        export_single_csv(df, out_routes, os.path.join(OUTPUT_DIR, "_tmp_routes"))
        print(f"   ✅ Routes exportées : {out_routes}")

        if df_air is not None:
            out_air = os.path.join(OUTPUT_DIR, OUTPUT_AIRPORTS_FILE)
            export_single_csv(df_air, out_air, os.path.join(OUTPUT_DIR, "_tmp_airports"))
            print(f"   ✅ Aéroports exportés : {out_air}")

        print("\n🎉 Transformation terminée.")

    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    run_transform()