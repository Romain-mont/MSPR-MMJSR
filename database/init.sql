-- 1. DIM_ROUTE
CREATE TABLE IF NOT EXISTS dim_route (
    route_id          SERIAL PRIMARY KEY,
    dep_name          VARCHAR(255) NOT NULL,
    arr_name          VARCHAR(255) NOT NULL,
    distance_km       NUMERIC(10,3),
    is_long_distance  BOOLEAN,
    UNIQUE(dep_name, arr_name)
);

-- 2. DIM_VEHICLE_TYPE
CREATE TABLE IF NOT EXISTS dim_vehicle_type (
    vehicle_type_id  SERIAL PRIMARY KEY,
    label            VARCHAR(100) NOT NULL,
    service_type     VARCHAR(100) NOT NULL,
    co2_vt           NUMERIC(10,4),
    UNIQUE(label, service_type)
);

-- 3. DIM_STATION_FREQUENTATION
CREATE TABLE IF NOT EXISTS dim_station_frequentation (
    station_id              SERIAL PRIMARY KEY,
    station_name            VARCHAR(255) NOT NULL,
    city                    VARCHAR(255),
    lat                     NUMERIC(10,6),
    lon                     NUMERIC(10,6),
    country_code            CHAR(2),
    annual_station_traffic  BIGINT,
    city_population         BIGINT,
    UNIQUE(station_name)
);

-- 4. FACT_ROUTE_ANALYSIS
CREATE TABLE IF NOT EXISTS fact_route_analysis (
    fact_id              SERIAL PRIMARY KEY,

    route_id             INTEGER NOT NULL,
    vehicle_type_id      INTEGER,
    origin_station_id    INTEGER,
    dest_station_id      INTEGER,

    co2_train_kg         NUMERIC(10,4),
    co2_avion_kg         NUMERIC(10,4),
    co2_saved_kg         NUMERIC(10,4),
    traffic_share_pct    NUMERIC(5,2),
    is_substitutable     SMALLINT,

    FOREIGN KEY (route_id)          REFERENCES dim_route(route_id),
    FOREIGN KEY (vehicle_type_id)   REFERENCES dim_vehicle_type(vehicle_type_id),
    FOREIGN KEY (origin_station_id) REFERENCES dim_station_frequentation(station_id),
    FOREIGN KEY (dest_station_id)   REFERENCES dim_station_frequentation(station_id)
);
