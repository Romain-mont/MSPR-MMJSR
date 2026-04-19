-- 1. Table DIM_ROUTE (La Géographie)
CREATE TABLE IF NOT EXISTS dim_route (
   route_id SERIAL PRIMARY KEY,
   dep_name VARCHAR(255) NOT NULL,
   arr_name VARCHAR(255) NOT NULL,
   dep_city VARCHAR(255),
   arr_city VARCHAR(255),
   distance_km NUMERIC(10,3) NOT NULL,
   is_long_distance BOOLEAN NOT NULL,
   UNIQUE(dep_name, arr_name)
);

-- 2. Table DIM_VEHICLE_TYPE (Le Matériel)
CREATE TABLE IF NOT EXISTS dim_vehicle_type (
   vehicle_type_id SERIAL PRIMARY KEY,
   label VARCHAR(50) NOT NULL,      
   co2_vt NUMERIC(10,3) NOT NULL,    
   service_type VARCHAR(50) NOT NULL 
);

-- 3. Table FACT_EM (Les Faits / Mesures)
CREATE TABLE IF NOT EXISTS fact_em (
   fact_id SERIAL PRIMARY KEY,
   
   route_id INTEGER NOT NULL,
   vehicle_type_id INTEGER NOT NULL,
   
   co2_kg_passenger NUMERIC(10,3) NOT NULL,
   
   FOREIGN KEY(route_id) REFERENCES dim_route(route_id),
   FOREIGN KEY(vehicle_type_id) REFERENCES dim_vehicle_type(vehicle_type_id)
);