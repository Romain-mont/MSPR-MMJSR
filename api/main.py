from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# 1. Chargement de la config
load_dotenv()

app = FastAPI(
    title="Euro Rail CO2 API",
    description="API pour comparer l'impact carbone des trains en Europe",
    version="1.0.0"
)

# 2. Connexion Base de Données (Mêmes variables que l'ingest)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")  # "db" en Docker, "127.0.0.1" en local
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# 3. Modèle de réponse (Ce que l'API renvoie au client)
class TrajetResponse(BaseModel):
    depart: str
    arrivee: str
    distance_km: float
    vehicule_type: str
    co2_kg: float

class DataResponse(BaseModel):
    origine: str
    destination: str
    distance_km: float
    is_long_distance: bool
    vehicule_type: str
    facteur_co2: float
    co2_kg: float

# 4. Route de Test (Pour vérifier que l'API est en vie)
@app.get("/")
def read_root():
    return {"status": "online", "message": "Bienvenue sur l'API Euro Rail !"}

# 5. Endpoint pour récupérer toutes les données (pour dashboards)
@app.get("/data", response_model=list[DataResponse])
def get_all_data(limit: int = None):
    """
    Récupère toutes les données du datamart pour analyses.
    Utilisé par les dashboards et outils de visualisation.
    Paramètre optionnel: limit (nombre max de résultats)
    """
    query_str = """
        SELECT 
            r.dep_name AS origine,
            r.arr_name AS destination,
            r.distance_km,
            r.is_long_distance,
            v.label AS vehicule_type,
            v.co2_vt AS facteur_co2,
            f.co2_kg_passenger AS co2_kg
        FROM fact_em f
        JOIN dim_route r ON f.route_id = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
    """
    
    if limit:
        query_str += f" LIMIT {int(limit)}"
    
    query = text(query_str)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
        
        if not result:
            return []
        
        response_list = []
        for row in result:
            response_list.append(DataResponse(
                origine=row[0],
                destination=row[1],
                distance_km=row[2],
                is_long_distance=row[3],
                vehicule_type=row[4],
                facteur_co2=row[5],
                co2_kg=row[6]
            ))
        
        return response_list
    
    except HTTPException:
        # Relancer les HTTPException sans les transformer
        raise
    except Exception as e:
        print(f"Erreur SQL : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

# 6. La Route Principale : Recherche d'itinéraire
@app.get("/search", response_model=list[TrajetResponse])
def search_route(depart: str, arrivee: str, vehicle_type: str = None):
    """
    Cherche un trajet et renvoie les émissions de CO2.
    """
    
    # Requete SQL optimisée avec JOIN pour tout récupérer d'un coup
    # On utilise text() pour la sécurité (évite les injections SQL)
    base_query = """
        SELECT 
            r.dep_name, 
            r.arr_name, 
            r.distance_km, 
            v.label, 
            f.co2_kg_passenger
        FROM fact_em f
        JOIN dim_route r ON f.route_id = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        WHERE r.dep_name ILIKE '%' || :dep || '%' 
          AND r.arr_name ILIKE '%' || :arr || '%'
    """
    
    # Ajout du filtre optionnel par type de véhicule
    params = {"dep": depart, "arr": arrivee}
    if vehicle_type:
        base_query += " AND v.label ILIKE '%' || :vehicle_type || '%'"
        params["vehicle_type"] = vehicle_type
    
    query = text(base_query)
    # Note : ILIKE avec % permet de chercher "contient" (ex: Paris matchera "Gare de Paris...")

    try:
        with engine.connect() as conn:
            result = conn.execute(query, params).fetchall()

        if not result:
            raise HTTPException(status_code=404, detail=f"Aucun trajet trouvé entre {depart} et {arrivee}")

        # On transforme le résultat SQL en liste d'objets propres pour l'API
        response_list = []
        for row in result:
            response_list.append(TrajetResponse(
                depart=row[0],
                arrivee=row[1],
                distance_km=row[2],
                vehicule_type=row[3],
                co2_kg=row[4]
            ))
        
        return response_list

    except HTTPException:
        # Relancer les HTTPException (404, etc.) sans les transformer
        raise
    except Exception as e:
        print(f"Erreur SQL : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")