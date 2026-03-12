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
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")  
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

class CompareResponse(BaseModel):
    depart: str
    arrivee: str
    trains_jour: dict
    trains_nuit: dict
    gain_ecologique_pct: float
    recommandation: str

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
       
        raise
    except Exception as e:
        print(f"Erreur SQL : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

# 7. Endpoint de Comparaison Écologique (Train Jour vs Train Nuit)
@app.get("/compare", response_model=CompareResponse)
def compare_day_night_trains(depart: str, arrivee: str):
    """
    Compare l'impact écologique des trains de jour vs trains de nuit.
    Retourne les statistiques (moyenne CO2, nombre de trajets) et le gain écologique.
    
    Exemple: /compare?depart=Lyon&arrivee=Paris
    """
    
    # Requête pour trains de JOUR (exclut les trains avec "nuit" dans le label)
    query_day = text("""
        SELECT 
            AVG(f.co2_kg_passenger) as avg_co2,
            COUNT(*) as count_trips,
            MIN(f.co2_kg_passenger) as min_co2,
            MAX(f.co2_kg_passenger) as max_co2
        FROM fact_em f
        JOIN dim_route r ON f.route_id = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        WHERE r.dep_name ILIKE '%' || :dep || '%'
          AND r.arr_name ILIKE '%' || :arr || '%'
          AND v.label NOT ILIKE '%nuit%'
    """)
    
    # Requête pour trains de NUIT
    query_night = text("""
        SELECT 
            AVG(f.co2_kg_passenger) as avg_co2,
            COUNT(*) as count_trips,
            MIN(f.co2_kg_passenger) as min_co2,
            MAX(f.co2_kg_passenger) as max_co2
        FROM fact_em f
        JOIN dim_route r ON f.route_id = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        WHERE r.dep_name ILIKE '%' || :dep || '%'
          AND r.arr_name ILIKE '%' || :arr || '%'
          AND v.label ILIKE '%nuit%'
    """)
    
    params = {"dep": depart, "arr": arrivee}
    
    try:
        with engine.connect() as conn:
            day_result = conn.execute(query_day, params).fetchone()
            night_result = conn.execute(query_night, params).fetchone()
        
        # Vérifier qu'on a au moins des trains de jour
        if not day_result or day_result[0] is None:
            raise HTTPException(
                status_code=404, 
                detail=f"Aucun train de jour trouvé entre {depart} et {arrivee}"
            )
        
        day_stats = {
            "moyenne_co2_kg": round(float(day_result[0]), 2),
            "nombre_trajets": int(day_result[1]),
            "min_co2_kg": round(float(day_result[2]), 2),
            "max_co2_kg": round(float(day_result[3]), 2)
        }
        
        # Si pas de trains de nuit, on retourne quand même la comparaison
        if not night_result or night_result[0] is None:
            return CompareResponse(
                depart=depart,
                arrivee=arrivee,
                trains_jour=day_stats,
                trains_nuit={
                    "moyenne_co2_kg": 0,
                    "nombre_trajets": 0,
                    "min_co2_kg": 0,
                    "max_co2_kg": 0
                },
                gain_ecologique_pct=0.0,
                recommandation=f"Aucun train de nuit disponible sur {depart} → {arrivee}. Privilégiez les trains de jour existants."
            )
        
        night_stats = {
            "moyenne_co2_kg": round(float(night_result[0]), 2),
            "nombre_trajets": int(night_result[1]),
            "min_co2_kg": round(float(night_result[2]), 2),
            "max_co2_kg": round(float(night_result[3]), 2)
        }
        
        # Calcul du gain écologique (en %)
        gain_pct = round(
            ((day_stats["moyenne_co2_kg"] - night_stats["moyenne_co2_kg"]) 
             / day_stats["moyenne_co2_kg"]) * 100, 
            1
        )
        
        # Génération de la recommandation
        if gain_pct > 15:
            recommandation = f"🌙 Train de nuit RECOMMANDÉ : {gain_pct}% moins de CO2 ({day_stats['moyenne_co2_kg']}kg → {night_stats['moyenne_co2_kg']}kg)"
        elif gain_pct > 0:
            recommandation = f"✅ Léger avantage au train de nuit : {gain_pct}% de réduction CO2"
        elif gain_pct < -15:
            recommandation = f"☀️ Train de jour RECOMMANDÉ : {abs(gain_pct)}% moins de CO2"
        else:
            recommandation = f"≈ Impact similaire ({abs(gain_pct)}% de différence)"
        
        return CompareResponse(
            depart=depart,
            arrivee=arrivee,
            trains_jour=day_stats,
            trains_nuit=night_stats,
            gain_ecologique_pct=gain_pct,
            recommandation=recommandation
        )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Erreur SQL dans /compare : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")