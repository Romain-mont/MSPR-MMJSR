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
DB_HOST = "127.0.0.1"  # Localhost pour l'exécution locale
DB_PORT = os.getenv("DB_PORT", "5433")
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

# 4. Route de Test (Pour vérifier que l'API est en vie)
@app.get("/")
def read_root():
    return {"status": "online", "message": "Bienvenue sur l'API Euro Rail ! 🚄"}

# 5. La Route Principale : Recherche d'itinéraire
@app.get("/search", response_model=list[TrajetResponse])
def search_route(depart: str, arrivee: str):
    """
    Cherche un trajet et renvoie les émissions de CO2.
    Exemple: /search?depart=Paris&arrivee=Lyon
    """
    
    # Requete SQL optimisée avec JOIN pour tout récupérer d'un coup
    # On utilise text() pour la sécurité (évite les injections SQL)
    query = text("""
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
    """)
    # Note : ILIKE avec % permet de chercher "contient" (ex: Paris matchera "Gare de Paris...")

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"dep": depart, "arr": arrivee}).fetchall()

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

    except Exception as e:
        print(f"Erreur SQL : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")