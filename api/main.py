from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Optional
import os
import sys
import time

# Ajout du dossier parent pour importer scripts/predict.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.predict import predict_corridor, VEHICULE_TYPES

# 1. Chargement de la config
load_dotenv()

app = FastAPI(
    title="ObRail Europe — ML API",
    description=(
        "API de prédiction IA pour la substitution avion → train.\n\n"
        "**Modèle 1** `/predict/substitution` — Classification : ce corridor est-il substituable ?\n\n"
        "**Modèle 2** `/predict/co2_saved` — Régression : combien de CO2 économisé ?\n\n"
        f"Types de train acceptés : `{', '.join(VEHICULE_TYPES)}`"
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Préchargement des modèles au démarrage pour éviter la latence au 1er appel
@app.on_event("startup")
def preload_models():
    try:
        predict_corridor({
            "distance_km": 1, "vehicule_type": "InterCity",
            "co2_avion_kg": 0, "co2_train_kg": 0,
        })
        print("✅ Modèles ML chargés en mémoire")
    except Exception as e:
        print(f"⚠️  Préchargement modèles : {e}")

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
    origine_ville: str
    destination_ville: str
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

# ── Modèles Pydantic ML ───────────────────────────────────────────────────────

def _estimate_co2_avion(distance_km: float) -> float:
    """Estimation EcoPassenger simplifiée : 40 + distance × 0.123 kg CO2/passager."""
    return round(40.0 + distance_km * 0.123, 1)


class CorridorInput(BaseModel):
    origin:       str   = Field(..., example="Paris",                description="Nom de la gare de départ")
    destination:  str   = Field(..., example="Lyon",                 description="Nom de la gare d'arrivée")
    distance_km:  float = Field(..., example=450.0, gt=0,            description="Distance en km")
    vehicule_type: str  = Field(..., example="Train Longue Distance", description=f"Type de train : {VEHICULE_TYPES}")
    flight_exists: bool = Field(True, example=True,                  description="True si un vol direct existe sur ce corridor. Si True et co2_avion_kg absent, la valeur est estimée depuis la distance (EcoPassenger).")
    co2_train_kg:             Optional[float] = Field(None, example=3.5,      description="CO2 train kg (EcoPassenger). Optionnel.")
    co2_avion_kg:             Optional[float] = Field(None, example=95.0,     description="CO2 avion kg (EcoPassenger). Si absent et flight_exists=True, estimé automatiquement.")
    origin_station_traffic:   Optional[float] = Field(None, example=18000000, description="Fréquentation annuelle gare départ")
    dest_station_traffic:     Optional[float] = Field(None, example=14000000, description="Fréquentation annuelle gare arrivée")
    origin_city_population:   Optional[float] = Field(None, example=2161000,  description="Population ville départ")
    dest_city_population:     Optional[float] = Field(None, example=522000,   description="Population ville arrivée")
    ratio_origin:             Optional[float] = Field(None, example=8.33,     description="trafic/population gare départ")
    ratio_dest:               Optional[float] = Field(None, example=26.82,    description="trafic/population gare arrivée")
    trip_count_corridor:      Optional[float] = Field(None, example=14,       description="Trajets hebdomadaires sur ce corridor")
    trip_count_origin:        Optional[float] = Field(None, example=180,      description="Trajets hebdomadaires total gare départ")
    service_share:            Optional[float] = Field(None, example=0.078,    description="trip_count_corridor / trip_count_origin")

class SubstitutionResponse(BaseModel):
    origin:                 str
    destination:            str
    distance_km:            float
    vehicule_type:          str
    is_substitutable:       int   = Field(..., description="1 = substituable, 0 = non substituable")
    proba_substitutable:    float = Field(..., description="Probabilité de substitution (0 à 1)")
    co2_avion_kg_used:      float = Field(..., description="CO2 avion utilisé par le modèle (fourni ou estimé EcoPassenger)")
    co2_avion_estimated:    bool  = Field(..., description="True si co2_avion_kg a été estimé automatiquement depuis la distance")
    latency_ms:             float = Field(..., description="Temps de réponse du modèle en ms")

class CO2SavedResponse(BaseModel):
    origin:                 str
    destination:            str
    distance_km:            float
    vehicule_type:          str
    is_substitutable:       int
    proba_substitutable:    float
    co2_saved_kg:           Optional[float] = Field(None, description="CO2 économisé en kg/passager (None si non substituable)")
    co2_avion_kg_used:      float = Field(..., description="CO2 avion utilisé par le modèle (fourni ou estimé EcoPassenger)")
    co2_avion_estimated:    bool  = Field(..., description="True si co2_avion_kg a été estimé automatiquement depuis la distance")
    latency_ms:             float


# ── Health check ─────────────────────────────────────────────────────────────

# 4. Route de Test (Pour vérifier que l'API est en vie)
@app.get("/", tags=["Health"])
def read_root():
    return {"status": "online", "message": "ObRail Europe ML API — voir /docs"}

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
            COALESCE(r.dep_city, r.dep_name) AS origine_ville,
            COALESCE(r.arr_city, r.arr_name) AS destination_ville,
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
                origine_ville=row[2],
                destination_ville=row[3],
                distance_km=row[4],
                is_long_distance=row[5],
                vehicule_type=row[6],
                facteur_co2=row[7],
                co2_kg=row[8]
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


# ── Endpoints ML ─────────────────────────────────────────────────────────────

def _resolve_co2_avion(corridor: CorridorInput) -> tuple[float, bool]:
    """
    Retourne (co2_avion_kg, was_estimated).
    - Si flight_exists=False → 0.0 (pas de vol = non substituable)
    - Si co2_avion_kg fourni   → valeur fournie
    - Sinon                    → estimation EcoPassenger depuis la distance
    """
    if not corridor.flight_exists:
        return 0.0, False
    if corridor.co2_avion_kg is not None:
        return corridor.co2_avion_kg, False
    return _estimate_co2_avion(corridor.distance_km), True


@app.post(
    "/predict/substitution",
    response_model=SubstitutionResponse,
    tags=["ML Prédiction"],
    summary="Modèle 1 — Ce corridor est-il substituable avion → train ?",
)
def predict_substitution(corridor: CorridorInput):
    """
    Prédit si un corridor ferroviaire peut remplacer un vol aérien.

    **Règle métier (loi française 2023) :** distance ≤ 600 km ET vol existant.
    Le modèle Random Forest généralise cette règle à toute l'Europe.

    - `is_substitutable = 1` → le train peut remplacer l'avion
    - `proba_substitutable` → confiance du modèle (0 à 1)
    - `co2_avion_kg_used` → valeur CO2 avion utilisée (fournie ou estimée)
    - `co2_avion_estimated = true` → la valeur a été estimée depuis la distance

    **`flight_exists` :** mettre à `false` si aucun vol direct n'existe sur ce corridor.
    """
    co2_avion, estimated = _resolve_co2_avion(corridor)
    t0 = time.perf_counter()
    try:
        payload = corridor.model_dump()
        payload["co2_avion_kg"] = co2_avion
        result = predict_corridor(payload)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur modèle : {e}")

    latency = round((time.perf_counter() - t0) * 1000, 2)

    return SubstitutionResponse(
        origin=corridor.origin,
        destination=corridor.destination,
        distance_km=corridor.distance_km,
        vehicule_type=corridor.vehicule_type,
        is_substitutable=result["is_substitutable"],
        proba_substitutable=result["proba_substitutable"],
        co2_avion_kg_used=co2_avion,
        co2_avion_estimated=estimated,
        latency_ms=latency,
    )


@app.post(
    "/predict/co2_saved",
    response_model=CO2SavedResponse,
    tags=["ML Prédiction"],
    summary="Modèle 2 — Combien de CO2 économisé si le passager prend le train ?",
)
def predict_co2_saved(corridor: CorridorInput):
    """
    Prédit le gain CO2 en kg/passager si le passager substitue l'avion par le train.

    Enchaîne les deux modèles :
    1. **Modèle 1** vérifie la substituabilité (en utilisant `co2_avion_kg`)
    2. **Modèle 2** (XGBoost, R²=0.907) prédit `co2_saved_kg` — **n'utilise pas** co2_avion_kg

    Si le corridor n'est pas substituable, `co2_saved_kg` est `null`.

    **Interprétation :** un passager Paris→Lyon économise ~106 kg de CO2
    par rapport à l'avion (équivalent à ~700 km en voiture).

    **Garde-fou :** 90.3% des prédictions sont à ±10 kg des valeurs EcoPassenger calculées.
    """
    co2_avion, estimated = _resolve_co2_avion(corridor)
    t0 = time.perf_counter()
    try:
        payload = corridor.model_dump()
        payload["co2_avion_kg"] = co2_avion
        result = predict_corridor(payload)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur modèle : {e}")

    latency = round((time.perf_counter() - t0) * 1000, 2)

    return CO2SavedResponse(
        origin=corridor.origin,
        destination=corridor.destination,
        distance_km=corridor.distance_km,
        vehicule_type=corridor.vehicule_type,
        is_substitutable=result["is_substitutable"],
        proba_substitutable=result["proba_substitutable"],
        co2_saved_kg=result["co2_saved_kg"],
        co2_avion_kg_used=co2_avion,
        co2_avion_estimated=estimated,
        latency_ms=latency,
    )