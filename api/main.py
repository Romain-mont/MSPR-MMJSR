from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Optional, List
import os
import sys
import time

from prometheus_fastapi_instrumentator import Instrumentator

# Ajout du dossier parent pour importer scripts/predict.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.predict import predict_corridor, VEHICULE_TYPES

# 1. Chargement de la config
load_dotenv()

app = FastAPI(
    title="ObRail Europe — API",
    description=(
        "API complète ObRail Europe : données ferroviaires + prédiction IA CO2.\n\n"
        "**Données** : `/trajets`, `/trajets/{id}`, `/stats/volumes`\n\n"
        "**ML Modèle 1** `/predict/substitution` — Ce corridor est-il substituable avion→train ?\n\n"
        "**ML Modèle 2** `/predict/co2_saved` — Combien de CO2 économisé ?\n\n"
        f"Types de train : `{', '.join(VEHICULE_TYPES)}`"
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Exposition des métriques Prometheus sur /metrics
Instrumentator().instrument(app).expose(app)

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

# ── Health & racine ───────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def read_root():
    return {"status": "online", "message": "ObRail Europe API — voir /docs"}


@app.get("/health", tags=["Health"], summary="État de santé du service")
def health():
    """Vérifie que l'API et la base de données sont accessibles."""
    db_ok = False
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass
    status = "ok" if db_ok else "degraded"
    if not db_ok:
        raise HTTPException(status_code=503, detail={"status": status, "db": db_ok})
    return {"status": status, "db": db_ok, "version": "2.0.0"}


# ── Trajets ferroviaires ──────────────────────────────────────────────────────

class TrajetDetail(BaseModel):
    id:           int
    origine:      str
    destination:  str
    distance_km:  Optional[float]
    vehicule_type: str
    co2_train_kg: Optional[float]
    co2_avion_kg: Optional[float]
    co2_saved_kg: Optional[float]
    is_substitutable: Optional[int]


@app.get("/trajets", response_model=List[TrajetDetail], tags=["Trajets"],
         summary="Liste des trajets ferroviaires")
def get_trajets(
    limit: int = 100,
    offset: int = 0,
    origine: Optional[str] = None,
    destination: Optional[str] = None,
    substituable: Optional[bool] = None,
):
    """
    Retourne les trajets ferroviaires avec filtres optionnels.
    - `origine` / `destination` : filtre partiel (ILIKE)
    - `substituable` : `true` = uniquement les corridors substituables avion→train
    """
    query = """
        SELECT
            f.fact_id,
            r.dep_name    AS origine,
            r.arr_name    AS destination,
            r.distance_km,
            v.label       AS vehicule_type,
            f.co2_train_kg,
            f.co2_avion_kg,
            f.co2_saved_kg,
            f.is_substitutable
        FROM fact_route_analysis f
        JOIN dim_route r        ON f.route_id        = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        WHERE 1=1
    """
    params: dict = {}
    if origine:
        query += " AND r.dep_name ILIKE :origine"
        params["origine"] = f"%{origine}%"
    if destination:
        query += " AND r.arr_name ILIKE :destination"
        params["destination"] = f"%{destination}%"
    if substituable is not None:
        query += " AND f.is_substitutable = :sub"
        params["sub"] = 1 if substituable else 0
    query += " ORDER BY f.fact_id LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()
        return [TrajetDetail(
            id=r[0], origine=r[1], destination=r[2], distance_km=r[3],
            vehicule_type=r[4], co2_train_kg=r[5], co2_avion_kg=r[6],
            co2_saved_kg=r[7], is_substitutable=r[8]
        ) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/trajets/{trajet_id}", response_model=TrajetDetail, tags=["Trajets"],
         summary="Détail d'un trajet par ID")
def get_trajet(trajet_id: int):
    """Retourne les informations complètes d'un trajet ferroviaire."""
    query = """
        SELECT f.fact_id, r.dep_name, r.arr_name, r.distance_km,
               v.label, f.co2_train_kg, f.co2_avion_kg, f.co2_saved_kg, f.is_substitutable
        FROM fact_route_analysis f
        JOIN dim_route r        ON f.route_id        = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        WHERE f.fact_id = :id
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(query), {"id": trajet_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Trajet {trajet_id} introuvable")
        return TrajetDetail(
            id=row[0], origine=row[1], destination=row[2], distance_km=row[3],
            vehicule_type=row[4], co2_train_kg=row[5], co2_avion_kg=row[6],
            co2_saved_kg=row[7], is_substitutable=row[8]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/volumes", tags=["Statistiques"],
         summary="Volumes et répartition jour/nuit par opérateur")
def stats_volumes():
    """
    Agrégations globales sur les trajets :
    - Répartition trains de jour vs nuit
    - Volume par type de véhicule
    - Statistiques CO2
    """
    query_repartition = """
        SELECT
            CASE WHEN v.label ILIKE '%nuit%' OR v.label ILIKE '%night%'
                 THEN 'Nuit' ELSE 'Jour' END AS type_service,
            COUNT(*)                          AS nb_trajets,
            ROUND(AVG(r.distance_km)::numeric, 1) AS distance_moy_km,
            ROUND(AVG(f.co2_saved_kg)::numeric, 1) AS co2_saved_moy_kg
        FROM fact_route_analysis f
        JOIN dim_route r        ON f.route_id        = r.route_id
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        GROUP BY type_service
        ORDER BY nb_trajets DESC
    """
    query_vehicule = """
        SELECT v.label, COUNT(*) AS nb_trajets,
               ROUND(AVG(f.co2_saved_kg)::numeric, 1) AS co2_saved_moy_kg,
               SUM(CASE WHEN f.is_substitutable = 1 THEN 1 ELSE 0 END) AS nb_substituables
        FROM fact_route_analysis f
        JOIN dim_vehicle_type v ON f.vehicle_type_id = v.vehicle_type_id
        GROUP BY v.label
        ORDER BY nb_trajets DESC
    """
    query_global = """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN is_substitutable = 1 THEN 1 ELSE 0 END) AS substituables,
               ROUND(AVG(co2_saved_kg)::numeric, 1) AS co2_saved_moy_kg
        FROM fact_route_analysis
        WHERE co2_saved_kg IS NOT NULL
    """
    try:
        with engine.connect() as conn:
            repartition = [dict(r._mapping) for r in conn.execute(text(query_repartition)).fetchall()]
            par_vehicule = [dict(r._mapping) for r in conn.execute(text(query_vehicule)).fetchall()]
            global_row   = conn.execute(text(query_global)).fetchone()
        return {
            "global": {
                "total_trajets":   global_row[0],
                "substituables":   global_row[1],
                "co2_saved_moy_kg": global_row[2],
            },
            "repartition_jour_nuit": repartition,
            "par_vehicule":          par_vehicule,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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