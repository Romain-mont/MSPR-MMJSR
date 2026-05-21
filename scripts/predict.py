"""
predict.py — Prédiction standalone ObRail Europe
Charge les modèles entraînés et prédit pour un corridor donné.

Modèle 1 (classification) : is_substitutable (le train peut-il remplacer l'avion ?)
Modèle 2 (régression)     : co2_saved_kg (combien de CO2 économisé ?)

Usage CLI :
    python predict.py --origin "Paris" --destination "Lyon" --distance 450 \
                      --vehicule_type "Train Longue Distance" --co2_train 3.5 --co2_avion 95.0

Import module :
    from scripts.predict import predict_corridor
    result = predict_corridor({"distance_km": 450, "vehicule_type": "Train Longue Distance", ...})
"""

import os
import sys
import argparse
import warnings
import numpy as np
import pandas as pd
import joblib

warnings.filterwarnings("ignore")

# ── Chemins ──────────────────────────────────────────────────────────────────
MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")

FEATURES_M1 = [
    "distance_km", "co2_train_kg", "co2_avion_kg", "vehicule_type",
    "origin_station_traffic", "origin_city_population",
    "dest_station_traffic", "dest_city_population",
    "ratio_origin", "ratio_dest",
    "trip_count_corridor", "trip_count_origin", "service_share",
]
FEATURES_M2 = [f for f in FEATURES_M1 if f not in ("co2_avion_kg", "co2_train_kg")]

VEHICULE_TYPES = ["EuroNight", "InterCity", "Nightjet",
                  "Train Longue Distance", "Train Longue Distance Nuit"]


def _load_models():
    model1  = joblib.load(os.path.join(MODELS_DIR, "model1_classification.joblib"))
    model2  = joblib.load(os.path.join(MODELS_DIR, "model2_regression.joblib"))
    scaler  = joblib.load(os.path.join(MODELS_DIR, "scaler.joblib"))
    le      = joblib.load(os.path.join(MODELS_DIR, "label_encoder_vehicule.joblib"))
    return model1, model2, scaler, le


def predict_corridor(corridor: dict) -> dict:
    """
    Prédit is_substitutable et co2_saved_kg pour un corridor.

    Paramètres requis :
        distance_km     (float) — distance du corridor en km
        vehicule_type   (str)   — type de train parmi VEHICULE_TYPES

    Paramètres optionnels (NULL → 0) :
        co2_train_kg, co2_avion_kg              — valeurs EcoPassenger si connues
        origin_station_traffic, dest_station_traffic  — fréquentation annuelle (voyageurs)
        origin_city_population, dest_city_population  — population des villes
        ratio_origin, ratio_dest                — trafic/population
        trip_count_corridor, trip_count_origin  — trajets hebdomadaires (GTFS)
        service_share                           — trip_count_corridor / trip_count_origin

    Retourne :
        dict avec is_substitutable (0/1), proba_substitutable (float),
        co2_saved_kg (float|None), vehicule_type_encoded (int)
    """
    model1, model2, scaler, le = _load_models()

    # ── Validation vehicule_type ──────────────────────────────────────────────
    vtype = corridor.get("vehicule_type", "Train Longue Distance")
    if vtype not in le.classes_:
        raise ValueError(
            f"vehicule_type '{vtype}' invalide. Valeurs acceptées : {list(le.classes_)}"
        )
    vtype_enc = int(le.transform([vtype])[0])

    # ── Construction du vecteur de features (ordre identique à l'entraînement) ─
    def _f(key):
        v = corridor.get(key)
        return float(v) if v is not None else 0.0

    row = {
        "distance_km":              float(corridor.get("distance_km", 0)),
        "co2_train_kg":             _f("co2_train_kg"),
        "co2_avion_kg":             _f("co2_avion_kg"),
        "vehicule_type":            float(vtype_enc),
        "origin_station_traffic":   _f("origin_station_traffic"),
        "origin_city_population":   _f("origin_city_population"),
        "dest_station_traffic":     _f("dest_station_traffic"),
        "dest_city_population":     _f("dest_city_population"),
        "ratio_origin":             _f("ratio_origin"),
        "ratio_dest":               _f("ratio_dest"),
        "trip_count_corridor":      _f("trip_count_corridor"),
        "trip_count_origin":        _f("trip_count_origin"),
        "service_share":            _f("service_share"),
    }

    df_input = pd.DataFrame([row])[FEATURES_M1]

    # ── Normalisation (les modèles ont été entraînés sur données scalées) ──────
    X_scaled = scaler.transform(df_input)
    X_m1 = X_scaled                                          # 13 features
    X_m2 = X_scaled[:, [FEATURES_M1.index(f) for f in FEATURES_M2]]  # 11 features

    # ── Modèle 1 — Classification ─────────────────────────────────────────────
    is_sub = int(model1.predict(X_m1)[0])
    proba  = float(model1.predict_proba(X_m1)[0][1])

    # ── Modèle 2 — Régression (uniquement si corridors substituables) ──────────
    co2_saved = None
    if is_sub == 1:
        co2_saved = float(model2.predict(X_m2)[0])
        co2_saved = max(0.0, round(co2_saved, 1))

    return {
        "is_substitutable":     is_sub,
        "proba_substitutable":  round(proba, 4),
        "co2_saved_kg":         co2_saved,
        "vehicule_type_encoded": vtype_enc,
    }


def predict_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prédit sur un DataFrame complet. Colonnes attendues identiques à predict_corridor.
    Retourne le DataFrame original avec les colonnes prédites ajoutées.
    """
    model1, model2, scaler, le = _load_models()

    df = df.copy()

    # Encodage vehicule_type
    df["vehicule_type"] = le.transform(df["vehicule_type"])

    # Valeurs manquantes → 0
    for col in FEATURES_M1:
        if col not in df.columns:
            df[col] = 0.0
    df[FEATURES_M1] = df[FEATURES_M1].fillna(0)

    X_scaled = scaler.transform(df[FEATURES_M1])
    X_m1 = X_scaled
    X_m2 = X_scaled[:, [FEATURES_M1.index(f) for f in FEATURES_M2]]

    df["is_substitutable"]    = model1.predict(X_m1)
    df["proba_substitutable"] = model1.predict_proba(X_m1)[:, 1].round(4)

    # Régression uniquement sur les corridors substituables
    df["co2_saved_kg"] = np.nan
    mask = df["is_substitutable"] == 1
    if mask.any():
        df.loc[mask, "co2_saved_kg"] = np.maximum(
            0, model2.predict(X_m2[mask])
        ).round(1)

    return df


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args():
    parser = argparse.ArgumentParser(
        description="ObRail — Prédiction substitution avion → train"
    )
    parser.add_argument("--origin",       type=str,   default="?",    help="Nom gare départ")
    parser.add_argument("--destination",  type=str,   default="?",    help="Nom gare arrivée")
    parser.add_argument("--distance",     type=float, required=True,  help="Distance en km")
    parser.add_argument("--vehicule_type",type=str,   default="Train Longue Distance",
                        help=f"Type de train : {VEHICULE_TYPES}")
    parser.add_argument("--co2_train",    type=float, default=0,      help="CO2 train kg (optionnel)")
    parser.add_argument("--co2_avion",    type=float, default=0,      help="CO2 avion kg (optionnel)")
    parser.add_argument("--origin_traffic",   type=float, default=0)
    parser.add_argument("--dest_traffic",     type=float, default=0)
    parser.add_argument("--origin_pop",       type=float, default=0)
    parser.add_argument("--dest_pop",         type=float, default=0)
    parser.add_argument("--trip_corridor",    type=float, default=0)
    parser.add_argument("--trip_origin",      type=float, default=0)
    return parser.parse_args()


def _print_result(args, result):
    print()
    print("=" * 55)
    print(f"  Corridor : {args.origin} → {args.destination}")
    print(f"  Distance : {args.distance} km")
    print(f"  Train    : {args.vehicule_type}")
    print("=" * 55)

    sub = result["is_substitutable"]
    prob = result["proba_substitutable"] * 100

    if sub == 1:
        print(f"  ✅ SUBSTITUABLE  (confiance : {prob:.1f}%)")
        print(f"  🌿 CO2 économisé : {result['co2_saved_kg']} kg/passager")
        print(f"     (vs avion — méthodologie EcoPassenger)")
    else:
        print(f"  ❌ NON SUBSTITUABLE  (confiance non-sub : {100-prob:.1f}%)")
        print(f"     Distance > 600km ou pas de vol direct")

    print("=" * 55)
    print()


if __name__ == "__main__":
    args = _parse_args()

    service_share = (args.trip_corridor / args.trip_origin
                     if args.trip_origin > 0 else 0.0)

    corridor = {
        "distance_km":            args.distance,
        "vehicule_type":          args.vehicule_type,
        "co2_train_kg":           args.co2_train,
        "co2_avion_kg":           args.co2_avion,
        "origin_station_traffic": args.origin_traffic,
        "dest_station_traffic":   args.dest_traffic,
        "origin_city_population": args.origin_pop,
        "dest_city_population":   args.dest_pop,
        "trip_count_corridor":    args.trip_corridor,
        "trip_count_origin":      args.trip_origin,
        "service_share":          service_share,
    }

    try:
        result = predict_corridor(corridor)
        _print_result(args, result)
    except ValueError as e:
        print(f"\n❌ Erreur : {e}")
        print(f"   Types acceptés : {VEHICULE_TYPES}")
        sys.exit(1)
