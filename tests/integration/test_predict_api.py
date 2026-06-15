"""
Tests d'intégration — Endpoints ML : POST /predict/substitution et POST /predict/co2_saved
AMDEC R1 (NPR=240, CRITIQUE) : la prédiction ML est le cœur du service.
Niveau : intégration (TestClient + predict_corridor mocké, DB mockée).
"""
import pytest
from unittest.mock import patch

from tests.conftest import (
    make_engine_mock,
    CORRIDOR_PAYLOAD,
    PREDICT_SUBSTITUABLE,
    PREDICT_NON_SUBSTITUABLE,
)


# ── Fixture avec contrôle du résultat ML ───────────────────────────────────────

def _make_client(predict_result):
    """Crée un context manager retournant un TestClient avec predict_corridor mocké."""
    from fastapi.testclient import TestClient
    from api.main import app

    class _Ctx:
        def __enter__(self):
            self._p_engine = patch("api.main.engine", make_engine_mock())
            self._p_predict = patch("api.main.predict_corridor", return_value=predict_result)
            self._p_engine.__enter__()
            self._p_predict.__enter__()
            self._client = TestClient(app, raise_server_exceptions=False)
            self._client.__enter__()
            return self._client

        def __exit__(self, *args):
            self._client.__exit__(*args)
            self._p_predict.__exit__(*args)
            self._p_engine.__exit__(*args)

    return _Ctx()


# ── Tests POST /predict/substitution ─────────────────────────────────────────

class TestPredictSubstitution:
    """Modèle 1 — Classification : ce corridor est-il substituable avion → train ?"""

    def test_retourne_200_avec_payload_valide(self, client):
        response = client.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        assert response.status_code == 200

    def test_structure_reponse_substituable(self, client):
        response = client.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        data = response.json()
        champs = {"origin", "destination", "distance_km", "vehicule_type",
                  "is_substitutable", "proba_substitutable",
                  "co2_avion_kg_used", "co2_avion_estimated", "latency_ms"}
        assert champs.issubset(data.keys())

    def test_is_substitutable_vaut_1_quand_substituable(self, client):
        response = client.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        assert response.json()["is_substitutable"] == 1

    def test_proba_entre_0_et_1(self, client):
        response = client.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        proba = response.json()["proba_substitutable"]
        assert 0.0 <= proba <= 1.0

    def test_latency_ms_positive(self, client):
        response = client.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        assert response.json()["latency_ms"] >= 0.0

    def test_co2_avion_estime_si_absent(self, client):
        """Sans co2_avion_kg dans le payload, la valeur doit être estimée."""
        payload = {**CORRIDOR_PAYLOAD, "flight_exists": True}
        # co2_avion_kg absent → co2_avion_estimated doit être True
        payload.pop("co2_avion_kg", None)
        response = client.post("/predict/substitution", json=payload)
        assert response.json()["co2_avion_estimated"] is True

    def test_co2_avion_non_estime_si_fourni(self, client):
        payload = {**CORRIDOR_PAYLOAD, "co2_avion_kg": 95.0}
        response = client.post("/predict/substitution", json=payload)
        assert response.json()["co2_avion_estimated"] is False

    def test_flight_exists_false_co2_avion_zero(self, client):
        """Pas de vol direct → CO2 avion forcé à 0, non estimé."""
        payload = {**CORRIDOR_PAYLOAD, "flight_exists": False}
        response = client.post("/predict/substitution", json=payload)
        data = response.json()
        assert data["co2_avion_kg_used"] == 0.0
        assert data["co2_avion_estimated"] is False

    def test_is_substitutable_0_quand_non_substituable(self):
        with _make_client(PREDICT_NON_SUBSTITUABLE.copy()) as c:
            response = c.post("/predict/substitution", json=CORRIDOR_PAYLOAD)
        assert response.json()["is_substitutable"] == 0

    def test_champ_manquant_retourne_422(self, client):
        """AMDEC R4 : la validation Pydantic doit rejeter un payload incomplet."""
        payload_incomplet = {"origin": "Paris", "destination": "Lyon"}
        response = client.post("/predict/substitution", json=payload_incomplet)
        assert response.status_code == 422

    def test_distance_negative_retourne_422(self, client):
        """distance_km doit être > 0 (contrainte Field gt=0)."""
        payload = {**CORRIDOR_PAYLOAD, "distance_km": -50.0}
        response = client.post("/predict/substitution", json=payload)
        assert response.status_code == 422

    def test_vehicule_type_invalide_retourne_erreur(self):
        """Un vehicule_type non reconnu par le label encoder doit retourner une erreur."""
        with patch("api.main.engine", make_engine_mock()), \
             patch("api.main.predict_corridor", side_effect=ValueError("vehicule_type invalide")):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                payload = {**CORRIDOR_PAYLOAD, "vehicule_type": "TGV_INCONNU"}
                response = c.post("/predict/substitution", json=payload)
        assert response.status_code in (422, 500)

    @pytest.mark.parametrize("vtype", [
        "EuroNight", "InterCity", "Nightjet",
        "Train Longue Distance", "Train Longue Distance Nuit"
    ])
    def test_tous_vehicule_types_valides_acceptes(self, client, vtype):
        payload = {**CORRIDOR_PAYLOAD, "vehicule_type": vtype}
        response = client.post("/predict/substitution", json=payload)
        assert response.status_code == 200


# ── Tests POST /predict/co2_saved ─────────────────────────────────────────────

class TestPredictCo2Saved:
    """Modèle 2 — Régression : combien de CO2 économisé si le passager prend le train ?"""

    def test_retourne_200_avec_payload_valide(self, client):
        response = client.post("/predict/co2_saved", json=CORRIDOR_PAYLOAD)
        assert response.status_code == 200

    def test_structure_reponse(self, client):
        response = client.post("/predict/co2_saved", json=CORRIDOR_PAYLOAD)
        data = response.json()
        champs = {"origin", "destination", "distance_km", "vehicule_type",
                  "is_substitutable", "proba_substitutable",
                  "co2_saved_kg", "co2_avion_kg_used", "co2_avion_estimated", "latency_ms"}
        assert champs.issubset(data.keys())

    def test_co2_saved_kg_present_si_substituable(self, client):
        response = client.post("/predict/co2_saved", json=CORRIDOR_PAYLOAD)
        assert response.json()["co2_saved_kg"] == 95.0

    def test_co2_saved_kg_null_si_non_substituable(self):
        with _make_client(PREDICT_NON_SUBSTITUABLE.copy()) as c:
            response = c.post("/predict/co2_saved", json=CORRIDOR_PAYLOAD)
        assert response.json()["co2_saved_kg"] is None

    def test_champ_manquant_retourne_422(self, client):
        payload_incomplet = {"distance_km": 450.0}
        response = client.post("/predict/co2_saved", json=payload_incomplet)
        assert response.status_code == 422

    def test_latency_ms_presente(self, client):
        response = client.post("/predict/co2_saved", json=CORRIDOR_PAYLOAD)
        assert "latency_ms" in response.json()

    def test_distance_zero_retourne_422(self, client):
        payload = {**CORRIDOR_PAYLOAD, "distance_km": 0.0}
        response = client.post("/predict/co2_saved", json=payload)
        assert response.status_code == 422

    def test_payload_avec_toutes_features_optionnelles(self, client):
        payload = {
            **CORRIDOR_PAYLOAD,
            "co2_train_kg": 3.5,
            "co2_avion_kg": 95.0,
            "origin_station_traffic": 18_000_000,
            "dest_station_traffic": 14_000_000,
            "origin_city_population": 2_161_000,
            "dest_city_population": 522_000,
            "trip_count_corridor": 14,
            "trip_count_origin": 180,
            "service_share": 0.078,
        }
        response = client.post("/predict/co2_saved", json=payload)
        assert response.status_code == 200
