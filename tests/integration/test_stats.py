"""
Tests d'intégration — Endpoint statistiques : GET /stats/volumes
AMDEC R2 (NPR=81) : cohérence des agrégations — les statistiques doivent refléter
fidèlement les données de la DB.
Niveau : intégration (TestClient + mock SQLAlchemy avec side_effect multi-execute).
"""
import pytest
from unittest.mock import MagicMock, patch

from tests.conftest import make_engine_mock


# ── Mock pour 3 appels execute() successifs dans stats_volumes ────────────────

def _make_stats_engine():
    """
    /stats/volumes fait 3 appels conn.execute() dans la même connexion.
    On utilise side_effect pour retourner un résultat différent à chaque appel.
    """
    # Résultat 1 : répartition jour/nuit
    row_repartition = MagicMock()
    row_repartition._mapping = {
        "type_service": "Jour",
        "nb_trajets": 150,
        "distance_moy_km": 420.5,
        "co2_saved_moy_kg": 82.3,
    }
    result_repartition = MagicMock()
    result_repartition.fetchall.return_value = [row_repartition]

    # Résultat 2 : par type de véhicule
    row_vehicule = MagicMock()
    row_vehicule._mapping = {
        "label": "InterCity",
        "nb_trajets": 150,
        "co2_saved_moy_kg": 82.3,
        "nb_substituables": 120,
    }
    result_vehicule = MagicMock()
    result_vehicule.fetchall.return_value = [row_vehicule]

    # Résultat 3 : global (fetchone retourne un tuple)
    result_global = MagicMock()
    result_global.fetchone.return_value = (300, 240, 82.3)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [result_repartition, result_vehicule, result_global]
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    return mock_engine


class TestStatsVolumes:
    """Vérifie la structure et la cohérence des agrégations retournées."""

    @pytest.fixture
    def stats_client(self):
        with patch("api.main.engine", _make_stats_engine()), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                yield c

    def test_retourne_200(self, stats_client):
        response = stats_client.get("/stats/volumes")
        assert response.status_code == 200

    def test_structure_reponse_contient_cles_attendues(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        assert "global" in data
        assert "repartition_jour_nuit" in data
        assert "par_vehicule" in data

    def test_global_contient_total_trajets(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        assert "total_trajets" in data["global"]
        assert data["global"]["total_trajets"] == 300

    def test_global_contient_substituables(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        assert "substituables" in data["global"]
        assert data["global"]["substituables"] == 240

    def test_repartition_est_une_liste(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        assert isinstance(data["repartition_jour_nuit"], list)

    def test_repartition_contient_type_service(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        repartition = data["repartition_jour_nuit"]
        assert len(repartition) >= 1
        assert "type_service" in repartition[0]

    def test_par_vehicule_est_une_liste(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        assert isinstance(data["par_vehicule"], list)

    def test_par_vehicule_contient_label(self, stats_client):
        data = stats_client.get("/stats/volumes").json()
        vehicules = data["par_vehicule"]
        assert len(vehicules) >= 1
        assert "label" in vehicules[0]
        assert "nb_trajets" in vehicules[0]
