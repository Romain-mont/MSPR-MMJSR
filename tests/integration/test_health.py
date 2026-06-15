"""
Tests d'intégration — Endpoints de santé : GET / et GET /health
AMDEC R3 (NPR=120) : disponibilité de l'API = exigence de monitoring de base.
Niveau : intégration (TestClient FastAPI + mock DB).
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError

from tests.conftest import make_engine_mock, TRAJET_ROW


class TestRootEndpoint:
    """GET / — endpoint de bienvenue, toujours disponible sans DB."""

    def test_root_retourne_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_root_contient_status_online(self, client):
        response = client.get("/")
        assert response.json()["status"] == "online"

    def test_root_contient_lien_docs(self, client):
        response = client.get("/")
        body = response.json()
        assert "message" in body
        assert "/docs" in body["message"]


class TestHealthEndpoint:
    """GET /health — vérifie l'état de l'API et de la connexion DB."""

    def test_health_db_ok_retourne_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_db_ok_retourne_status_ok(self, client):
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "ok"
        assert data["db"] is True

    def test_health_contient_version(self, client):
        response = client.get("/health")
        assert "version" in response.json()

    def test_health_db_ko_retourne_503(self):
        """AMDEC R3 : si la DB est indisponible, le service doit signaler la dégradation."""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError(
            "could not connect", None, None
        )
        with patch("api.main.engine", mock_engine), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/health")
        assert response.status_code == 503

    def test_health_db_ko_retourne_status_degraded(self):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError(
            "could not connect", None, None
        )
        with patch("api.main.engine", mock_engine), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/health")
        body = response.json()
        assert body["detail"]["status"] == "degraded"
        assert body["detail"]["db"] is False
