"""
Tests d'intégration — Endpoints trajets : GET /trajets et GET /trajets/{id}
AMDEC R2 (NPR=81) : intégrité des données — les endpoints doivent retourner
les bonnes données et gérer correctement les cas d'erreur.
Niveau : intégration (TestClient FastAPI + mock SQLAlchemy).
"""
import pytest
from unittest.mock import MagicMock, patch

from tests.conftest import make_engine_mock, TRAJET_ROW, TRAJET_ROW_NONSUB


# ── Helpers ────────────────────────────────────────────────────────────────────

def _client_with_rows(rows=None, fetchone=None):
    """Crée un TestClient avec une DB mockée retournant les lignes spécifiées."""
    from fastapi.testclient import TestClient
    from api.main import app
    mock_engine = make_engine_mock(fetchall=rows, fetchone=fetchone)
    return patch("api.main.engine", mock_engine), \
           patch("api.main.predict_corridor", return_value={})


# ── Tests GET /trajets ─────────────────────────────────────────────────────────

class TestGetTrajets:
    """Vérifie la liste des trajets avec et sans filtres."""

    def test_retourne_200(self, client):
        response = client.get("/trajets")
        assert response.status_code == 200

    def test_retourne_liste(self, client):
        response = client.get("/trajets")
        assert isinstance(response.json(), list)

    def test_retourne_au_moins_un_trajet_avec_donnees_mockees(self, client):
        response = client.get("/trajets")
        data = response.json()
        assert len(data) >= 1

    def test_structure_trajet(self, client):
        """Vérifie que les champs obligatoires sont présents dans chaque trajet."""
        response = client.get("/trajets")
        trajet = response.json()[0]
        champs_requis = {"id", "origine", "destination", "distance_km", "vehicule_type"}
        assert champs_requis.issubset(trajet.keys())

    def test_filtre_par_origine(self, client):
        response = client.get("/trajets?origine=Paris")
        assert response.status_code == 200

    def test_filtre_par_destination(self, client):
        response = client.get("/trajets?destination=Lyon")
        assert response.status_code == 200

    def test_filtre_substituable_true(self, client):
        response = client.get("/trajets?substituable=true")
        assert response.status_code == 200

    def test_filtre_substituable_false(self, client):
        response = client.get("/trajets?substituable=false")
        assert response.status_code == 200

    def test_pagination_limit(self, client):
        response = client.get("/trajets?limit=10")
        assert response.status_code == 200

    def test_pagination_offset(self, client):
        response = client.get("/trajets?offset=0")
        assert response.status_code == 200

    def test_liste_vide_retourne_200_et_tableau_vide(self):
        mock_engine = make_engine_mock(fetchall=[])
        with patch("api.main.engine", mock_engine), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/trajets")
        assert response.status_code == 200
        assert response.json() == []

    def test_valeurs_numeriques_correctes(self, client):
        response = client.get("/trajets")
        trajet = response.json()[0]
        assert trajet["distance_km"] == 450.0
        assert trajet["origine"] == "Paris"
        assert trajet["destination"] == "Lyon"


# ── Tests GET /trajets/{id} ────────────────────────────────────────────────────

class TestGetTrajetById:
    """AMDEC R2 : un trajet introuvable doit renvoyer 404, pas une erreur 500."""

    def test_id_existant_retourne_200(self, client):
        response = client.get("/trajets/1")
        assert response.status_code == 200

    def test_id_existant_retourne_bon_trajet(self, client):
        response = client.get("/trajets/1")
        data = response.json()
        assert data["id"] == 1
        assert data["origine"] == "Paris"
        assert data["destination"] == "Lyon"

    def test_id_inexistant_retourne_404(self):
        """DB retourne None → le trajet n'existe pas → 404."""
        mock_engine = make_engine_mock(fetchone=None)
        with patch("api.main.engine", mock_engine), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/trajets/99999")
        assert response.status_code == 404

    def test_id_inexistant_message_clair(self):
        mock_engine = make_engine_mock(fetchone=None)
        with patch("api.main.engine", mock_engine), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/trajets/99999")
        assert "99999" in response.json()["detail"]

    def test_id_non_entier_retourne_422(self, client):
        """Validation Pydantic : un ID non-entier doit être rejeté."""
        response = client.get("/trajets/abc")
        assert response.status_code == 422

    def test_structure_trajet_detail(self, client):
        response = client.get("/trajets/1")
        data = response.json()
        champs_requis = {"id", "origine", "destination", "distance_km", "vehicule_type"}
        assert champs_requis.issubset(data.keys())
