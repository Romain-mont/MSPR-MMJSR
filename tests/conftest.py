"""
Fixtures partagées — Stratégie de test ObRail Europe
Pyramide : beaucoup d'unitaires (base) → intégration (milieu) → peu d'E2E (sommet)
AMDEC couverts : R1 (ML), R2 (DB intégrité), R3 (API endpoints), R4 (validation inputs)
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# Racine du projet dans le path pour permettre les imports api.main et scripts.predict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Données de test représentatives ───────────────────────────────────────────

TRAJET_ROW = (
    1, "Paris", "Lyon", 450.0, "InterCity",
    3.5, 95.0, 91.5, 1,
    18_000_000.0, 14_000_000.0, 14.0, 180.0, 0.078,
)

TRAJET_ROW_NONSUB = (
    2, "Paris", "Marseille", 780.0, "Train Longue Distance",
    6.0, 145.0, None, 0,
    None, None, None, None, None,
)

PREDICT_SUBSTITUABLE = {
    "is_substitutable": 1,
    "proba_substitutable": 0.87,
    "co2_saved_kg": 95.0,
    "vehicule_type_encoded": 1,
}

PREDICT_NON_SUBSTITUABLE = {
    "is_substitutable": 0,
    "proba_substitutable": 0.12,
    "co2_saved_kg": None,
    "vehicule_type_encoded": 1,
}

# Payload valide pour les endpoints ML
CORRIDOR_PAYLOAD = {
    "origin": "Paris",
    "destination": "Lyon",
    "distance_km": 450.0,
    "vehicule_type": "InterCity",
    "flight_exists": True,
}

# ── Helpers de mocking DB ──────────────────────────────────────────────────────

def _make_conn_mock(fetchall=None, fetchone=None, side_effect=None):
    """Construit un mock de connexion SQLAlchemy (context manager)."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = fetchall if fetchall is not None else []
    mock_result.fetchone.return_value = fetchone

    mock_conn = MagicMock()
    if side_effect:
        mock_conn.execute.side_effect = side_effect
    else:
        mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


def make_engine_mock(fetchall=None, fetchone=None, side_effect=None):
    """Construit un mock de SQLAlchemy engine prêt à l'emploi."""
    mock_engine = MagicMock()
    mock_engine.connect.return_value = _make_conn_mock(fetchall, fetchone, side_effect)
    return mock_engine


# ── Fixture principale ─────────────────────────────────────────────────────────

@pytest.fixture
def client():
    """
    Client de test FastAPI.
    - DB mockée (pas de connexion réelle PostgreSQL)
    - predict_corridor mocké (pas de chargement des modèles .joblib)
    Comportement par défaut : trajet substituable, liste avec un trajet Paris→Lyon.
    """
    mock_engine = make_engine_mock(fetchall=[TRAJET_ROW], fetchone=TRAJET_ROW)
    with patch("api.main.engine", mock_engine), \
         patch("api.main.predict_corridor", return_value=PREDICT_SUBSTITUABLE.copy()):
        from fastapi.testclient import TestClient
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


@pytest.fixture
def client_db(request):
    """
    Client de test avec contrôle fin de la DB via le marqueur pytest.
    Usage: @pytest.mark.db(fetchall=[...], fetchone=..., side_effect=[...])
    """
    marker = request.node.get_closest_marker("db")
    kwargs = marker.kwargs if marker else {}
    mock_engine = make_engine_mock(**kwargs)
    with patch("api.main.engine", mock_engine), \
         patch("api.main.predict_corridor", return_value=PREDICT_SUBSTITUABLE.copy()):
        from fastapi.testclient import TestClient
        from api.main import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c
