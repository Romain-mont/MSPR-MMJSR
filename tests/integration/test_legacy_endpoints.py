"""
Tests d'intégration — Endpoints hérités : GET /data, GET /search, GET /compare
Ces endpoints couvrent l'accès aux données brutes fact_em (pipeline MSPR précédent).
Niveau : intégration (TestClient + mock SQLAlchemy).
"""
import pytest
from unittest.mock import MagicMock, patch

from tests.conftest import make_engine_mock


# ── Helpers ────────────────────────────────────────────────────────────────────

# Ligne renvoyée par la requête /data (9 colonnes)
DATA_ROW = ("Paris", "Lyon", "Paris", "Lyon", 450.0, True, "InterCity", 0.0041, 1.85)

# Ligne renvoyée par /search (5 colonnes)
SEARCH_ROW = ("Paris", "Lyon", 450.0, "InterCity", 1.85)

# Résultat agrégé pour /compare (4 colonnes : avg, count, min, max)
DAY_ROW   = (12.5, 3, 10.0, 15.0)
NIGHT_ROW = (8.0, 2, 7.0, 9.0)


def _make_client_with_rows(fetchall=None, fetchone=None, side_effect=None):
    """Client de test avec DB contrôlée."""
    from fastapi.testclient import TestClient
    from api.main import app
    mock_engine = make_engine_mock(fetchall=fetchall, fetchone=fetchone, side_effect=side_effect)

    class _Ctx:
        def __enter__(self_):
            self_._pe = patch("api.main.engine", mock_engine)
            self_._pp = patch("api.main.predict_corridor", return_value={})
            self_._pe.__enter__()
            self_._pp.__enter__()
            self_._c = TestClient(app, raise_server_exceptions=False)
            self_._c.__enter__()
            return self_._c

        def __exit__(self_, *a):
            self_._c.__exit__(*a)
            self_._pp.__exit__(*a)
            self_._pe.__exit__(*a)

    return _Ctx()


# ── Tests GET /data ─────────────────────────────────────────────────────────────

class TestDataEndpoint:
    """Endpoint d'export des données brutes pour dashboards."""

    def test_retourne_200(self):
        with _make_client_with_rows(fetchall=[DATA_ROW]) as c:
            assert c.get("/data").status_code == 200

    def test_retourne_liste(self):
        with _make_client_with_rows(fetchall=[DATA_ROW]) as c:
            assert isinstance(c.get("/data").json(), list)

    def test_structure_dataresponse(self):
        with _make_client_with_rows(fetchall=[DATA_ROW]) as c:
            item = c.get("/data").json()[0]
        champs = {"origine", "destination", "distance_km", "vehicule_type", "co2_kg"}
        assert champs.issubset(item.keys())

    def test_liste_vide_retourne_200(self):
        with _make_client_with_rows(fetchall=[]) as c:
            response = c.get("/data")
        assert response.status_code == 200
        assert response.json() == []

    def test_param_limit_accepte(self):
        with _make_client_with_rows(fetchall=[DATA_ROW]) as c:
            response = c.get("/data?limit=10")
        assert response.status_code == 200

    def test_valeurs_numeriques_correctes(self):
        with _make_client_with_rows(fetchall=[DATA_ROW]) as c:
            item = c.get("/data").json()[0]
        assert item["distance_km"] == 450.0
        assert item["co2_kg"] == pytest.approx(1.85, rel=1e-3)


# ── Tests GET /search ───────────────────────────────────────────────────────────

class TestSearchEndpoint:
    """Recherche d'itinéraire par gare de départ et d'arrivée."""

    def test_retourne_200_avec_resultat(self):
        with _make_client_with_rows(fetchall=[SEARCH_ROW]) as c:
            response = c.get("/search?depart=Paris&arrivee=Lyon")
        assert response.status_code == 200

    def test_retourne_liste_trajets(self):
        with _make_client_with_rows(fetchall=[SEARCH_ROW]) as c:
            data = c.get("/search?depart=Paris&arrivee=Lyon").json()
        assert isinstance(data, list)
        assert len(data) == 1

    def test_structure_trajet_response(self):
        with _make_client_with_rows(fetchall=[SEARCH_ROW]) as c:
            trajet = c.get("/search?depart=Paris&arrivee=Lyon").json()[0]
        champs = {"depart", "arrivee", "distance_km", "vehicule_type", "co2_kg"}
        assert champs.issubset(trajet.keys())

    def test_aucun_resultat_retourne_404(self):
        with _make_client_with_rows(fetchall=[]) as c:
            response = c.get("/search?depart=Inconnu&arrivee=Ailleurs")
        assert response.status_code == 404

    def test_filtre_vehicle_type_accepte(self):
        with _make_client_with_rows(fetchall=[SEARCH_ROW]) as c:
            response = c.get("/search?depart=Paris&arrivee=Lyon&vehicle_type=InterCity")
        assert response.status_code == 200

    def test_param_depart_requis(self):
        with _make_client_with_rows(fetchall=[]) as c:
            response = c.get("/search?arrivee=Lyon")
        assert response.status_code == 422


# ── Tests GET /compare ──────────────────────────────────────────────────────────

def _make_compare_engine(day_row=DAY_ROW, night_row=NIGHT_ROW):
    """Mock pour /compare : deux appels execute() (jour puis nuit)."""
    def _result(row, is_fetchone=True):
        r = MagicMock()
        r.fetchone.return_value = row
        return r

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [_result(day_row), _result(night_row)]
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    eng = MagicMock()
    eng.connect.return_value = mock_conn
    return eng


class TestCompareEndpoint:
    """Comparaison écologique train de jour vs train de nuit."""

    def test_retourne_200_avec_jour_et_nuit(self):
        with patch("api.main.engine", _make_compare_engine()), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/compare?depart=Paris&arrivee=Lyon")
        assert response.status_code == 200

    def test_structure_compare_response(self):
        with patch("api.main.engine", _make_compare_engine()), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                data = c.get("/compare?depart=Paris&arrivee=Lyon").json()
        champs = {"depart", "arrivee", "trains_jour", "trains_nuit",
                  "gain_ecologique_pct", "recommandation"}
        assert champs.issubset(data.keys())

    def test_gain_ecologique_calcule(self):
        with patch("api.main.engine", _make_compare_engine()), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                data = c.get("/compare?depart=Paris&arrivee=Lyon").json()
        # Gain = (12.5 - 8.0) / 12.5 * 100 = 36%
        assert data["gain_ecologique_pct"] == pytest.approx(36.0, rel=1e-2)

    def test_recommandation_nuit_si_gain_eleve(self):
        with patch("api.main.engine", _make_compare_engine()), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                data = c.get("/compare?depart=Paris&arrivee=Lyon").json()
        assert len(data["recommandation"]) > 0

    def test_pas_de_trains_de_jour_retourne_404(self):
        """Si aucun train de jour n'existe, l'endpoint doit renvoyer 404."""
        def _null_result():
            r = MagicMock()
            r.fetchone.return_value = (None, 0, None, None)
            return r

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [_null_result(), _null_result()]
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        eng = MagicMock()
        eng.connect.return_value = mock_conn

        with patch("api.main.engine", eng), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/compare?depart=Inexistant&arrivee=Nulle_Part")
        assert response.status_code == 404

    def test_pas_de_trains_de_nuit_retourne_200(self):
        """Sans train de nuit, la réponse est quand même 200 (cas partiel)."""
        def _make_results():
            day = MagicMock()
            day.fetchone.return_value = DAY_ROW
            night = MagicMock()
            night.fetchone.return_value = (None, 0, None, None)
            return [day, night]

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = _make_results()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        eng = MagicMock()
        eng.connect.return_value = mock_conn

        with patch("api.main.engine", eng), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/compare?depart=Paris&arrivee=Lyon")
        assert response.status_code == 200


# ── Tests error handling (DB exception sur endpoints) ─────────────────────────

class TestDbErrorHandling:
    """AMDEC R2 : les erreurs DB doivent renvoyer 500, pas planter le serveur."""

    def test_trajets_db_error_retourne_500(self):
        eng = MagicMock()
        eng.connect.side_effect = Exception("DB down")
        with patch("api.main.engine", eng), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/trajets")
        assert response.status_code == 500

    def test_trajet_by_id_db_error_retourne_500(self):
        eng = MagicMock()
        eng.connect.side_effect = Exception("DB down")
        with patch("api.main.engine", eng), \
             patch("api.main.predict_corridor", return_value={}):
            from fastapi.testclient import TestClient
            from api.main import app
            with TestClient(app, raise_server_exceptions=False) as c:
                response = c.get("/trajets/1")
        assert response.status_code == 500
