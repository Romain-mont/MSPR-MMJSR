"""
Tests unitaires — scripts/predict.py (predict_corridor)
AMDEC R1 (NPR=240, CRITIQUE) : une prédiction ML incorrecte est le risque le plus élevé.
Les modèles sont mockés : on teste la LOGIQUE de predict_corridor, pas les modèles eux-mêmes.
Niveau : unitaire (isolation totale via mocks joblib).
"""
import pytest
import sys
import os
import numpy as np
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from scripts.predict import predict_corridor, VEHICULE_TYPES, FEATURES_M1, FEATURES_M2


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_mock_models(is_sub=1, proba=0.87, co2_saved=95.0):
    """Crée des mocks des 4 artefacts joblib (model1, model2, scaler, label_encoder)."""
    model1 = MagicMock()
    model1.predict.return_value = np.array([is_sub])
    model1.predict_proba.return_value = np.array([[1 - proba, proba]])

    model2 = MagicMock()
    model2.predict.return_value = np.array([co2_saved])

    scaler = MagicMock()
    scaler.transform.return_value = np.zeros((1, len(FEATURES_M1)))

    le = MagicMock()
    le.classes_ = np.array(VEHICULE_TYPES)
    le.transform.return_value = np.array([1])

    return model1, model2, scaler, le


CORRIDOR_MINIMAL = {
    "distance_km": 450.0,
    "vehicule_type": "InterCity",
}

CORRIDOR_COMPLET = {
    "distance_km": 450.0,
    "vehicule_type": "InterCity",
    "co2_train_kg": 3.5,
    "co2_avion_kg": 95.0,
    "origin_station_traffic": 18_000_000,
    "dest_station_traffic": 14_000_000,
    "origin_city_population": 2_161_000,
    "dest_city_population": 522_000,
    "ratio_origin": 8.33,
    "ratio_dest": 26.82,
    "trip_count_corridor": 14,
    "trip_count_origin": 180,
    "service_share": 0.078,
}


# ── Tests predict_corridor ─────────────────────────────────────────────────────

class TestPredictCorridorSubstituable:
    """Corridor court-courrier avec vol direct → doit être substituable (is_sub=1)."""

    def test_retourne_is_substitutable_1(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models(is_sub=1)):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert result["is_substitutable"] == 1

    def test_retourne_proba_substitutable_correcte(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models(proba=0.87)):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert result["proba_substitutable"] == pytest.approx(0.87, rel=1e-3)

    def test_retourne_co2_saved_kg_positif(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models(co2_saved=95.0)):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert result["co2_saved_kg"] == pytest.approx(95.0, rel=1e-2)

    def test_co2_saved_jamais_negatif(self):
        """Le modèle peut prédire une valeur négative — predict_corridor doit la ramener à 0."""
        with patch("scripts.predict._load_models", return_value=_make_mock_models(co2_saved=-5.0)):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert result["co2_saved_kg"] == 0.0

    def test_corridor_complet_avec_toutes_features(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models()):
            result = predict_corridor(CORRIDOR_COMPLET)
        assert "is_substitutable" in result
        assert "proba_substitutable" in result
        assert "co2_saved_kg" in result


class TestPredictCorridorNonSubstituable:
    """Corridor long-courrier ou sans vol → non substituable (is_sub=0)."""

    def test_retourne_is_substitutable_0(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models(is_sub=0, proba=0.12)):
            result = predict_corridor({"distance_km": 1200.0, "vehicule_type": "InterCity"})
        assert result["is_substitutable"] == 0

    def test_co2_saved_kg_est_none_si_non_substituable(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models(is_sub=0, proba=0.12)):
            result = predict_corridor({"distance_km": 1200.0, "vehicule_type": "InterCity"})
        assert result["co2_saved_kg"] is None

    def test_model2_non_appele_si_non_substituable(self):
        """Le modèle de régression ne doit pas être appelé si is_sub=0 (performance)."""
        mocks = _make_mock_models(is_sub=0, proba=0.12)
        with patch("scripts.predict._load_models", return_value=mocks):
            predict_corridor({"distance_km": 1200.0, "vehicule_type": "InterCity"})
        mocks[1].predict.assert_not_called()


class TestPredictCorridorValidation:
    """AMDEC R4 : validation des entrées — un type de véhicule invalide doit lever ValueError."""

    def test_vehicule_type_invalide_leve_value_error(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models()):
            with pytest.raises(ValueError, match="vehicule_type"):
                predict_corridor({"distance_km": 450.0, "vehicule_type": "TGV_INCONNU"})

    @pytest.mark.parametrize("vtype", VEHICULE_TYPES)
    def test_tous_les_types_valides_acceptes(self, vtype):
        with patch("scripts.predict._load_models", return_value=_make_mock_models()):
            result = predict_corridor({"distance_km": 450.0, "vehicule_type": vtype})
        assert result["is_substitutable"] in (0, 1)

    def test_features_manquantes_completees_avec_zero(self):
        """Les features optionnelles absentes doivent être remplacées par 0 sans erreur."""
        with patch("scripts.predict._load_models", return_value=_make_mock_models()):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert result is not None

    def test_vehicule_type_encoded_dans_resultat(self):
        with patch("scripts.predict._load_models", return_value=_make_mock_models()):
            result = predict_corridor(CORRIDOR_MINIMAL)
        assert "vehicule_type_encoded" in result
        assert isinstance(result["vehicule_type_encoded"], int)

    def test_dist_to_600_calcule_correctement(self):
        """dist_to_600 = distance - 600 (pivot loi française 2023)."""
        captured_df = {}

        def capture_transform(df):
            captured_df["data"] = df.copy()
            return np.zeros((1, len(FEATURES_M1)))

        mocks = _make_mock_models()
        mocks[2].transform.side_effect = capture_transform

        with patch("scripts.predict._load_models", return_value=mocks):
            predict_corridor({"distance_km": 450.0, "vehicule_type": "InterCity"})

        assert captured_df["data"]["dist_to_600"].iloc[0] == pytest.approx(450.0 - 600.0)


# ── Tests predict_batch ───────────────────────────────────────────────────────

class TestPredictBatch:
    """predict_batch doit traiter un DataFrame complet et ajouter les colonnes de prédiction."""

    def _make_df(self, n=3):
        import pandas as pd
        return pd.DataFrame({
            "distance_km": ([450.0, 1200.0, 300.0] * (n // 3 + 1))[:n],
            "vehicule_type": ["InterCity"] * n,
            "co2_train_kg": [3.5] * n,
            "co2_avion_kg": [95.0] * n,
        })

    def test_retourne_dataframe_avec_colonnes_prediction(self):
        from scripts.predict import predict_batch
        import pandas as pd
        import numpy as np

        mocks = _make_mock_models(is_sub=1, proba=0.87, co2_saved=95.0)
        # model1.predict doit retourner un array de la bonne taille
        mocks[0].predict.return_value = np.array([1, 1, 1])
        mocks[0].predict_proba.return_value = np.array([[0.13, 0.87]] * 3)
        mocks[1].predict.return_value = np.array([95.0, 95.0, 95.0])
        mocks[2].transform.return_value = np.zeros((3, len(FEATURES_M1)))
        mocks[3].transform.return_value = np.array([1, 1, 1])

        df = self._make_df(3)
        with patch("scripts.predict._load_models", return_value=mocks):
            result = predict_batch(df)

        assert "is_substitutable" in result.columns
        assert "proba_substitutable" in result.columns
        assert "co2_saved_kg" in result.columns

    def test_batch_non_substituable_co2_saved_nan(self):
        from scripts.predict import predict_batch
        import pandas as pd
        import numpy as np

        mocks = _make_mock_models(is_sub=0, proba=0.12)
        mocks[0].predict.return_value = np.array([0, 0])
        mocks[0].predict_proba.return_value = np.array([[0.88, 0.12]] * 2)
        mocks[2].transform.return_value = np.zeros((2, len(FEATURES_M1)))
        mocks[3].transform.return_value = np.array([1, 1])

        df = pd.DataFrame({
            "distance_km": [1200.0, 900.0],
            "vehicule_type": ["InterCity", "InterCity"],
        })
        with patch("scripts.predict._load_models", return_value=mocks):
            result = predict_batch(df)

        assert result["co2_saved_kg"].isna().all()
