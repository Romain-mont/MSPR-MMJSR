"""
Tests unitaires — Fonctions de calcul CO2
AMDEC R1 (NPR élevé) : toute erreur dans le calcul CO2 fausse les prédictions ML.
Niveau : unitaire (shift-left, isolation totale, pas de dépendance externe).
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from api.main import _estimate_co2_avion, _resolve_co2_avion, CorridorInput


# ── Tests _estimate_co2_avion ─────────────────────────────────────────────────
# Formule EcoPassenger : 40 + distance × 0.123

class TestEstimateCo2Avion:
    """Valide la formule EcoPassenger simplifiée utilisée quand aucune valeur n'est fournie."""

    def test_distance_zero_renvoie_emission_base(self):
        result = _estimate_co2_avion(0.0)
        assert result == 40.0

    def test_distance_450km_paris_lyon(self):
        result = _estimate_co2_avion(450.0)
        assert result == pytest.approx(40.0 + 450.0 * 0.123, rel=1e-3)

    def test_distance_600km_seuil_legal(self):
        result = _estimate_co2_avion(600.0)
        assert result == pytest.approx(40.0 + 600.0 * 0.123, rel=1e-3)

    def test_distance_1000km_longue_distance(self):
        result = _estimate_co2_avion(1000.0)
        assert result == pytest.approx(40.0 + 1000.0 * 0.123, rel=1e-3)

    def test_retourne_float_arrondi_1_decimal(self):
        result = _estimate_co2_avion(333.0)
        # Doit être arrondi à 1 décimale
        assert result == round(40.0 + 333.0 * 0.123, 1)

    def test_distance_negative_renvoie_valeur_coherente(self):
        # Le modèle ne valide pas les distances négatives à ce niveau
        result = _estimate_co2_avion(-100.0)
        assert isinstance(result, float)


# ── Tests _resolve_co2_avion ──────────────────────────────────────────────────

def _make_corridor(**kwargs):
    """Crée un CorridorInput minimal avec les champs requis."""
    defaults = {
        "origin": "Paris",
        "destination": "Lyon",
        "distance_km": 450.0,
        "vehicule_type": "InterCity",
        "flight_exists": True,
    }
    defaults.update(kwargs)
    return CorridorInput(**defaults)


class TestResolveCo2Avion:
    """
    Valide la logique de résolution de la valeur CO2 avion.
    Trois cas : vol inexistant / valeur fournie / estimation automatique.
    """

    def test_pas_de_vol_renvoie_zero_non_estime(self):
        corridor = _make_corridor(flight_exists=False)
        co2, estimated = _resolve_co2_avion(corridor)
        assert co2 == 0.0
        assert estimated is False

    def test_co2_fourni_utilise_valeur_sans_estimation(self):
        corridor = _make_corridor(co2_avion_kg=95.0)
        co2, estimated = _resolve_co2_avion(corridor)
        assert co2 == 95.0
        assert estimated is False

    def test_co2_absent_avec_vol_estime_automatiquement(self):
        corridor = _make_corridor(co2_avion_kg=None)
        co2, estimated = _resolve_co2_avion(corridor)
        assert co2 == pytest.approx(_estimate_co2_avion(450.0), rel=1e-3)
        assert estimated is True

    def test_co2_fourni_a_zero_utilise_zero(self):
        corridor = _make_corridor(co2_avion_kg=0.0)
        co2, estimated = _resolve_co2_avion(corridor)
        assert co2 == 0.0
        assert estimated is False

    def test_pas_de_vol_ignore_co2_avion_fourni(self):
        """flight_exists=False doit court-circuiter même si co2_avion_kg est fourni."""
        corridor = _make_corridor(flight_exists=False, co2_avion_kg=95.0)
        co2, estimated = _resolve_co2_avion(corridor)
        assert co2 == 0.0
