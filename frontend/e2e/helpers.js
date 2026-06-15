/**
 * Données fictives et helpers partagés entre tous les tests E2E.
 * L'API backend (port 8000) est entièrement mockée via page.route() —
 * le serveur Vite (port 5173) est le seul processus requis.
 *
 * IMPORTANT : les patterns de route ciblent explicitement http://localhost:8000/
 * pour éviter d'intercepter les assets JS servis par Vite (ex: Trajets-xxxx.js).
 */

export const MOCK_HEALTH_OK = {
  status: 'ok',
  db: true,
  version: '1.0.0',
}

export const MOCK_HEALTH_DOWN = {
  status: 'error',
  db: false,
  version: '1.0.0',
}

export const MOCK_STATS = {
  repartition_jour_nuit: [
    { type_service: 'Jour',  nb_trajets: 38_500, nb_substituables: 34_000 },
    { type_service: 'Nuit',  nb_trajets:  7_606, nb_substituables:  7_000 },
  ],
  par_vehicule: [
    { label: 'Train Longue Distance', nb_trajets: 15_000, co2_saved_moy_kg: 92.5, nb_substituables: 13_500 },
    { label: 'InterCity',             nb_trajets: 12_000, co2_saved_moy_kg: 88.0, nb_substituables: 10_800 },
  ],
  global: {
    total_trajets:     46_106,
    substituables:     41_000,
    co2_saved_moy_kg:  92.8,
  },
}

export const MOCK_TRAJETS = [
  {
    id: 1, origine: 'Paris', destination: 'Lyon',
    distance_km: 450.0, vehicule_type: 'InterCity',
    co2_saved_kg: 95.0, is_substitutable: 1,
    co2_train_kg: 3.5, proba_substitutable: 0.87,
  },
  {
    id: 2, origine: 'Paris', destination: 'Marseille',
    distance_km: 770.0, vehicule_type: 'Train Longue Distance',
    co2_saved_kg: null, is_substitutable: 0,
    co2_train_kg: 6.0, proba_substitutable: 0.12,
  },
]

export const MOCK_PREDICT_OK = {
  is_substitutable: 1,
  proba_substitutable: 0.87,
  co2_saved_kg: 95.0,
  co2_avion_kg_used: 134.0,
  co2_avion_estimated: true,
  origin: 'Paris',
  destination: 'Lyon',
  vehicule_type_encoded: 1,
  latency_ms: 12.5,
}

export const MOCK_PREDICT_NO = {
  is_substitutable: 0,
  proba_substitutable: 0.12,
  co2_saved_kg: null,
  co2_avion_kg_used: 200.0,
  co2_avion_estimated: true,
  origin: 'Paris',
  destination: 'New York',
  vehicule_type_encoded: 1,
  latency_ms: 8.0,
}

/** URL de base de l'API FastAPI dans les tests (valeur par défaut de VITE_API_URL). */
const API = 'http://localhost:8000'

/**
 * Monte les mocks API standard (health OK + stats + trajets + predict).
 * Doit être appelé AVANT page.goto() pour que les requêtes soient interceptées.
 */
export async function mockApiOk(page) {
  await page.route(`${API}/health`, route =>
    route.fulfill({ contentType: 'application/json', body: JSON.stringify(MOCK_HEALTH_OK) })
  )
  await page.route(`${API}/stats/volumes`, route =>
    route.fulfill({ contentType: 'application/json', body: JSON.stringify(MOCK_STATS) })
  )
  await page.route(`${API}/trajets**`, route =>
    route.fulfill({ contentType: 'application/json', body: JSON.stringify(MOCK_TRAJETS) })
  )
  await page.route(`${API}/predict/**`, route =>
    route.fulfill({ contentType: 'application/json', body: JSON.stringify(MOCK_PREDICT_OK) })
  )
}

/** Monte les mocks avec API/DB en erreur (HTTP 503). */
export async function mockApiDown(page) {
  await page.route(`${API}/health`, route =>
    route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify(MOCK_HEALTH_DOWN) })
  )
  await page.route(`${API}/stats/volumes`, route =>
    route.fulfill({ status: 503, contentType: 'application/json', body: '{}' })
  )
  await page.route(`${API}/trajets**`, route =>
    route.fulfill({ status: 503, contentType: 'application/json', body: '{}' })
  )
  await page.route(`${API}/predict/**`, route =>
    route.fulfill({ status: 503, contentType: 'application/json', body: '{}' })
  )
}
