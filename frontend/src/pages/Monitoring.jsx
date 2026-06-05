import { useEffect, useState } from 'react'
import { api } from '../services/api'
import BackButton from '../components/BackButton'
import './Monitoring.css'

const GRAFANA_URL = import.meta.env.VITE_GRAFANA_URL || 'http://localhost:3000'
const PROMETHEUS_URL = import.meta.env.VITE_PROMETHEUS_URL || 'http://localhost:9090'

export default function Monitoring() {
  const [health, setHealth]     = useState(null)
  const [error, setError]       = useState(null)
  const [lastCheck, setLastCheck] = useState(null)

  const check = () => {
    api.health()
      .then(h => { setHealth(h); setError(null) })
      .catch(e => { setError(e.message); setHealth(null) })
      .finally(() => setLastCheck(new Date().toLocaleTimeString()))
  }

  useEffect(() => {
    check()
    const interval = setInterval(check, 30000)
    return () => clearInterval(interval)
  }, [])

  const isOk = health?.status === 'ok'

  return (
    <main className="monitoring" id="main-content">
      <BackButton />
      <h1>Monitoring — ObRail Europe</h1>

      <div className="monitoring__grid">

        <section className="card monitoring__health" aria-labelledby="health-title">
          <h2 id="health-title">État du service</h2>
          <div className={`monitoring__status-badge ${isOk ? 'status-ok' : 'status-error'}`}>
            <span className="monitoring__dot" aria-hidden="true" />
            {isOk ? 'Opérationnel' : error ? 'Indisponible' : 'Vérification…'}
          </div>
          <dl className="monitoring__details">
            <div>
              <dt>API</dt>
              <dd><span className={`badge ${isOk ? 'badge-green' : 'badge-red'}`}>{health?.status ?? '—'}</span></dd>
            </div>
            <div>
              <dt>Base de données</dt>
              <dd><span className={`badge ${health?.db ? 'badge-green' : 'badge-red'}`}>{health?.db ? 'connectée' : 'déconnectée'}</span></dd>
            </div>
            <div>
              <dt>Version API</dt>
              <dd>{health?.version ?? '—'}</dd>
            </div>
            <div>
              <dt>Dernière vérification</dt>
              <dd>{lastCheck ?? '—'}</dd>
            </div>
          </dl>
          <button className="btn-outline" onClick={check} aria-label="Vérifier l'état maintenant">
            🔄 Vérifier maintenant
          </button>
          {error && <p className="monitoring__error" role="alert">⚠️ {error}</p>}
        </section>

        <section className="card" aria-labelledby="tools-title">
          <h2 id="tools-title">Outils de monitoring</h2>
          <div className="monitoring__tools">
            <a href={GRAFANA_URL} target="_blank" rel="noopener noreferrer"
               className="monitoring__tool-link" aria-label="Ouvrir Grafana (nouvel onglet)">
              <span className="monitoring__tool-icon" aria-hidden="true">📊</span>
              <div>
                <strong>Grafana</strong>
                <p>Tableaux de bord : latence, débit, erreurs, métriques ML</p>
                <p className="monitoring__tool-url">{GRAFANA_URL}</p>
              </div>
            </a>
            <a href={PROMETHEUS_URL} target="_blank" rel="noopener noreferrer"
               className="monitoring__tool-link" aria-label="Ouvrir Prometheus (nouvel onglet)">
              <span className="monitoring__tool-icon" aria-hidden="true">🔥</span>
              <div>
                <strong>Prometheus</strong>
                <p>Collecte des métriques — scrape toutes les 15s</p>
                <p className="monitoring__tool-url">{PROMETHEUS_URL}</p>
              </div>
            </a>
          </div>
        </section>

        <section className="card monitoring__endpoints" aria-labelledby="endpoints-title">
          <h2 id="endpoints-title">Endpoints disponibles</h2>
          <table>
            <thead>
              <tr>
                <th scope="col">Endpoint</th>
                <th scope="col">Méthode</th>
                <th scope="col">Description</th>
              </tr>
            </thead>
            <tbody>
              {[
                ['/health',                'GET',  'État de santé du service'],
                ['/trajets',              'GET',  'Liste des trajets (filtrable)'],
                ['/trajets/{id}',         'GET',  'Détail d\'un trajet'],
                ['/stats/volumes',        'GET',  'Statistiques agrégées jour/nuit'],
                ['/predict/substitution', 'POST', 'Modèle 1 — substituabilité'],
                ['/predict/co2_saved',    'POST', 'Modèle 2 — gain CO₂'],
                ['/metrics',              'GET',  'Métriques Prometheus'],
                ['/docs',                 'GET',  'Documentation Swagger'],
              ].map(([path, method, desc]) => (
                <tr key={path}>
                  <td><code>{path}</code></td>
                  <td><span className={`badge ${method === 'GET' ? 'badge-grey' : 'badge-green'}`}>{method}</span></td>
                  <td>{desc}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

      </div>
    </main>
  )
}
