import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../services/api'
import StatCard from '../components/StatCard'
import './Home.css'

export default function Home() {
  const [stats, setStats]   = useState(null)
  const [health, setHealth] = useState(null)
  const [error, setError]   = useState(null)

  useEffect(() => {
    Promise.all([api.stats(), api.health()])
      .then(([s, h]) => { setStats(s); setHealth(h) })
      .catch(e => setError(e.message))
  }, [])

  const jour  = stats?.repartition_jour_nuit?.find(r => r.type_service === 'Jour')
  const nuit  = stats?.repartition_jour_nuit?.find(r => r.type_service === 'Nuit')
  const total = stats?.global?.total_trajets ?? '—'
  const co2   = stats?.global?.co2_saved_moy_kg ?? '—'
  const subs  = stats?.global?.substituables ?? '—'

  return (
    <main className="home" id="main-content">
      <section className="home__hero" aria-labelledby="hero-title">
        <h1 id="hero-title">ObRail Europe</h1>
        <p>Observatoire de la substitution avion → train et de l'impact CO₂</p>
        <div className="home__actions">
          <Link to="/trajets"    className="btn-primary" role="button">Explorer les trajets</Link>
          <Link to="/prediction" className="btn-outline" role="button">Tester la prédiction IA</Link>
        </div>
      </section>

      {error && (
        <div className="home__alert" role="alert">
          ⚠️ API indisponible : {error}
        </div>
      )}

      <section className="home__stats" aria-label="Indicateurs clés">
        <h2>Indicateurs clés</h2>
        <div className="home__cards">
          <StatCard icon="🚄" label="Corridors analysés"  value={total}           color="var(--primary)" />
          <StatCard icon="🌿" label="CO₂ économisé moyen" value={co2}   unit="kg" color="#27ae60" />
          <StatCard icon="✅" label="Corridors substituables" value={subs}         color="#2980b9" />
          <StatCard icon="☀️" label="Trains de jour"       value={jour?.nb_trajets ?? '—'} color="#f39c12" />
          <StatCard icon="🌙" label="Trains de nuit"       value={nuit?.nb_trajets ?? '—'} color="#8e44ad" />
        </div>
      </section>

      {stats?.par_vehicule?.length > 0 && (
        <section className="home__table card" aria-labelledby="vehicule-title">
          <h2 id="vehicule-title">Répartition par type de train</h2>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th scope="col">Type de train</th>
                  <th scope="col">Trajets</th>
                  <th scope="col">CO₂ économisé moy.</th>
                  <th scope="col">Substituables</th>
                </tr>
              </thead>
              <tbody>
                {stats.par_vehicule.map(v => (
                  <tr key={v.label}>
                    <td>{v.label}</td>
                    <td>{v.nb_trajets}</td>
                    <td>{v.co2_saved_moy_kg ?? '—'} kg</td>
                    <td>{v.nb_substituables}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <section className="home__status card" aria-label="État du service">
        <h2>État du service</h2>
        <p>
          API :{' '}
          <span className={`badge ${health?.status === 'ok' ? 'badge-green' : 'badge-red'}`}>
            {health?.status ?? 'inconnu'}
          </span>
          {' · '}
          Base de données :{' '}
          <span className={`badge ${health?.db ? 'badge-green' : 'badge-red'}`}>
            {health?.db ? 'connectée' : 'déconnectée'}
          </span>
        </p>
        <p>
          <Link to="/monitoring">→ Voir le tableau de bord monitoring</Link>
        </p>
      </section>
    </main>
  )
}
