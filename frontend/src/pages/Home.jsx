import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../services/api'
import './Home.css'

const MODULES = [
  {
    to: '/trajets',
    icon: '🚄',
    title: 'Trajets ferroviaires',
    desc: 'Consultez et filtrez les 46 106 corridors ferroviaires européens. Recherche par gare, type de train ou niveau de substituabilité.',
    cta: 'Explorer les trajets',
    color: '#15803d',
    bg: '#f0fdf4',
  },
  {
    to: '/prediction',
    icon: '🌿',
    title: 'Prédiction IA',
    desc: 'Entrez un corridor et l\'IA prédit si le train peut remplacer l\'avion, avec le gain CO₂ exact (Random Forest R²=0.948).',
    cta: 'Analyser un corridor',
    color: '#0284c7',
    bg: '#e0f2fe',
  },
  {
    to: '/monitoring',
    icon: '📊',
    title: 'Monitoring',
    desc: 'Tableau de bord temps réel : état de l\'API, base de données, métriques Prometheus et liens Grafana.',
    cta: 'Voir le monitoring',
    color: '#7c3aed',
    bg: '#f5f3ff',
  },
]

const FACTS = [
  { icon: '⚖️', title: 'Loi française 2023', body: 'Tout vol de moins de 600 km desservi par une liaison ferroviaire directe inférieure à 2h30 doit être supprimé.' },
  { icon: '🌍', title: 'Enjeu climatique', body: 'L\'avion émet en moyenne 23,7× plus de CO₂ que le train sur les mêmes trajets (méthodologie EcoPassenger UIC/IFEU).' },
  { icon: '🔬', title: 'Méthodologie CRISP-DM', body: '6 phases itératives — de la collecte GTFS SNCF + Back on Track à la mise en production de 3 modèles ML complémentaires.' },
  { icon: '🤖', title: '3 modèles ML', body: 'M1 Classification (F1=1.000), M2 Régression CO₂ (R²=0.948, MAE=4.07 kg), M3 Clustering K-Means k=4 (Silhouette=0.652).' },
]

export default function Home() {
  const [stats, setStats]   = useState(null)
  const [health, setHealth] = useState(null)
  const [error, setError]   = useState(null)

  useEffect(() => {
    Promise.all([api.stats(), api.health()])
      .then(([s, h]) => { setStats(s); setHealth(h) })
      .catch(e => setError(e.message))
  }, [])

  const apiOk = health?.status === 'ok'
  const total = stats?.global?.total_trajets
  const subs  = stats?.global?.substituables
  const co2   = stats?.global?.co2_saved_moy_kg
  const jour  = stats?.repartition_jour_nuit?.find(r => r.type_service === 'Jour')
  const nuit  = stats?.repartition_jour_nuit?.find(r => r.type_service === 'Nuit')
  const subsPct = total && subs ? Math.round((subs / total) * 100) : 89

  return (
    <main className="home" id="main-content">

      {/* ── Hero ───────────────────────────────────────────── */}
      <section className="hero" aria-labelledby="hero-title">
        <div className="hero__content">
          <div className="hero__eyebrow">
            <span className="hero__badge">Bloc E6.3 — MSPR EPSI 2025–2026</span>
            <span className={'hero__api-pill ' + (apiOk ? 'pill-ok' : health === null ? 'pill-loading' : 'pill-down')}>
              <span className="hero__dot" aria-hidden="true" />
              {apiOk ? 'API opérationnelle' : health === null ? 'Vérification…' : 'API indisponible'}
            </span>
          </div>

          <h1 id="hero-title">
            Remplacer l'avion<br />
            <span className="hero__highlight">par le train</span>
          </h1>
          <p className="hero__desc">
            ObRail Europe analyse 46 106 corridors ferroviaires européens pour identifier
            les vols remplaçables par le train et quantifier le gain CO₂ par passager.
          </p>

          <div className="hero__kpis" aria-label="Indicateurs principaux">
            <div className="hero__kpi">
              <span className="hero__kpi-num">{total?.toLocaleString('fr-FR') ?? '46 106'}</span>
              <span className="hero__kpi-label">corridors analysés</span>
            </div>
            <div className="hero__kpi-sep" aria-hidden="true" />
            <div className="hero__kpi">
              <span className="hero__kpi-num">{co2 ?? '92.8'} kg</span>
              <span className="hero__kpi-label">CO₂ économisé / passager</span>
            </div>
            <div className="hero__kpi-sep" aria-hidden="true" />
            <div className="hero__kpi">
              <span className="hero__kpi-num">{subsPct}%</span>
              <span className="hero__kpi-label">corridors substituables</span>
            </div>
          </div>

          <div className="hero__actions">
            <Link to="/trajets" className="btn-primary hero__cta">
              Explorer les trajets →
            </Link>
            <Link to="/prediction" className="hero__cta-ghost">
              Tester la prédiction IA
            </Link>
          </div>
        </div>

        <div className="hero__visual" aria-hidden="true">
          <div className="hero__visual-card">
            <div className="hero__visual-row">
              <span>✈️</span>
              <div className="hero__visual-bar hero__visual-bar--red" style={{ width: '100%' }}>134 kg CO₂</div>
            </div>
            <div className="hero__visual-row">
              <span>🚄</span>
              <div className="hero__visual-bar hero__visual-bar--green" style={{ width: '20%' }}>3.5 kg CO₂</div>
            </div>
            <div className="hero__visual-saving">💚 130 kg économisés • Paris → Marseille</div>
          </div>
          <div className="hero__visual-badge">
            <span>×23.7</span>
            <small>plus de CO₂ en avion</small>
          </div>
        </div>
      </section>

      {error && (
        <div className="home__alert" role="alert">
          ⚠️ Impossible de joindre l'API — certaines données seront indisponibles.
        </div>
      )}

      {/* ── 3 modules ──────────────────────────────────────── */}
      <section className="modules" aria-labelledby="modules-title">
        <h2 id="modules-title" className="section-title">L'application en 3 modules</h2>
        <div className="modules__grid">
          {MODULES.map(m => (
            <Link to={m.to} key={m.to} className="module-card" style={{ '--m-color': m.color, '--m-bg': m.bg }}>
              <div className="module-card__icon">{m.icon}</div>
              <div className="module-card__body">
                <h3>{m.title}</h3>
                <p>{m.desc}</p>
              </div>
              <span className="module-card__cta">{m.cta} →</span>
            </Link>
          ))}
        </div>
      </section>

      {/* ── Stats en direct ───────────────────────────────── */}
      {stats && (
        <section className="live-stats" aria-labelledby="stats-title">
          <h2 id="stats-title" className="section-title">Données en direct</h2>
          <div className="live-stats__grid">
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#f0fdf4', color: '#15803d' }}>🚄</div>
              <div>
                <div className="stat-big__num">{total?.toLocaleString('fr-FR') ?? '—'}</div>
                <div className="stat-big__label">Corridors analysés</div>
              </div>
            </div>
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#dcfce7', color: '#15803d' }}>🌿</div>
              <div>
                <div className="stat-big__num">{co2 ?? '—'} kg</div>
                <div className="stat-big__label">CO₂ économisé moyen</div>
              </div>
            </div>
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#dbeafe', color: '#1d4ed8' }}>✅</div>
              <div>
                <div className="stat-big__num">{subs?.toLocaleString('fr-FR') ?? '—'}</div>
                <div className="stat-big__label">Corridors substituables</div>
              </div>
            </div>
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#fef3c7', color: '#92400e' }}>☀️</div>
              <div>
                <div className="stat-big__num">{jour?.nb_trajets?.toLocaleString('fr-FR') ?? '—'}</div>
                <div className="stat-big__label">Trains de jour</div>
              </div>
            </div>
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#ede9fe', color: '#6d28d9' }}>🌙</div>
              <div>
                <div className="stat-big__num">{nuit?.nb_trajets?.toLocaleString('fr-FR') ?? '—'}</div>
                <div className="stat-big__label">Trains de nuit</div>
              </div>
            </div>
            <div className="stat-big card">
              <div className="stat-big__icon" style={{ background: '#f0fdf4', color: '#15803d' }}>🚃</div>
              <div>
                <div className="stat-big__num">15</div>
                <div className="stat-big__label">Types de trains</div>
              </div>
            </div>
          </div>

          {stats.par_vehicule?.length > 0 && (
            <div className="card live-stats__table">
              <h3>Répartition par type de train</h3>
              <div className="table-wrapper">
                <table>
                  <thead>
                    <tr>
                      <th scope="col">Type de train</th>
                      <th scope="col">Corridors</th>
                      <th scope="col">CO₂ économisé moy.</th>
                      <th scope="col">Substituables</th>
                      <th scope="col">Taux</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stats.par_vehicule.slice(0, 8).map(v => {
                      const pct = v.nb_trajets > 0 ? Math.round((v.nb_substituables / v.nb_trajets) * 100) : 0
                      return (
                        <tr key={v.label}>
                          <td><strong>{v.label}</strong></td>
                          <td>{v.nb_trajets?.toLocaleString('fr-FR')}</td>
                          <td>{v.co2_saved_moy_kg ?? '—'} kg</td>
                          <td>{v.nb_substituables?.toLocaleString('fr-FR')}</td>
                          <td>
                            <div className="taux-bar">
                              <div className="taux-bar__fill" style={{ width: `${pct}%` }} />
                              <span>{pct}%</span>
                            </div>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
              <div className="live-stats__table-footer">
                <Link to="/trajets" className="stats-link">Voir tous les trajets →</Link>
              </div>
            </div>
          )}
        </section>
      )}

      {/* ── Contexte & faits clés ─────────────────────────── */}
      <section className="facts" aria-labelledby="facts-title">
        <h2 id="facts-title" className="section-title">Contexte &amp; méthodologie</h2>
        <div className="facts__grid">
          {FACTS.map(f => (
            <div key={f.title} className="fact-card card">
              <div className="fact-card__icon">{f.icon}</div>
              <div>
                <h3 className="fact-card__title">{f.title}</h3>
                <p className="fact-card__body">{f.body}</p>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* ── Barre état API ─────────────────────────────────── */}
      <div className="health-bar" role="status" aria-label="État du service">
        <div className={'health-bar__dot ' + (apiOk ? 'dot-ok' : 'dot-down')} aria-hidden="true" />
        <span>
          API <strong>{health?.status ?? '…'}</strong>
          {' · '}
          Base de données <strong>{health?.db ? 'connectée' : health ? 'déconnectée' : '…'}</strong>
          {' · '}
          Version <strong>{health?.version ?? '…'}</strong>
        </span>
        <Link to="/monitoring" className="health-bar__link">Tableau de bord monitoring →</Link>
      </div>

    </main>
  )
}
