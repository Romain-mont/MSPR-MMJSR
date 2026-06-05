import { useEffect, useState } from 'react'
import { api } from '../services/api'
import BackButton from '../components/BackButton'
import './Trajets.css'

export default function Trajets() {
  const [trajets, setTrajets]       = useState([])
  const [loading, setLoading]       = useState(false)
  const [error, setError]           = useState(null)
  const [filters, setFilters]       = useState({ origine: '', destination: '', substituable: '' })

  const load = (f = filters) => {
    setLoading(true)
    setError(null)
    const params = { limit: 100 }
    if (f.origine)      params.origine      = f.origine
    if (f.destination)  params.destination  = f.destination
    if (f.substituable !== '') params.substituable = f.substituable === 'true'
    api.trajets(params)
      .then(setTrajets)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  const handleChange = e => setFilters(p => ({ ...p, [e.target.name]: e.target.value }))

  const handleSubmit = e => { e.preventDefault(); load() }

  const handleReset = () => {
    const f = { origine: '', destination: '', substituable: '' }
    setFilters(f)
    load(f)
  }

  return (
    <main className="trajets" id="main-content">
      <div className="trajets__topbar">
        <BackButton />
        <h1>Trajets ferroviaires</h1>
      </div>

      <form className="trajets__filters card" onSubmit={handleSubmit} aria-label="Filtres de recherche">
        <div className="trajets__filters-grid">
          <div>
            <label htmlFor="origine">Gare de départ</label>
            <input id="origine" name="origine" value={filters.origine}
              onChange={handleChange} placeholder="ex: Paris" />
          </div>
          <div>
            <label htmlFor="destination">Gare d'arrivée</label>
            <input id="destination" name="destination" value={filters.destination}
              onChange={handleChange} placeholder="ex: Lyon" />
          </div>
          <div>
            <label htmlFor="substituable">Substituabilité</label>
            <select id="substituable" name="substituable" value={filters.substituable} onChange={handleChange}>
              <option value="">Tous</option>
              <option value="true">Substituables ✅</option>
              <option value="false">Non substituables ❌</option>
            </select>
          </div>
        </div>
        <div className="trajets__filters-actions">
          <button type="submit" className="btn-primary">Rechercher</button>
          <button type="button" className="btn-outline" onClick={handleReset}>Réinitialiser</button>
        </div>
      </form>

      {error && <div className="trajets__error" role="alert">⚠️ {error}</div>}

      {loading
        ? <p className="trajets__loading" aria-live="polite">Chargement…</p>
        : (
          <section className="card" aria-label={`${trajets.length} trajets trouvés`}>
            <p className="trajets__count">{trajets.length} résultat{trajets.length > 1 ? 's' : ''}</p>
            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Origine</th>
                    <th scope="col">Destination</th>
                    <th scope="col">Distance</th>
                    <th scope="col">Type de train</th>
                    <th scope="col">CO₂ économisé</th>
                    <th scope="col">Substituable</th>
                  </tr>
                </thead>
                <tbody>
                  {trajets.length === 0
                    ? <tr><td colSpan={6} className="trajets__empty">Aucun résultat</td></tr>
                    : trajets.map(t => (
                      <tr key={t.id}>
                        <td>{t.origine}</td>
                        <td>{t.destination}</td>
                        <td>{t.distance_km ? `${t.distance_km} km` : '—'}</td>
                        <td>{t.vehicule_type}</td>
                        <td>{t.co2_saved_kg ? `${t.co2_saved_kg} kg` : '—'}</td>
                        <td>
                          <span className={`badge ${t.is_substitutable === 1 ? 'badge-green' : 'badge-red'}`}>
                            {t.is_substitutable === 1 ? '✅ Oui' : '❌ Non'}
                          </span>
                        </td>
                      </tr>
                    ))
                  }
                </tbody>
              </table>
            </div>
          </section>
        )
      }
    </main>
  )
}
