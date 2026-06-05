import { useState, useEffect, useRef } from 'react'
import { api } from '../services/api'
import BackButton from '../components/BackButton'
import './Prediction.css'

const VEHICULE_TYPES = [
  'Train Longue Distance', 'Train Longue Distance Nuit',
  'InterCity', 'EuroNight', 'Nightjet',
]

function getClusterInfo(distance, isSubstitutable) {
  if (!isSubstitutable) return { label: 'Longue distance (> 600km)', desc: 'Au-delà du seuil légal — pas de substitution viable.', color: '#e74c3c', icon: '🔴' }
  if (distance < 400)   return { label: 'Cluster 0 — Court trajet évident', desc: '100% des corridors similaires sont substituables. Service ferroviaire dense.', color: '#27ae60', icon: '🟢' }
  return { label: 'Cluster 1 — Zone grise (400–600km)', desc: 'Corridor autour du seuil légal. Substitution possible selon le service disponible.', color: '#f39c12', icon: '🟡' }
}

function StationAutocomplete({ id, label, value, onChange, onSelect, placeholder }) {
  const [suggestions, setSuggestions] = useState([])
  const [open, setOpen]     = useState(false)
  const [loading, setLoading] = useState(false)
  const timerRef = useRef(null)
  const wrapRef  = useRef(null)

  useEffect(() => {
    const fn = (e) => { if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', fn)
    return () => document.removeEventListener('mousedown', fn)
  }, [])

  const handleChange = (e) => {
    const v = e.target.value
    onChange(v)
    clearTimeout(timerRef.current)
    if (v.length < 2) { setSuggestions([]); setOpen(false); return }
    setLoading(true)
    timerRef.current = setTimeout(async () => {
      try {
        const param = id === 'origin' ? { origine: v, limit: 30 } : { destination: v, limit: 30 }
        const results = await api.trajets(param)
        const unique = [...new Set(results.map(t => id === 'origin' ? t.origine : t.destination))]
          .filter(s => s.toLowerCase().includes(v.toLowerCase())).slice(0, 8)
        setSuggestions(unique); setOpen(unique.length > 0)
      } catch { setSuggestions([]) }
      finally { setLoading(false) }
    }, 300)
  }

  return (
    <div className="autocomplete" ref={wrapRef}>
      <label htmlFor={id}>{label}</label>
      <div className="autocomplete__input-wrap">
        <input id={id} autoComplete="off" value={value} onChange={handleChange}
          onFocus={() => suggestions.length > 0 && setOpen(true)}
          placeholder={placeholder} aria-autocomplete="list" aria-expanded={open}
          aria-controls={`${id}-list`} />
        {loading && <span className="autocomplete__spinner" aria-hidden="true">⟳</span>}
      </div>
      {open && (
        <ul id={`${id}-list`} className="autocomplete__list" role="listbox">
          {suggestions.map(s => (
            <li key={s} role="option" className="autocomplete__item"
              onMouseDown={() => { onChange(s); onSelect(s); setOpen(false); setSuggestions([]) }}>
              🚉 {s}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

// Mini barre horizontale
function FeatureBar({ label, value, max, unit, color, highlight }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0
  return (
    <div className={`fbar ${highlight ? 'fbar--highlight' : ''}`}>
      <div className="fbar__header">
        <span className="fbar__label">{label}</span>
        <span className="fbar__value">{value != null ? `${value}${unit || ''}` : '—'}</span>
      </div>
      <div className="fbar__track">
        <div className="fbar__fill" style={{ width: `${pct}%`, background: color || 'var(--primary)' }} />
      </div>
    </div>
  )
}

export default function Prediction() {
  const [origin,       setOrigin]       = useState('')
  const [destination,  setDestination]  = useState('')
  const [distance,     setDistance]     = useState('')
  const [vehiculeType, setVehiculeType] = useState('Train Longue Distance')
  // Vol direct toujours supposé existant — l'enjeu est de remplacer un avion par le train
  const flightExists = true
  const [co2Avion,     setCo2Avion]     = useState('')
  const [corridorData, setCorridorData] = useState(null) // données DB du corridor

  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  const fetchCorridor = async (orig, dest) => {
    if (!orig || !dest) return
    try {
      const results = await api.trajets({ origine: orig, destination: dest, limit: 5 })
      if (results.length > 0) {
        const t = results[0]
        setDistance(t.distance_km?.toString() || '')
        if (t.vehicule_type) setVehiculeType(t.vehicule_type)
        setCorridorData(t)
      } else { setCorridorData(null) }
    } catch { setCorridorData(null) }
  }

  const handleOriginSelect  = (s) => { setOrigin(s);      fetchCorridor(s, destination) }
  const handleDestSelect    = (s) => { setDestination(s); fetchCorridor(origin, s) }

  const parseDistance = (val) => parseFloat(String(val).replace(',', '.'))

  const handleSubmit = async (e) => {
    e.preventDefault()
    const distVal = parseDistance(distance)
    if (!distance || isNaN(distVal) || distVal <= 0) { setError('La distance est requise.'); return }
    setLoading(true); setError(null); setResult(null)
    try {
      const payload = {
        origin: origin || 'Départ', destination: destination || 'Arrivée',
        distance_km: distVal, vehicule_type: vehiculeType,
        flight_exists: flightExists,
      }
      if (co2Avion) payload.co2_avion_kg = parseFloat(co2Avion)
      // Enrichir avec les données SNCF/GTFS de la DB si disponibles
      if (corridorData) {
        if (corridorData.origin_station_traffic) payload.origin_station_traffic = corridorData.origin_station_traffic
        if (corridorData.dest_station_traffic)   payload.dest_station_traffic   = corridorData.dest_station_traffic
        if (corridorData.trip_count_corridor)    payload.trip_count_corridor    = corridorData.trip_count_corridor
        if (corridorData.trip_count_origin)      payload.trip_count_origin      = corridorData.trip_count_origin
        if (corridorData.service_share)          payload.service_share          = corridorData.service_share
        if (corridorData.co2_train_kg)           payload.co2_train_kg           = corridorData.co2_train_kg
      }
      const data = await api.predictCO2(payload)
      setResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const reset = () => {
    setOrigin(''); setDestination(''); setDistance(''); setCo2Avion('')
    setVehiculeType('Train Longue Distance'); setFlightExists(true)
    setResult(null); setError(null); setCorridorData(null)
  }

  const co2Voiture   = result?.co2_saved_kg ? Math.round(result.co2_saved_kg / 0.147) : null
  const co2AvionUsed = result?.co2_avion_kg_used ?? null
  const cluster      = result ? getClusterInfo(parseDistance(distance), result.is_substitutable) : null

  // Données SNCF/GTFS pour l'affichage
  const traffic    = corridorData?.origin_station_traffic
  const tripCount  = corridorData?.trip_count_corridor
  const totalTrips = corridorData?.trip_count_origin
  const svcShare   = corridorData?.service_share

  // Facteurs pour "Pourquoi cette prédiction ?"
  const factors = result ? [
    {
      ok: parseDistance(distance) <= 600,
      label: `Distance : ${parseDistance(distance).toFixed(0)} km`,
      detail: parseDistance(distance) <= 600 ? `≤ 600km — dans le périmètre légal (loi française 2023)` : `> 600km — hors périmètre de substitution`,
    },
    {
      ok: flightExists,
      label: `Vol direct : ${flightExists ? 'existant' : 'absent'}`,
      detail: flightExists ? 'Un vol direct existe sur ce corridor — substitution pertinente' : 'Pas de vol direct — substitution non applicable',
    },
    ...(traffic ? [{
      ok: true,
      label: `Fréquentation gare : ${traffic >= 1e6 ? (traffic/1e6).toFixed(1)+'M' : Math.round(traffic/1000)+'k'} voyageurs/an`,
      detail: 'Donnée SNCF réelle — mesure l\'attractivité de la gare de départ',
      highlight: true,
    }] : []),
    ...(tripCount ? [{
      ok: tripCount >= 5,
      label: `Service sur ce corridor : ${tripCount} trains/semaine`,
      detail: svcShare ? `soit ${(svcShare * 100).toFixed(1)}% du service total de la gare (service_share)` : '',
      highlight: true,
    }] : []),
  ] : []

  return (
    <main className="pred" id="main-content">
      <div className="pred__header">
        <BackButton />
        <h1>Analyser un corridor</h1>
        <p>L'IA prédit si un vol peut être remplacé par le train et calcule le CO₂ économisé.</p>
      </div>

      <div className="pred__layout">

        {/* ── Formulaire ── */}
        <div className="pred__left">
          <form className="card pred__form" onSubmit={handleSubmit}>
            <div className="pred__stations">
              <StationAutocomplete id="origin" label="Gare de départ 🛫"
                value={origin} onChange={setOrigin} onSelect={handleOriginSelect}
                placeholder="ex: Paris Gare de Lyon" />
              <div className="pred__arrow" aria-hidden="true">→</div>
              <StationAutocomplete id="dest" label="Gare d'arrivée 🛬"
                value={destination} onChange={setDestination} onSelect={handleDestSelect}
                placeholder="ex: Marseille Saint-Charles" />
            </div>

            {/* ── Option 1 : données SNCF/GTFS chargées ── */}
            {corridorData && (
              <div className="pred__sncf-data" role="status">
                <div className="pred__sncf-title">📡 Données réelles chargées depuis la base</div>
                <div className="pred__sncf-grid">
                  {traffic && (
                    <div className="pred__sncf-item">
                      <span className="pred__sncf-label">Fréquentation SNCF</span>
                      <span className="pred__sncf-value">
                        {traffic >= 1e6 ? (traffic/1e6).toFixed(1)+'M' : Math.round(traffic/1000)+'k'} voyageurs/an
                      </span>
                    </div>
                  )}
                  {tripCount && (
                    <div className="pred__sncf-item pred__sncf-item--highlight">
                      <span className="pred__sncf-label">Trains/semaine (corridor)</span>
                      <span className="pred__sncf-value">{tripCount}</span>
                    </div>
                  )}
                  {svcShare && (
                    <div className="pred__sncf-item pred__sncf-item--highlight">
                      <span className="pred__sncf-label">Part du service gare</span>
                      <span className="pred__sncf-value">{(svcShare * 100).toFixed(1)}%</span>
                    </div>
                  )}
                  {totalTrips && (
                    <div className="pred__sncf-item">
                      <span className="pred__sncf-label">Total trains/sem gare</span>
                      <span className="pred__sncf-value">{Math.round(totalTrips)}</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            <div className="pred__fields">
              <div>
                <label htmlFor="distance">Distance (km) *</label>
                <input id="distance" type="text" inputMode="decimal" required
                  value={distance} onChange={e => setDistance(e.target.value)} placeholder="ex: 450" />
              </div>
              {!corridorData && (
                <div>
                  <label htmlFor="vtype">Type de train *</label>
                  <select id="vtype" value={vehiculeType} onChange={e => setVehiculeType(e.target.value)}>
                    {VEHICULE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
              )}
              <div>
                <label htmlFor="co2avion">CO₂ avion (kg) — optionnel</label>
                <input id="co2avion" type="number" step="0.1"
                  value={co2Avion} onChange={e => setCo2Avion(e.target.value)} placeholder="estimé si vide" />
              </div>
            </div>

            {error && <div className="pred__error" role="alert">⚠️ {error}</div>}
            <div className="pred__actions">
              <button type="submit" className="btn-primary pred__submit" disabled={loading}>
                {loading ? 'Analyse en cours…' : '🔍 Analyser le corridor'}
              </button>
              {result && <button type="button" className="btn-outline" onClick={reset}>Réinitialiser</button>}
            </div>
          </form>

          {/* ── Option 3 : mini graphique features ── */}
          {result && corridorData && (
            <div className="card pred__features" aria-label="Profil des données utilisées par le modèle">
              <h3>📊 Données utilisées par le modèle</h3>
              <FeatureBar label="Distance" value={parseDistance(distance).toFixed(0)}
                max={800} unit=" km" color={parseDistance(distance) <= 600 ? '#27ae60' : '#e74c3c'} />
              {traffic && <FeatureBar label="Fréquentation gare départ"
                value={traffic >= 1e6 ? (traffic/1e6).toFixed(1)+'M' : Math.round(traffic/1000)+'k'}
                max={100} unit="" color="#2980b9" highlight />}
              {svcShare && <FeatureBar label="Part du service (service_share)"
                value={(svcShare * 100).toFixed(1)} max={100} unit="%" color="#8e44ad" highlight />}
              {tripCount && <FeatureBar label="Trains/semaine sur ce corridor"
                value={tripCount} max={200} unit="" color="#e67e22" highlight />}
              <p className="pred__features-note">
                Les données en <strong>violet/orange</strong> proviennent des fichiers GTFS (horaires réels) et SNCF (fréquentation) — elles enrichissent la prédiction au-delà de la simple distance.
              </p>
            </div>
          )}
        </div>

        {/* ── Résultat ── */}
        {result && (
          <div className="pred__result-col" aria-live="polite">

            <div className={`card pred__verdict ${result.is_substitutable ? 'verdict-yes' : 'verdict-no'}`}>
              <div className="pred__verdict-icon">{result.is_substitutable ? '✅' : '❌'}</div>
              <div>
                <h2>{result.is_substitutable ? 'Substitution possible' : 'Substitution non recommandée'}</h2>
                <p className="pred__route">{result.origin} → {result.destination} · {parseDistance(distance).toFixed(0)} km</p>
                <p className="pred__confidence">Confiance du modèle : <strong>{(result.proba_substitutable * 100).toFixed(1)}%</strong></p>
              </div>
            </div>

            {result.co2_saved_kg && (
              <div className="card pred__co2">
                <h3>Comparaison CO₂ / passager</h3>
                <div className="pred__co2-bars">
                  <div className="pred__co2-row">
                    <span className="pred__co2-label">🛫 Avion</span>
                    <div className="pred__co2-bar-wrap">
                      <div className="pred__co2-bar pred__co2-bar--avion" style={{ width: '100%' }}>
                        {co2AvionUsed?.toFixed(1)} kg
                      </div>
                    </div>
                  </div>
                  <div className="pred__co2-row">
                    <span className="pred__co2-label">🚄 Train</span>
                    <div className="pred__co2-bar-wrap">
                      <div className="pred__co2-bar pred__co2-bar--train"
                        style={{ width: `${Math.max(4, ((co2AvionUsed - result.co2_saved_kg) / co2AvionUsed) * 100)}%` }}>
                        {(co2AvionUsed - result.co2_saved_kg).toFixed(1)} kg
                      </div>
                    </div>
                  </div>
                </div>
                <div className="pred__co2-saving">
                  <span className="pred__co2-saving-value">💚 {result.co2_saved_kg} kg économisés</span>
                  <span className="pred__co2-saving-equiv">≈ {co2Voiture} km en voiture évités</span>
                </div>
                {result.co2_avion_estimated && <p className="pred__note">* CO₂ avion estimé via formule EcoPassenger</p>}
              </div>
            )}

            {/* ── Option 2 : Pourquoi cette prédiction ? ── */}
            <div className="card pred__why">
              <h3>🔍 Pourquoi cette prédiction ?</h3>
              <ul className="pred__why-list">
                {factors.map((f, i) => (
                  <li key={i} className={`pred__why-item ${f.ok ? 'why-ok' : 'why-no'} ${f.highlight ? 'why-highlight' : ''}`}>
                    <span className="pred__why-icon">{f.highlight ? '📡' : f.ok ? '✅' : '❌'}</span>
                    <div>
                      <strong>{f.label}</strong>
                      {f.detail && <p className="pred__why-detail">{f.detail}</p>}
                    </div>
                  </li>
                ))}
              </ul>
              {svcShare && (
                <p className="pred__why-footer">
                  Le <code>service_share</code> ({(svcShare*100).toFixed(1)}%) mesure la part du service hebdomadaire de la gare dédiée à ce corridor. Plus il est élevé, plus le train est une alternative crédible à l'avion.
                </p>
              )}
            </div>

            <div className="card pred__cluster" style={{ borderLeft: `5px solid ${cluster.color}` }}>
              <h3>Profil du corridor</h3>
              <p className="pred__cluster-label" style={{ color: cluster.color }}>{cluster.icon} {cluster.label}</p>
              <p className="pred__cluster-desc">{cluster.desc}</p>
              <p className="pred__cluster-source">K-Means k=3 · Silhouette=0.640 · 46k corridors français</p>
            </div>

            <p className="pred__latency">⚡ {result.latency_ms} ms</p>
          </div>
        )}
      </div>
    </main>
  )
}
