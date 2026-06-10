import { NavLink } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { api } from '../services/api'
import './Navbar.css'

const links = [
  { to: '/',           icon: '🏠', label: 'Accueil' },
  { to: '/trajets',    icon: '🚄', label: 'Trajets' },
  { to: '/prediction', icon: '🌿', label: 'Prédiction IA' },
  { to: '/monitoring', icon: '📊', label: 'Monitoring' },
]

export default function Navbar() {
  const [apiOk, setApiOk] = useState(null)

  useEffect(() => {
    api.health()
      .then(h => setApiOk(h?.status === 'ok'))
      .catch(() => setApiOk(false))
  }, [])

  return (
    <header className="navbar" role="banner">
      <NavLink to="/" className="navbar__brand" aria-label="ObRail Europe — Accueil">
        <span className="navbar__logo" aria-hidden="true">🚆</span>
        <div>
          <span>ObRail Europe</span>
          <span className="navbar__subtitle">Observatoire ferroviaire CO₂</span>
        </div>
      </NavLink>

      <nav aria-label="Navigation principale">
        <ul className="navbar__links" role="list">
          {links.map(l => (
            <li key={l.to}>
              <NavLink
                to={l.to}
                end={l.to === '/'}
                className={({ isActive }) => 'navbar__link' + (isActive ? ' navbar__link--active' : '')}
              >
                <span className="navbar__icon" aria-hidden="true">{l.icon}</span>
                <span>{l.label}</span>
              </NavLink>
            </li>
          ))}
          {apiOk !== null && (
            <li style={{ display: 'flex', alignItems: 'center', paddingLeft: '.5rem' }}>
              <div
                className={'navbar__status' + (apiOk ? '' : ' navbar__status--down')}
                title={apiOk ? 'API opérationnelle' : 'API indisponible'}
                aria-label={apiOk ? 'API opérationnelle' : 'API indisponible'}
              />
            </li>
          )}
        </ul>
      </nav>
    </header>
  )
}
