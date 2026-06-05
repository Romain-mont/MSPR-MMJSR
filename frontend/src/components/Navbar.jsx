import { NavLink } from 'react-router-dom'
import './Navbar.css'

const links = [
  { to: '/',           label: '🏠 Accueil' },
  { to: '/trajets',    label: '🚄 Trajets' },
  { to: '/prediction', label: '🌿 Prédiction IA' },
  { to: '/monitoring', label: '📊 Monitoring' },
]

export default function Navbar() {
  return (
    <header className="navbar" role="banner">
      <div className="navbar__brand">
        <span className="navbar__logo" aria-hidden="true">🚆</span>
        <span className="navbar__title">ObRail Europe</span>
      </div>
      <nav aria-label="Navigation principale">
        <ul className="navbar__links" role="list">
          {links.map(l => (
            <li key={l.to}>
              <NavLink
                to={l.to}
                end={l.to === '/'}
                className={({ isActive }) =>
                  'navbar__link' + (isActive ? ' navbar__link--active' : '')
                }
              >
                {l.label}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>
    </header>
  )
}
