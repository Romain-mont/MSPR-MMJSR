import { Link } from 'react-router-dom'

export default function BackButton({ to = '/', label = '← Accueil' }) {
  return (
    <Link to={to} className="back-btn" aria-label={`Retour : ${label}`}>
      {label}
    </Link>
  )
}
