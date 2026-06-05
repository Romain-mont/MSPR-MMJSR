import './StatCard.css'

export default function StatCard({ icon, label, value, unit, color }) {
  return (
    <article className="stat-card card" style={{ borderTop: `4px solid ${color || 'var(--primary)'}` }}>
      <div className="stat-card__icon" aria-hidden="true">{icon}</div>
      <div className="stat-card__body">
        <p className="stat-card__label">{label}</p>
        <p className="stat-card__value">
          {value ?? '—'}
          {unit && <span className="stat-card__unit"> {unit}</span>}
        </p>
      </div>
    </article>
  )
}
