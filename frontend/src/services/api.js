const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

async function request(path, options = {}) {
  const res = await fetch(`${BASE_URL}${path}`, options)
  if (!res.ok) throw new Error(`API ${path} → ${res.status}`)
  return res.json()
}

export const api = {
  health: () => request('/health'),
  stats:  () => request('/stats/volumes'),

  trajets: (params = {}) => {
    const q = new URLSearchParams()
    if (params.origine)      q.set('origine', params.origine)
    if (params.destination)  q.set('destination', params.destination)
    if (params.substituable !== undefined) q.set('substituable', params.substituable)
    if (params.limit)        q.set('limit', params.limit)
    return request(`/trajets?${q.toString()}`)
  },

  trajet: (id) => request(`/trajets/${id}`),

  predictSubstitution: (data) => request('/predict/substitution', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }),

  predictCO2: (data) => request('/predict/co2_saved', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }),
}
