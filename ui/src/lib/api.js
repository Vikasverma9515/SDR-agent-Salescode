// Base URL for API calls.
// In dev: empty (Vite proxy handles /api -> localhost:8080)
// In prod: set VITE_API_URL to your EC2 backend URL
const BASE = import.meta.env.VITE_API_URL || ''

export function apiUrl(path) {
  return `${BASE}${path}`
}

export function wsUrl(path) {
  const base = BASE || `${window.location.protocol}//${window.location.host}`
  const protocol = base.startsWith('https') ? 'wss:' : 'ws:'
  const host = base.replace(/^https?:\/\//, '')
  return `${protocol}//${host}${path}`
}
