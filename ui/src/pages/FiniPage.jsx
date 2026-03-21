import { useState, useCallback, useRef, useEffect } from 'react'
import LogStream from '../components/LogStream'
import ConfirmationModal from '../components/ConfirmationModal'

const REGIONS = [
  // Continents
  { label: 'Africa', value: 'Africa' },
  { label: 'Asia', value: 'Asia' },
  { label: 'Europe', value: 'Europe' },
  { label: 'North America', value: 'North America' },
  { label: 'South America', value: 'South America' },
  // Countries A-Z
  { label: 'Argentina', value: 'Argentina' },
  { label: 'Australia', value: 'Australia' },
  { label: 'Belgium', value: 'Belgium' },
  { label: 'Brazil', value: 'Brazil' },
  { label: 'Cambodia', value: 'Cambodia' },
  { label: 'Canada', value: 'Canada' },
  { label: 'Chile', value: 'Chile' },
  { label: 'China', value: 'China' },
  { label: 'Colombia', value: 'Colombia' },
  { label: 'Egypt', value: 'Egypt' },
  { label: 'England', value: 'England' },
  { label: 'Estonia', value: 'Estonia' },
  { label: 'France', value: 'France' },
  { label: 'Germany', value: 'Germany' },
  { label: 'Greece', value: 'Greece' },
  { label: 'India', value: 'India' },
  { label: 'Indonesia', value: 'Indonesia' },
  { label: 'Israel', value: 'Israel' },
  { label: 'Italy', value: 'Italy' },
  { label: 'Japan', value: 'Japan' },
  { label: 'Jordan', value: 'Jordan' },
  { label: 'Kuwait', value: 'Kuwait' },
  { label: 'Latvia', value: 'Latvia' },
  { label: 'Malaysia', value: 'Malaysia' },
  { label: 'Mexico', value: 'Mexico' },
  { label: 'Myanmar', value: 'Myanmar' },
  { label: 'Nepal', value: 'Nepal' },
  { label: 'Netherlands', value: 'Netherlands' },
  { label: 'Nigeria', value: 'Nigeria' },
  { label: 'Oman', value: 'Oman' },
  { label: 'Philippines', value: 'Philippines' },
  { label: 'Qatar', value: 'Qatar' },
  { label: 'Russia', value: 'Russia' },
  { label: 'Saudi Arabia', value: 'Saudi Arabia' },
  { label: 'Singapore', value: 'Singapore' },
  { label: 'South Korea', value: 'South Korea' },
  { label: 'Spain', value: 'Spain' },
  { label: 'Sweden', value: 'Sweden' },
  { label: 'Switzerland', value: 'Switzerland' },
  { label: 'Thailand', value: 'Thailand' },
  { label: 'Turkey', value: 'Turkey' },
  { label: 'United Arab Emirates', value: 'United Arab Emirates' },
  { label: 'United States', value: 'United States' },
  { label: 'Vietnam', value: 'Vietnam' },
]

function RegionSelect({ value, onChange, disabled }) {
  const [search, setSearch] = useState('')
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  const filtered = REGIONS.filter(r =>
    r.label.toLowerCase().includes(search.toLowerCase())
  )
  const isKnown = REGIONS.some(r => r.value.toLowerCase() === value.toLowerCase())
  const showWarning = value && !isKnown

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const select = (val) => { onChange(val); setSearch(''); setOpen(false) }
  const clear = () => { onChange(''); setSearch(''); setOpen(false) }

  return (
    <div ref={ref} className="relative">
      <div
        className={`input-field flex items-center justify-between cursor-pointer ${disabled ? 'opacity-50 pointer-events-none' : ''}`}
        onClick={() => !disabled && setOpen(v => !v)}
      >
        <span className={value ? 'text-gray-200' : 'text-gray-600'}>
          {value || 'Select region…'}
        </span>
        <div className="flex items-center gap-1">
          {value && (
            <button
              onClick={e => { e.stopPropagation(); clear() }}
              className="text-gray-600 hover:text-gray-400 text-xs px-1"
            >✕</button>
          )}
          <span className="text-gray-600 text-xs">▾</span>
        </div>
      </div>

      {showWarning && (
        <div className="mt-1 text-[10px] font-mono text-yellow-500/80">
          ⚠ Region not in list — URL will be generated without region filter
        </div>
      )}

      {open && (
        <div className="absolute z-50 top-full mt-1 w-full bg-[#111] border border-white/10 rounded shadow-xl">
          <div className="p-2 border-b border-white/10">
            <input
              autoFocus
              className="w-full bg-transparent text-sm text-gray-300 placeholder-gray-600 outline-none"
              placeholder="Search…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              onClick={e => e.stopPropagation()}
            />
          </div>
          <div className="max-h-52 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-xs text-gray-600 font-mono">
                Not found — type to use as custom value
                <button
                  className="block mt-1 text-yellow-500/80 hover:text-yellow-400"
                  onClick={() => select(search)}
                >Use "{search}"</button>
              </div>
            ) : (
              filtered.map(r => (
                <div
                  key={r.value}
                  className={`px-3 py-2 text-sm cursor-pointer hover:bg-white/5 ${value === r.value ? 'text-blue-400' : 'text-gray-300'}`}
                  onClick={() => select(r.value)}
                >
                  {r.label}
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export default function FiniPage() {
  const [companies, setCompanies] = useState('')
  const [sdr, setSdr] = useState('')
  const [region, setRegion] = useState('')
  const [submitN8n, setSubmitN8n] = useState(false)
  const [running, setRunning] = useState(false)
  const [threadId, setThreadId] = useState(null)
  const [pendingConfirmation, setPendingConfirmation] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)

  const handleEvent = useCallback((msg) => {
    if (msg.type === 'confirmation_required') setPendingConfirmation(msg.data)
    else if (msg.type === 'completed') { setRunning(false); setResult(msg.data) }
    else if (msg.type === 'error')     { setRunning(false); setError(msg.data.error) }
  }, [])

  const handleRun = async () => {
    if (!companies.trim()) return
    setRunning(true); setResult(null); setError(null); setPendingConfirmation(null)
    try {
      const resp = await fetch('/api/fini/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ companies: companies.trim(), sdr: sdr.trim(), region: region.trim(), submit_n8n: submitN8n }),
      })
      const data = await resp.json()
      setThreadId(data.thread_id)
    } catch (e) { setError(e.message); setRunning(false) }
  }

  const companyList = companies.split(',').map(c => c.trim()).filter(Boolean)

  return (
    <div className="p-8 max-w-2xl mx-auto">
      {/* Header */}
      <div className="mb-8 flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="text-[10px] font-mono text-blue-500/60 tracking-[0.2em] uppercase">Agent · 01</span>
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Fini</h1>
          <p className="text-gray-500 text-sm mt-1 font-mono">Target Builder</p>
        </div>
        {running && (
          <div className="flex items-center gap-2 border border-blue-500/20 bg-blue-500/5 rounded px-3 py-1.5">
            <div className="relative w-2 h-2">
              <span className="absolute inset-0 rounded-full bg-blue-400 ping-slow" />
              <span className="relative block w-2 h-2 rounded-full bg-blue-400" />
            </div>
            <span className="text-xs font-mono text-blue-400">RUNNING</span>
          </div>
        )}
      </div>

      {/* Form */}
      <div className="panel mb-4">
        <div className="panel-header">
          <span className="text-xs font-mono text-gray-600 uppercase tracking-wider">Input</span>
        </div>
        <div className="p-5 space-y-4">
          <div>
            <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
              Companies <span className="text-blue-500">*</span>
            </label>
            <textarea
              className="input-field resize-none h-20"
              placeholder="Company A, Company B, Company C..."
              value={companies}
              onChange={e => setCompanies(e.target.value)}
              disabled={running}
            />
            {companyList.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {companyList.map(c => (
                  <span key={c} className="bg-white/5 border border-white/10 text-gray-400 text-[10px] font-mono px-2 py-0.5 rounded">
                    {c}
                  </span>
                ))}
              </div>
            )}
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">SDR Assigned</label>
              <input
                className="input-field"
                placeholder="SDR name"
                value={sdr}
                onChange={e => setSdr(e.target.value)}
                disabled={running}
              />
            </div>
            <div>
              <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
                Region <span className="text-blue-500">*</span>
              </label>
              <RegionSelect value={region} onChange={setRegion} disabled={running} />
            </div>
          </div>

          <div className="flex items-center justify-between py-3 border-t border-white/[0.05]">
            <div>
              <div className="text-sm text-gray-300">Submit to n8n</div>
              <div className="text-[10px] font-mono text-gray-700 mt-0.5">POST webhook after write · 60s delay/company</div>
            </div>
            <button
              onClick={() => setSubmitN8n(v => !v)}
              disabled={running}
              className={`relative w-10 h-5 rounded-full transition-colors duration-200 ${submitN8n ? 'bg-blue-600' : 'bg-white/10'}`}
            >
              <span className={`absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-all duration-200 ${submitN8n ? 'left-5' : 'left-0.5'}`} />
            </button>
          </div>
        </div>

        <div className="px-5 pb-5">
          <button
            onClick={handleRun}
            disabled={running || !companies.trim()}
            className="btn-primary w-full justify-center"
          >
            {running ? (
              <>
                <svg className="animate-spin w-3.5 h-3.5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
                </svg>
                Processing...
              </>
            ) : '→ Run Fini'}
          </button>
        </div>
      </div>

      {/* Log stream */}
      {threadId && <LogStream threadId={threadId} onEvent={handleEvent} />}

      {/* Result */}
      {result && (
        <div className="mt-4 border border-emerald-500/20 bg-emerald-500/5 rounded p-4">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-emerald-400 font-mono text-xs">DONE</span>
            <span className="text-emerald-300 text-sm font-medium">Fini completed</span>
          </div>
          <div className="font-mono text-xs text-gray-500 space-y-1">
            <div>companies_processed <span className="text-gray-300">{result.companies_processed}</span></div>
            {result.errors?.length > 0 && <div>errors <span className="text-red-400">{result.errors.length}</span></div>}
          </div>
          <div className="mt-2 text-xs text-emerald-500 font-mono">→ Target Accounts sheet updated</div>
        </div>
      )}

      {error && (
        <div className="mt-4 border border-red-500/20 bg-red-500/5 rounded p-4">
          <div className="text-xs font-mono text-red-400 mb-1">ERROR</div>
          <div className="text-sm text-red-300 font-mono">{error}</div>
        </div>
      )}

      {pendingConfirmation && (
        <ConfirmationModal data={pendingConfirmation} threadId={threadId} onClose={() => setPendingConfirmation(null)} />
      )}
    </div>
  )
}
