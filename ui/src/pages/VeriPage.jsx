import { useState, useCallback } from 'react'
import LogStream from '../components/LogStream'

export default function VeriPage() {
  const [running, setRunning] = useState(false)
  const [threadId, setThreadId] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [rowStart, setRowStart] = useState('')
  const [rowEnd, setRowEnd] = useState('')

  const handleEvent = useCallback((msg) => {
    if (msg.type === 'completed') {
      setRunning(false)
      setResult(msg.data)
    } else if (msg.type === 'error') {
      setRunning(false)
      setError(msg.data.error)
    }
  }, [])

  const handleRun = async () => {
    setRunning(true)
    setResult(null)
    setError(null)

    const body = {}
    if (rowStart) body.row_start = parseInt(rowStart)
    if (rowEnd) body.row_end = parseInt(rowEnd)

    try {
      const resp = await fetch('/api/veri/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await resp.json()
      setThreadId(data.thread_id)
    } catch (e) {
      setError(e.message)
      setRunning(false)
    }
  }

  return (
    <div className="p-8 max-w-2xl mx-auto">
      {/* Header */}
      <div className="mb-8 flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="text-[10px] font-mono text-emerald-500/60 tracking-[0.2em] uppercase">Agent · 03</span>
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Veri</h1>
          <p className="text-gray-500 text-sm mt-1 font-mono">Contact QC</p>
        </div>
        {running && (
          <div className="flex items-center gap-2 border border-emerald-500/20 bg-emerald-500/5 rounded px-3 py-1.5">
            <div className="relative w-2 h-2">
              <span className="absolute inset-0 rounded-full bg-emerald-400 ping-slow" />
              <span className="relative block w-2 h-2 rounded-full bg-emerald-400" />
            </div>
            <span className="text-xs font-mono text-emerald-400">RUNNING</span>
          </div>
        )}
      </div>

      {/* Form */}
      <div className="panel mb-4">
        <div className="panel-header">
          <span className="text-xs font-mono text-gray-600 uppercase tracking-wider">Input</span>
        </div>
        <div className="p-5 space-y-4">
          {/* Row range */}
          <div>
            <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
              Row Range <span className="text-gray-700 normal-case font-sans">(optional — blank runs all pending)</span>
            </label>
            <div className="flex gap-3 items-center">
              <input
                className="input-field flex-1"
                placeholder="From row  e.g. 2"
                type="number"
                min="1"
                value={rowStart}
                onChange={e => setRowStart(e.target.value)}
                disabled={running}
              />
              <span className="text-gray-600 font-mono">→</span>
              <input
                className="input-field flex-1"
                placeholder="To row  e.g. 50"
                type="number"
                min="1"
                value={rowEnd}
                onChange={e => setRowEnd(e.target.value)}
                disabled={running}
              />
            </div>
            <div className="text-[10px] font-mono text-gray-700 mt-1.5">
              Header = row 1 · data starts at row 2 · matches First Clean List sheet
            </div>
          </div>

          {/* Stack info */}
          <div className="border-t border-white/[0.05] pt-4">
            <div className="text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-2">6-step stack per contact</div>
            <div className="space-y-1">
              {[
                ['01', 'DDG sweep', 'name + company, name + role'],
                ['02', 'TheOrg check', 'org chart confirmation'],
                ['03', 'Tavily fallback', 'if DDG + TheOrg inconclusive'],
                ['04', 'Perplexity deep', 'conflict resolution'],
                ['05', 'LinkedIn audit', 'opens profile in Brave — role, tenure, connections'],
                ['06', 'ZeroBounce', 'email deliverability (skipped if already validated)'],
              ].map(([n, label, detail]) => (
                <div key={n} className="flex items-baseline gap-3 text-xs font-mono">
                  <span className="text-gray-700 w-5 shrink-0">{n}</span>
                  <span className="text-gray-400 w-36 shrink-0">{label}</span>
                  <span className="text-gray-600">{detail}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="px-5 pb-5">
          <button
            onClick={handleRun}
            disabled={running}
            className="btn-primary w-full justify-center"
          >
            {running ? (
              <>
                <svg className="animate-spin w-3.5 h-3.5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
                </svg>
                Verifying...
              </>
            ) : '→ Run Veri'}
          </button>
        </div>
      </div>

      {threadId && <LogStream threadId={threadId} onEvent={handleEvent} />}

      {result && (
        <div className="mt-4 border border-emerald-500/20 bg-emerald-500/5 rounded p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-emerald-400 font-mono text-xs">DONE</span>
            <span className="text-emerald-300 text-sm font-medium">Veri completed</span>
          </div>
          <div className="grid grid-cols-3 gap-3 mb-3">
            <div className="bg-emerald-500/10 rounded p-3 text-center">
              <div className="text-2xl font-bold text-emerald-300 font-mono">{result.verified}</div>
              <div className="text-[10px] font-mono text-emerald-600 mt-1 uppercase tracking-wider">Verified</div>
            </div>
            <div className="bg-amber-500/10 rounded p-3 text-center">
              <div className="text-2xl font-bold text-amber-300 font-mono">{result.review}</div>
              <div className="text-[10px] font-mono text-amber-600 mt-1 uppercase tracking-wider">Review</div>
            </div>
            <div className="bg-red-500/10 rounded p-3 text-center">
              <div className="text-2xl font-bold text-red-300 font-mono">{result.rejected}</div>
              <div className="text-[10px] font-mono text-red-600 mt-1 uppercase tracking-wider">Rejected</div>
            </div>
          </div>
          {result.errors?.length > 0 && (
            <div className="text-xs font-mono text-red-400 mb-2">{result.errors.length} errors</div>
          )}
          <div className="text-xs font-mono text-emerald-600">→ cols O–U written back to Final Filtered List (LinkedIn status, employment, title match, overall verdict, notes, timestamp)</div>
        </div>
      )}

      {error && (
        <div className="mt-4 border border-red-500/20 bg-red-500/5 rounded p-4">
          <div className="text-xs font-mono text-red-400 mb-1">ERROR</div>
          <div className="text-sm text-red-300 font-mono">{error}</div>
        </div>
      )}
    </div>
  )
}
