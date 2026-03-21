import { useState, useCallback } from 'react'
import { apiUrl } from '../lib/api'
import LogStream from '../components/LogStream'

const ROLE_COLORS = {
  DM: 'bg-violet-900/50 text-violet-300 border-violet-700',
  Champion: 'bg-blue-900/50 text-blue-300 border-blue-700',
  Influencer: 'bg-amber-900/50 text-amber-300 border-amber-700',
  GateKeeper: 'bg-gray-800 text-gray-400 border-gray-700',
  Unknown: 'bg-gray-800 text-gray-400 border-gray-700',
}

const DEFAULT_DM_ROLES = 'VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales'

export default function SearcherPage() {
  const [companies, setCompanies] = useState('')
  const [dmRoles, setDmRoles] = useState(DEFAULT_DM_ROLES)
  const [running, setRunning] = useState(false)
  const [threadId, setThreadId] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)

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
    if (!companies.trim()) return

    setRunning(true)
    setResult(null)
    setError(null)

    try {
      const resp = await fetch(apiUrl('/api/searcher/run'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companies: companies.trim(),
          dm_roles: dmRoles.trim() || DEFAULT_DM_ROLES,
        }),
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
            <span className="text-[10px] font-mono text-violet-500/60 tracking-[0.2em] uppercase">Agent · 02</span>
          </div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Searcher</h1>
          <p className="text-gray-500 text-sm mt-1 font-mono">Contact Gap-Fill</p>
        </div>
        {running && (
          <div className="flex items-center gap-2 border border-violet-500/20 bg-violet-500/5 rounded px-3 py-1.5">
            <div className="relative w-2 h-2">
              <span className="absolute inset-0 rounded-full bg-violet-400 ping-slow" />
              <span className="relative block w-2 h-2 rounded-full bg-violet-400" />
            </div>
            <span className="text-xs font-mono text-violet-400">RUNNING</span>
          </div>
        )}
      </div>

      {/* Form */}
      <div className="panel mb-4">
        <div className="panel-header">
          <span className="text-xs font-mono text-gray-600 uppercase tracking-wider">Input</span>
        </div>
        <div className="p-5 space-y-4">
          {/* Companies */}
          <div>
            <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
              Companies <span className="text-gray-700 normal-case font-sans">(comma-separated)</span>
            </label>
            <input
              className="input-field w-full"
              placeholder="Company A, Company B"
              value={companies}
              onChange={e => setCompanies(e.target.value)}
              disabled={running}
            />
            <div className="text-[10px] font-mono text-gray-700 mt-1.5">
              Must already exist in Target Accounts (Fini must have run first)
            </div>
          </div>

          {/* DM roles */}
          <div>
            <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
              DM Roles to Gap-Fill <span className="text-gray-700 normal-case font-sans">(comma-separated)</span>
            </label>
            <input
              className="input-field w-full"
              value={dmRoles}
              onChange={e => setDmRoles(e.target.value)}
              disabled={running}
            />
            <div className="text-[10px] font-mono text-gray-700 mt-1.5">
              Roles already present in Final Filtered List are skipped automatically
            </div>
          </div>

          {/* Stack info */}
          <div className="border-t border-white/[0.05] pt-4">
            <div className="text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-2">Pipeline per company</div>
            <div className="space-y-1">
              {[
                ['01', 'Gap analysis', 'reads Target Accounts for org_id, domain, email_format · deduplicates against existing lists'],
                ['02', 'Unipile search', 'LinkedIn people search via org_id + region filter'],
                ['03', 'Company website', 'httpx scrape of leadership pages · GPT-5 fallback for 403/JS sites'],
                ['04', 'Filings search', 'DDG + Perplexity — annual reports, board filings'],
                ['05', 'Web + TheOrg', 'multi-source web search + org chart'],
                ['06', 'PDF sweep', 'press releases, org charts — Tavily + DDG'],
                ['07', 'Dedup', 'rapidfuzz ≥90% — merges dupes, removes already-in-sheet contacts'],
                ['08', 'LinkedIn validate', 'Unipile profile fetch — confirms current role + company'],
                ['09', 'Enrich + ZeroBounce', 'keyword role classify · email build · deliverability probe'],
                ['10', 'Write', 'appends new contacts to Searcher Output (A–H)'],
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
            disabled={running || !companies.trim()}
            className="btn-primary w-full justify-center"
          >
            {running ? (
              <>
                <svg className="animate-spin w-3.5 h-3.5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
                </svg>
                Gap-filling...
              </>
            ) : '→ Run Searcher'}
          </button>
        </div>
      </div>

      {threadId && <LogStream threadId={threadId} onEvent={handleEvent} />}

      {result && (
        <div className="mt-4 border border-violet-500/20 bg-violet-500/5 rounded p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-violet-400 font-mono text-xs">DONE</span>
            <span className="text-violet-300 text-sm font-medium">Searcher completed</span>
          </div>
          <div className="text-xs font-mono text-gray-400 mb-2">
            {result.contacts_appended ?? result.contacts_found ?? 0} contacts written to Searcher Output
          </div>

          {result.contacts?.length > 0 && (
            <div className="mt-3 space-y-1.5">
              {result.contacts.slice(0, 15).map((c, i) => (
                <div key={i} className="flex items-center gap-3 text-xs font-mono">
                  <span className="text-gray-300 flex-1 truncate">{c.full_name}</span>
                  <span className={`text-[10px] px-1.5 py-0.5 rounded border ${ROLE_COLORS[c.role_bucket] || ROLE_COLORS.Unknown}`}>
                    {c.role_bucket}
                  </span>
                  <span className="text-gray-600 truncate max-w-[140px]">{c.role_title || '—'}</span>
                  <span className={c.linkedin_verified ? 'text-violet-400' : 'text-gray-700'}>
                    {c.linkedin_verified ? 'LI✓' : 'LI✗'}
                  </span>
                </div>
              ))}
              {result.contacts.length > 15 && (
                <div className="text-[10px] font-mono text-gray-700 pt-1">
                  +{result.contacts.length - 15} more in sheet
                </div>
              )}
            </div>
          )}

          {result.errors?.length > 0 && (
            <div className="text-xs font-mono text-red-400 mt-2">{result.errors.length} errors</div>
          )}
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
