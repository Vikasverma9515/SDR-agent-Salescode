import { useState, useEffect } from 'react'
import { apiUrl } from '../lib/api'

const AGENTS = [
  {
    id: 'fini', tag: '01', name: 'Fini', role: 'Target Builder',
    desc: 'Enriches company names with LinkedIn org ID, domain, and email format. Writes to Target Accounts after operator review.',
    color: 'blue', action: 'fini',
    output: 'Target Accounts sheet',
  },
  {
    id: 'searcher', tag: '02', name: 'Searcher', role: 'Contact Discovery',
    desc: 'Multi-source contact discovery via web, filings, TheOrg. Validates LinkedIn profiles and constructs verified emails.',
    color: 'violet', action: 'searcher',
    output: 'Searcher Output sheet',
  },
  {
    id: 'veri', tag: '03', name: 'Veri', role: 'Contact QC',
    desc: '6-step verification stack: DDG → TheOrg → Tavily → Perplexity → LinkedIn → ZeroBounce. Labels VERIFIED / REVIEW / REJECT.',
    color: 'emerald', action: 'veri',
    output: 'Final Filtered List',
  },
]

const C = {
  blue:   { accent: 'text-blue-400',   border: 'border-blue-500/20',   bg: 'bg-blue-500/5',   glow: 'rgba(59,130,246,0.15)'  },
  violet: { accent: 'text-violet-400', border: 'border-violet-500/20', bg: 'bg-violet-500/5', glow: 'rgba(139,92,246,0.15)' },
  emerald:{ accent: 'text-emerald-400',border: 'border-emerald-500/20',bg: 'bg-emerald-500/5',glow: 'rgba(16,185,129,0.15)'  },
}

export default function DashboardPage({ onNavigate }) {
  const [config, setConfig] = useState(null)
  const [time, setTime] = useState(new Date())

  useEffect(() => {
    fetch(apiUrl('/api/config/check')).then(r => r.json()).then(setConfig).catch(() => {})
    const t = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(t)
  }, [])

  const missing = config ? Object.entries(config).filter(([k,v]) => !v && k !== 'chrome_cdp').map(([k]) => k) : []

  return (
    <div className="p-8 max-w-5xl mx-auto">
      {/* Top bar */}
      <div className="flex items-start justify-between mb-10">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="text-[10px] font-mono text-blue-500/70 tracking-[0.2em] uppercase">SalesCode.ai</span>
            <span className="text-[10px] font-mono text-gray-700">·</span>
            <span className="text-[10px] font-mono text-gray-700 tracking-widest uppercase">Mapping Pipeline</span>
          </div>
          <h1 className="text-4xl font-bold text-white tracking-tight">
            Pipeline <span className="text-blue-500">Overview</span>
          </h1>
          <p className="text-gray-500 mt-2 text-sm max-w-xl">
            Three-agent B2B prospecting system for FMCG/CPG. Discover, enrich, and verify contacts across Google Sheets.
          </p>
        </div>
        <div className="text-right font-mono">
          <div className="text-2xl text-white tabular-nums">{time.toLocaleTimeString()}</div>
          <div className="text-xs text-gray-600">{time.toLocaleDateString('en-US', { weekday:'short', month:'short', day:'numeric' })}</div>
        </div>
      </div>

      {/* Missing config */}
      {missing.length > 0 && (
        <div className="mb-6 border border-amber-500/20 bg-amber-500/5 rounded px-4 py-3 flex items-center gap-3">
          <span className="text-amber-400 font-mono text-xs">WARN</span>
          <span className="text-amber-400/80 text-sm">
            Missing: {missing.map(k => k.replace(/_/g, ' ')).join(', ')} —{' '}
            <button onClick={() => onNavigate('settings')} className="underline underline-offset-2 hover:text-amber-300">configure in Settings</button>
          </span>
        </div>
      )}

      {/* Agents */}
      <div className="space-y-3 mb-8">
        {AGENTS.map((agent, i) => {
          const c = C[agent.color]
          return (
            <div
              key={agent.id}
              className={`relative border ${c.border} ${c.bg} rounded-lg p-5 group cursor-pointer transition-all duration-200 hover:border-opacity-50`}
              style={{ boxShadow: `inset 0 0 40px ${c.glow}` }}
              onClick={() => onNavigate(agent.action)}
            >
              <div className="flex items-start gap-5">
                {/* Tag */}
                <div className={`font-mono text-xs ${c.accent} opacity-40 w-6 shrink-0 mt-0.5`}>{agent.tag}</div>

                {/* Content */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-3 mb-1">
                    <span className={`font-semibold text-base ${c.accent}`}>{agent.name}</span>
                    <span className="text-gray-600 text-xs font-mono">{agent.role}</span>
                  </div>
                  <p className="text-gray-400 text-sm leading-relaxed">{agent.desc}</p>
                  <div className="flex items-center gap-2 mt-3">
                    <span className="text-[10px] font-mono text-gray-700">OUTPUT →</span>
                    <span className={`text-[10px] font-mono ${c.accent} opacity-70`}>{agent.output}</span>
                  </div>
                </div>

                {/* Arrow */}
                <div className={`text-gray-700 group-hover:${c.accent} group-hover:translate-x-1 transition-all duration-200 mt-0.5`}>
                  →
                </div>
              </div>

              {/* Connector line */}
              {i < AGENTS.length - 1 && (
                <div className="absolute left-[2.15rem] -bottom-3.5 w-px h-3.5 bg-gradient-to-b from-white/10 to-transparent" />
              )}
            </div>
          )
        })}
      </div>

      {/* Bottom grid */}
      <div className="grid grid-cols-3 gap-4">
        {/* Data flow */}
        <div className="col-span-2 panel">
          <div className="panel-header">
            <span className="text-xs font-mono text-gray-500 uppercase tracking-wider">Data Flow</span>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-2 gap-x-6 gap-y-2.5 text-xs font-mono">
              {[
                ['fini', 'writes', 'Target Accounts'],
                ['n8n', 'writes', 'First Clean List'],
                ['searcher', 'reads', 'Target Accounts'],
                ['searcher', 'writes', 'Searcher Output'],
                ['veri', 'reads', 'Final Filtered List'],
                ['veri', 'writes cols O–U', 'Final Filtered List'],
              ].map(([agent, verb, sheet], i) => (
                <div key={i} className="flex items-center gap-2">
                  <span className="text-blue-500/70 w-16 shrink-0">{agent}</span>
                  <span className="text-gray-700">{verb}</span>
                  <span className="text-gray-400 truncate">{sheet}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Role priority */}
        <div className="panel">
          <div className="panel-header">
            <span className="text-xs font-mono text-gray-500 uppercase tracking-wider">Role Priority</span>
          </div>
          <div className="p-4 space-y-3">
            {[
              { label: 'DM', desc: 'CEO MD VP CIO COO', color: 'text-blue-400', bar: 'bg-blue-500' },
              { label: 'Influencer', desc: 'RTM GTM Digital eB2B', color: 'text-violet-400', bar: 'bg-violet-500' },
              { label: 'GateKeeper', desc: 'SFA Sales IT Trade', color: 'text-amber-400', bar: 'bg-amber-500' },
            ].map(r => (
              <div key={r.label}>
                <div className="flex items-center justify-between mb-1">
                  <span className={`text-xs font-mono font-medium ${r.color}`}>{r.label}</span>
                  <span className="text-[10px] text-gray-600 font-mono">{r.desc}</span>
                </div>
                <div className="h-px bg-white/5 rounded-full overflow-hidden">
                  <div className={`h-full ${r.bar} opacity-40 rounded-full`} style={{ width: r.label === 'DM' ? '100%' : r.label === 'Influencer' ? '65%' : '40%' }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
