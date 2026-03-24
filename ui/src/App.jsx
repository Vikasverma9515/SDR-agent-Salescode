import { useState, useEffect } from 'react'
import { apiUrl } from './lib/api'
import FiniPage from './pages/FiniPage'
import SearcherPage from './pages/SearcherPage'
import VeriPage from './pages/VeriPage'
import DashboardPage from './pages/DashboardPage'
import SettingsPage from './pages/SettingsPage'

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Overview',  sub: null,                 dot: 'bg-blue-500' },
  { id: 'fini',      label: 'Fini',      sub: 'Target Builder',     dot: 'bg-blue-400' },
  { id: 'searcher',  label: 'Searcher',  sub: 'Contact Discovery',  dot: 'bg-violet-400' },
  { id: 'veri',      label: 'Veri',      sub: 'Contact QC',         dot: 'bg-emerald-400' },
  { id: 'settings',  label: 'Settings',  sub: null,                 dot: 'bg-gray-500' },
]

export default function App() {
  const [activePage, setActivePage] = useState('dashboard')
  const [configStatus, setConfigStatus] = useState(null)
  const [veriThreadId, setVeriThreadId] = useState(null)

  const navigateToVeri = (threadId) => {
    setVeriThreadId(threadId || null)
    setActivePage('veri')
  }

  useEffect(() => {
    fetch(apiUrl('/api/config/check')).then(r => r.json()).then(setConfigStatus).catch(() => null)
  }, [])

  return (
    <div className="flex h-screen bg-[#050810] overflow-hidden">
      {/* Ambient scan line */}
      <div className="scan-line" />

      {/* Sidebar */}
      <aside className="relative w-52 flex flex-col border-r border-white/[0.05] bg-white/[0.015] noise">
        {/* Logo */}
        <div className="px-5 py-5 border-b border-white/[0.05]">
          <div className="flex items-center gap-3">
            <div className="relative w-8 h-8 rounded flex items-center justify-center bg-blue-600 text-white text-xs font-bold tracking-tighter overflow-hidden">
              <span className="relative z-10">SC</span>
              <div className="absolute inset-0 bg-gradient-to-br from-blue-400/20 to-transparent" />
            </div>
            <div>
              <div className="text-white text-sm font-semibold leading-none tracking-tight">Mapping Pipeline</div>
              <div className="text-[10px] text-blue-400/60 font-mono mt-0.5 tracking-widest uppercase">SalesCode.ai</div>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-2 py-4 space-y-0.5">
          {NAV_ITEMS.map(item => (
            <button
              key={item.id}
              onClick={() => setActivePage(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded text-left transition-all duration-150 group ${
                activePage === item.id ? 'nav-active' : 'hover:bg-white/[0.04]'
              }`}
            >
              <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 transition-all ${
                activePage === item.id ? item.dot + ' shadow-lg' : 'bg-white/20 group-hover:bg-white/40'
              }`} style={activePage === item.id ? {boxShadow: `0 0 6px currentColor`} : {}} />
              <div>
                <div className={`text-sm font-medium leading-none ${activePage === item.id ? 'text-blue-300' : 'text-gray-400 group-hover:text-gray-200'}`}>
                  {item.label}
                </div>
                {item.sub && (
                  <div className="text-[10px] text-gray-600 mt-0.5 font-mono">{item.sub}</div>
                )}
              </div>
            </button>
          ))}
        </nav>

        {/* Service status */}
        {configStatus && (
          <div className="px-4 py-4 border-t border-white/[0.05]">
            <div className="text-[9px] font-mono text-gray-600 uppercase tracking-[0.15em] mb-2.5">System Status</div>
            <div className="space-y-1.5">
              {Object.entries(configStatus).map(([key, ok]) => (
                <div key={key} className="flex items-center gap-2">
                  <div className="relative flex-shrink-0">
                    {ok && <span className="absolute inset-0 rounded-full bg-emerald-400 ping-slow" />}
                    <span className={`relative block w-1.5 h-1.5 rounded-full ${ok ? 'bg-emerald-400' : 'bg-red-900'}`} />
                  </div>
                  <span className={`text-[10px] font-mono truncate ${ok ? 'text-gray-500' : 'text-red-700'}`}>
                    {key.replace(/_/g, '_')}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Bottom version */}
        <div className="px-5 py-3 border-t border-white/[0.04]">
          <div className="text-[9px] font-mono text-gray-700">v1.0.0 · {new Date().toLocaleDateString()}</div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-y-auto bg-grid relative">
        {/* Build timestamp badge */}
        <div className="fixed bottom-4 right-4 z-50 flex items-center gap-1.5 bg-white/[0.04] border border-white/[0.08] rounded px-2.5 py-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 flex-shrink-0" />
          <span className="text-[9px] font-mono text-gray-500 uppercase tracking-wider">
            deployed {new Date(__BUILD_TIME__).toLocaleString()}
          </span>
        </div>
        <div className="page-transition min-h-full">
          {activePage === 'dashboard' && <DashboardPage onNavigate={setActivePage} />}
          {activePage === 'fini'      && <FiniPage />}
          {activePage === 'searcher'  && <SearcherPage onNavigateVeri={navigateToVeri} />}
          {activePage === 'veri'      && <VeriPage initialThreadId={veriThreadId} />}
          {activePage === 'settings'  && <SettingsPage />}
        </div>
      </main>
    </div>
  )
}
