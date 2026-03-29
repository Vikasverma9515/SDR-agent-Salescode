'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { apiUrl } from '@/lib/api';

const MODULES = [
  {
    tag: '01', id: 'fini', name: 'Fini', role: 'Target Builder', href: '/fini',
    color: 'violet',
    what: 'Build your target account list — enrich company names with domains, LinkedIn org IDs, and email formats.',
    how: 'Enter company names (comma-separated or paste a list). Fini enriches each one and writes to the Target Accounts sheet after your review.',
    outputs: ['Company domain', 'LinkedIn Org ID', 'Email format'],
    time: '~2 min / company',
  },
  {
    tag: '02', id: 'searcher', name: 'Searcher', role: 'Contact Discovery', href: '/searcher',
    color: 'blue',
    what: 'Find the right people — multi-source contact discovery across LinkedIn, TheOrg, web filings, and AI research.',
    how: 'Enter target companies (auto-loaded from Fini) and define the role titles you need. Searcher finds people, groups them by department, and lets you approve each batch.',
    outputs: ['Full name + title', 'LinkedIn URL', 'Email (constructed)', 'Buying role (DM / Influencer)'],
    time: '~5–15 min / company',
  },
  {
    tag: '03', id: 'veri', name: 'Veri', role: 'Contact QC', href: '/veri',
    color: 'emerald',
    what: 'Validate every contact — 6-step verification stack cleans your list before outreach.',
    how: 'Runs automatically after Searcher, or manually on a row range. Each contact is scored and labeled VERIFIED, REVIEW, or DECOMMISSIONED.',
    outputs: ['Verified flag', 'ZeroBounce email score', 'LinkedIn tenure check'],
    time: '~30 sec / contact',
  },
];

const SDR_STEPS = [
  {
    n: '01', title: 'Add Target Companies', module: 'Fini', href: '/fini',
    desc: 'Go to Fini and paste your target company names. Hit Run — Fini will find the domain, LinkedIn page, and email pattern for each one. Review the results and approve.',
    tip: 'You can paste up to 50 companies at once.',
    color: 'violet',
  },
  {
    n: '02', title: 'Discover Contacts', module: 'Searcher', href: '/searcher',
    desc: 'Go to Searcher. Your Fini companies auto-load as targets. Set the gap-fill roles (e.g. "VP Sales, CMO, Head of Marketing"). Click Start Scan — the pipeline finds people across LinkedIn and the web, groups them by department, and asks you to approve each batch.',
    tip: 'Use the AI Scout tab to find specific people by chatting — great for niche roles or hard-to-find contacts.',
    color: 'blue',
  },
  {
    n: '03', title: 'Verify & Qualify', module: 'Veri', href: '/veri',
    desc: 'Veri runs automatically after Searcher, but you can also trigger it manually on any row range. It validates emails via ZeroBounce, cross-checks LinkedIn tenure, and labels each contact. Verified contacts are ready for outreach.',
    tip: 'Focus outreach on "VERIFIED" contacts first — highest deliverability.',
    color: 'emerald',
  },
];

const QUICK_TIPS = [
  { icon: '⚡', title: 'AI Scout', desc: 'Can\'t find a specific person? Open the AI Scout tab in Searcher — chat naturally to find contacts by role, seniority, or name.' },
  { icon: '🎯', title: 'Department Filter', desc: 'After a scan, Searcher groups contacts by department. Deselect irrelevant teams to save time and only process what matters.' },
  { icon: '📋', title: 'Pipeline Queue', desc: 'AI Scout results go into the Pipeline Queue. Review, then click "↑ Sheet" to write directly to the First Clean List.' },
  { icon: '🔁', title: 'Veri Auto-runs', desc: 'Searcher triggers Veri automatically on newly written contacts. You only need to run Veri manually if you\'re cleaning older data.' },
];

const COLOR_MAP: Record<string, { dot: string; badge: string; border: string; bg: string; text: string }> = {
  violet: { dot: 'bg-violet-400', badge: 'bg-violet-400/10 text-violet-400', border: 'border-violet-400/20', bg: 'bg-violet-400/[0.03]', text: 'text-violet-400' },
  blue: { dot: 'bg-blue-400', badge: 'bg-blue-400/10 text-blue-400', border: 'border-blue-400/20', bg: 'bg-blue-400/[0.03]', text: 'text-blue-400' },
  emerald: { dot: 'bg-emerald-400', badge: 'bg-emerald-400/10 text-emerald-400', border: 'border-emerald-400/20', bg: 'bg-emerald-400/[0.03]', text: 'text-emerald-400' },
};

export default function OverviewPage() {
  const [config, setConfig] = useState<Record<string, boolean> | null>(null);
  const [time, setTime] = useState(new Date());
  const [guideOpen, setGuideOpen] = useState<number | null>(0);

  useEffect(() => {
    fetch(apiUrl('/api/config/check')).then(r => r.json()).then(setConfig).catch(() => { });
    const t = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  const missing = config
    ? Object.entries(config).filter(([k, v]) => !v && k !== 'chrome_cdp').map(([k]) => k)
    : [];

  const allOnline = config && missing.length === 0;

  return (
    <div className="p-6 xl:p-8 max-w-[1500px] mx-auto font-sans">

      {/* ── Header ── */}
      <div className="flex items-start justify-between mb-6 pb-5 border-b border-white/[0.06]">
        <div>
          <div className="text-[9px] font-bold text-white/35 uppercase tracking-[0.45em] mb-2">Sales Pipeline · Overview</div>
          <h1 className="text-2xl font-bold text-white tracking-tight">
            Welcome to <span className="text-white/50 font-light">Salescode ProspectOps</span>
          </h1>
          <p className="text-white/45 mt-1.5 text-sm max-w-lg leading-relaxed">
            Your autonomous B2B prospecting engine. Three modules, one pipeline — from company list to verified contacts ready for outreach.
          </p>
        </div>
        <div className="hidden lg:flex flex-col items-end gap-1" suppressHydrationWarning>
          <div className="text-xl font-bold text-white tabular-nums font-mono" suppressHydrationWarning>
            {time.toLocaleTimeString([], { hour12: false })}
          </div>
          <div className="text-[9px] text-white/30 uppercase tracking-[0.2em]" suppressHydrationWarning>
            {time.toLocaleDateString('en-US', { weekday: 'long', month: 'short', day: 'numeric' })}
          </div>
          <div className="flex items-center gap-1.5 mt-1">
            <div className={`w-1.5 h-1.5 rounded-full ${allOnline ? 'bg-emerald-400' : 'bg-amber-400'}`} />
            <span className={`text-[9px] font-bold uppercase tracking-widest ${allOnline ? 'text-emerald-400/70' : 'text-amber-400/70'}`}>
              {allOnline ? 'All systems online' : `${missing.length} service${missing.length > 1 ? 's' : ''} offline`}
            </span>
          </div>
        </div>
      </div>

      {/* ── Config alert ── */}
      {missing.length > 0 && (
        <div className="mb-6 flex items-center gap-4 px-5 py-3.5 rounded-xl border border-amber-500/20 bg-amber-500/[0.04]">
          <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse flex-shrink-0" />
          <div className="flex-1 text-sm text-white/60">
            <span className="font-bold text-white/80">Services offline — </span>
            <span className="font-mono text-amber-400/70 text-xs">{missing.map(k => k.replace(/_/g, ' ')).join(', ')}</span>
          </div>
          <Link href="/settings" className="px-3 py-1 border border-amber-400/20 rounded-lg text-[10px] font-bold text-amber-400/70 hover:text-amber-400 hover:border-amber-400/40 uppercase tracking-widest transition-all">
            Configure
          </Link>
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">

        {/* ── Left: SDR Guide ── */}
        <div className="xl:col-span-2 space-y-5">

          {/* How to use — accordion */}
          <div className="border border-white/[0.07] rounded-2xl overflow-hidden">
            <div className="px-5 py-4 border-b border-white/[0.05] flex items-center justify-between bg-white/[0.01]">
              <div className="flex items-center gap-3">
                <div className="w-5 h-5 rounded-md bg-white/[0.06] border border-white/[0.08] flex items-center justify-center">
                  <span className="text-[9px] font-bold text-white/50">?</span>
                </div>
                <span className="text-[10px] font-bold text-white/60 uppercase tracking-[0.35em]">SDR Quick Start Guide</span>
              </div>
              <span className="text-[9px] text-white/30 uppercase tracking-wider">3 steps to first contact</span>
            </div>

            <div className="divide-y divide-white/[0.04]">
              {SDR_STEPS.map((step, i) => {
                const c = COLOR_MAP[step.color];
                const isOpen = guideOpen === i;
                return (
                  <div key={i}>
                    <button
                      onClick={() => setGuideOpen(isOpen ? null : i)}
                      className="w-full flex items-center gap-4 px-5 py-4 hover:bg-white/[0.02] transition-colors text-left"
                    >
                      <span className={`text-[9px] font-bold font-mono ${c.text} w-6 shrink-0`}>{step.n}</span>
                      <div className={`w-1.5 h-1.5 rounded-full ${c.dot} shrink-0`} />
                      <span className="text-sm font-bold text-white/85 flex-1">{step.title}</span>
                      <span className={`text-[9px] font-bold px-2 py-0.5 rounded-md ${c.badge} uppercase tracking-wider shrink-0`}>{step.module}</span>
                      <span className={`text-white/20 text-xs shrink-0 transition-transform duration-200 ${isOpen ? 'rotate-90' : ''}`}>›</span>
                    </button>

                    {isOpen && (
                      <div className={`px-5 pb-5 pt-0 border-t border-white/[0.04] ${c.bg}`}>
                        <div className="ml-10 space-y-3">
                          <p className="text-sm text-white/60 leading-relaxed pt-4">{step.desc}</p>
                          <div className={`flex items-start gap-2 px-3 py-2 rounded-lg border ${c.border} bg-black/20`}>
                            <span className="text-[9px] mt-0.5">💡</span>
                            <span className="text-[11px] text-white/50">{step.tip}</span>
                          </div>
                          <Link
                            href={step.href}
                            className={`inline-flex items-center gap-2 px-4 py-2 rounded-lg text-[10px] font-bold uppercase tracking-widest transition-all ${c.badge} border ${c.border} hover:opacity-80`}
                          >
                            Open {step.module} →
                          </Link>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>

          {/* Module cards — quick access */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {MODULES.map((mod) => {
              const c = COLOR_MAP[mod.color];
              return (
                <Link
                  key={mod.id}
                  href={mod.href}
                  className={`group flex flex-col border ${c.border} rounded-xl ${c.bg} hover:bg-white/[0.04] transition-all duration-200 overflow-hidden`}
                >
                  <div className="p-4 flex flex-col flex-1">
                    <div className="flex items-center justify-between mb-3">
                      <span className={`text-[8px] font-bold font-mono ${c.text} uppercase tracking-widest`}>Phase {mod.tag}</span>
                      <span className="text-[9px] text-white/30 uppercase tracking-widest">{mod.time}</span>
                    </div>
                    <h3 className="text-base font-bold text-white mb-0.5">{mod.name}</h3>
                    <div className={`text-[9px] font-bold uppercase tracking-[0.2em] ${c.text} mb-2`}>{mod.role}</div>
                    <p className="text-[11px] text-white/50 leading-relaxed flex-1 mb-3">{mod.what}</p>
                    <div className="space-y-1">
                      {mod.outputs.map((o, j) => (
                        <div key={j} className="flex items-center gap-1.5">
                          <div className={`w-1 h-1 rounded-full ${c.dot} opacity-60`} />
                          <span className="text-[10px] text-white/40">{o}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className={`px-4 py-2 border-t border-white/[0.04] flex items-center justify-between`}>
                    <span className="text-[9px] text-white/30 uppercase tracking-widest">Launch</span>
                    <span className={`text-[10px] font-bold ${c.text} group-hover:translate-x-0.5 transition-transform`}>→</span>
                  </div>
                </Link>
              );
            })}
          </div>

          {/* Quick tips */}
          <div className="border border-white/[0.07] rounded-2xl overflow-hidden">
            <div className="px-5 py-3.5 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.35em]">Power Tips</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-0 divide-y divide-x-0 md:divide-y-0 md:divide-x divide-white/[0.04]">
              {QUICK_TIPS.map((tip, i) => (
                <div key={i} className={`p-5 ${i >= 2 ? 'border-t border-white/[0.04]' : ''}`}>
                  <div className="flex items-start gap-3">
                    <span className="text-lg leading-none">{tip.icon}</span>
                    <div>
                      <div className="text-[11px] font-bold text-white/80 mb-1">{tip.title}</div>
                      <p className="text-[11px] text-white/45 leading-relaxed">{tip.desc}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* ── Right: Status + System ── */}
        <div className="space-y-4">

          {/* Pipeline flow */}
          <div className="border border-white/[0.07] rounded-2xl overflow-hidden">
            <div className="px-5 py-3.5 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.35em]">Pipeline Flow</span>
            </div>
            <div className="p-4 space-y-2">
              {[
                { label: 'Fini', sub: 'Enrichment', color: 'violet', arrow: true },
                { label: 'Google Sheets', sub: 'Target Accounts', color: 'white', arrow: true },
                { label: 'Searcher', sub: 'Contact Discovery', color: 'blue', arrow: true },
                { label: 'Veri', sub: 'Quality Control', color: 'emerald', arrow: true },
                { label: 'First Clean List', sub: 'Ready for outreach', color: 'white', arrow: false },
              ].map((s, i) => (
                <div key={i}>
                  <div className={`flex items-center gap-3 px-3 py-2 rounded-lg ${s.color !== 'white' ? COLOR_MAP[s.color]?.bg : 'bg-white/[0.02]'} border ${s.color !== 'white' ? COLOR_MAP[s.color]?.border : 'border-white/[0.05]'}`}>
                    <div className={`w-1.5 h-1.5 rounded-full shrink-0 ${s.color !== 'white' ? COLOR_MAP[s.color]?.dot : 'bg-white/30'}`} />
                    <div className="min-w-0 flex-1">
                      <div className="text-[11px] font-bold text-white/80 leading-none">{s.label}</div>
                      <div className="text-[9px] text-white/35 mt-0.5 uppercase tracking-wider">{s.sub}</div>
                    </div>
                  </div>
                  {s.arrow && (
                    <div className="flex justify-center py-0.5">
                      <div className="w-[1px] h-3 bg-white/10" />
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* System health */}
          <div className="border border-white/[0.07] rounded-2xl overflow-hidden">
            <div className="px-5 py-3.5 border-b border-white/[0.05] bg-white/[0.01] flex items-center justify-between">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.35em]">System Health</span>
              <Link href="/settings" className="text-[9px] text-white/25 hover:text-white/50 uppercase tracking-widest transition-colors">Configure →</Link>
            </div>
            <div className="p-4 space-y-2">
              {config ? (
                Object.entries(config).filter(([k]) => k !== 'chrome_cdp').map(([key, ok]) => (
                  <div key={key} className={`flex items-center gap-3 px-3 py-2 rounded-lg border ${ok ? 'border-emerald-400/10 bg-emerald-400/[0.03]' : 'border-red-400/15 bg-red-400/[0.04]'}`}>
                    <div className={`w-1.5 h-1.5 rounded-full shrink-0 ${ok ? 'bg-emerald-400' : 'bg-red-400 animate-pulse'}`} />
                    <span className="text-[11px] font-mono text-white/60 flex-1 truncate uppercase tracking-wide">
                      {key.replace(/_/g, ' ').replace(' api key', '').replace(' api', '')}
                    </span>
                    <span className={`text-[8px] font-bold uppercase tracking-widest ${ok ? 'text-emerald-400/60' : 'text-red-400/70'}`}>
                      {ok ? 'OK' : 'OFF'}
                    </span>
                  </div>
                ))
              ) : (
                <div className="py-4 text-center">
                  <div className="text-[10px] text-white/25 uppercase tracking-widest animate-pulse">Checking services…</div>
                </div>
              )}
            </div>
          </div>

          {/* Keyboard shortcuts */}
          <div className="border border-white/[0.07] rounded-2xl overflow-hidden">
            <div className="px-5 py-3.5 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.35em]">Navigation</span>
            </div>
            <div className="p-4 space-y-1.5">
              {[
                { path: '/fini', label: 'Target Builder', tag: '01' },
                { path: '/searcher', label: 'Contact Discovery', tag: '02' },
                { path: '/veri', label: 'Contact QC', tag: '03' },
                { path: '/settings', label: 'Settings', tag: '—' },
              ].map((nav) => (
                <Link
                  key={nav.path}
                  href={nav.path}
                  className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-white/[0.04] transition-colors group"
                >
                  <span className="text-[9px] font-mono text-white/25 w-5">{nav.tag}</span>
                  <span className="text-[11px] text-white/55 group-hover:text-white/80 transition-colors flex-1">{nav.label}</span>
                  <span className="text-white/15 text-xs group-hover:text-white/35 transition-colors">→</span>
                </Link>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
