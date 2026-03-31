'use client';

import React, { useEffect, useCallback, useRef } from 'react';
import { useCommandCenterStore } from '@/lib/stores';
import { apiUrl } from '@/lib/api';

// ---------------------------------------------------------------------------
// Stage config
// ---------------------------------------------------------------------------
const STAGE_CONFIG: Record<string, { label: string; color: string; bg: string }> = {
  enrichment_pending:    { label: 'No Data',            color: 'text-red-400',    bg: 'bg-red-500/10 border-red-500/20' },
  contacts_pending:      { label: 'Awaiting Contacts',  color: 'text-amber-400',  bg: 'bg-amber-500/10 border-amber-500/20' },
  verification_pending:  { label: 'Needs Verification', color: 'text-blue-400',   bg: 'bg-blue-500/10 border-blue-500/20' },
  verification_partial:  { label: 'Verifying...',       color: 'text-blue-400',   bg: 'bg-blue-500/10 border-blue-500/20' },
  verification_complete: { label: 'Verified',           color: 'text-teal-400',   bg: 'bg-teal-500/10 border-teal-500/20' },
  ready_for_outreach:    { label: 'Ready',              color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
};

const PRIORITY_COLORS: Record<string, string> = {
  high: 'text-red-400 bg-red-500/10 border-red-500/20',
  medium: 'text-amber-400 bg-amber-500/10 border-amber-500/20',
  low: 'text-white/40 bg-white/5 border-white/10',
};

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function CommandCenterPage() {
  const {
    loading, error, summary, companies, aiAnalysis, aiLoading,
    autoMode, autoModeStatus, expandedCompany, stageFilter, startRow, triggerLoading,
  } = useCommandCenterStore();
  const store = useCommandCenterStore;
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ---------------------------------------------------------------------------
  // Data fetching
  // ---------------------------------------------------------------------------
  const fetchStatus = useCallback(async () => {
    store.setState({ loading: true, error: null });
    const currentStartRow = useCommandCenterStore.getState().startRow;
    try {
      const resp = await fetch(apiUrl(`/api/orchestrator/status?start_row=${currentStartRow}`));
      const data = await resp.json();
      store.setState({
        summary: data.summary,
        companies: data.companies || [],
        loading: false,
      });
    } catch (e: any) {
      store.setState({ loading: false, error: e.message });
    }
  }, []);

  const fetchAiAnalysis = useCallback(async () => {
    store.setState({ aiLoading: true });
    try {
      const resp = await fetch(apiUrl('/api/orchestrator/analyze'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ focus: '' }),
      });
      const data = await resp.json();
      store.setState({ aiAnalysis: data, aiLoading: false });
    } catch {
      store.setState({ aiLoading: false });
    }
  }, []);

  const fetchAutoModeStatus = useCallback(async () => {
    try {
      const resp = await fetch(apiUrl('/api/orchestrator/auto-mode/status'));
      const data = await resp.json();
      store.setState({ autoModeStatus: data });
    } catch { /* ignore */ }
  }, []);

  // Initial load + polling
  useEffect(() => {
    fetchStatus();
    fetchAutoModeStatus();
    pollRef.current = setInterval(() => {
      fetchStatus();
      fetchAutoModeStatus();
    }, 30000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [fetchStatus, fetchAutoModeStatus]);

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------
  const triggerAgent = async (agent: string, companyNames: string[], params?: Record<string, any>) => {
    const key = companyNames.join(',');
    store.setState(s => ({ triggerLoading: { ...s.triggerLoading, [key]: true } }));
    try {
      await fetch(apiUrl('/api/orchestrator/trigger'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ agent, companies: companyNames, ...params }),
      });
      setTimeout(fetchStatus, 2000);
    } catch { /* ignore */ }
    store.setState(s => ({ triggerLoading: { ...s.triggerLoading, [key]: false } }));
  };

  const toggleAutoMode = async () => {
    const newEnabled = !autoMode.enabled;
    try {
      await fetch(apiUrl('/api/orchestrator/auto-mode'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          enabled: newEnabled,
          poll_interval_secs: autoMode.pollIntervalSecs,
          auto_trigger_searcher: true,
          auto_trigger_veri: true,
          dry_run: autoMode.dryRun,
        }),
      });
      store.setState(s => ({ autoMode: { ...s.autoMode, enabled: newEnabled } }));
      fetchAutoModeStatus();
    } catch { /* ignore */ }
  };

  const executeAiSuggestion = async (s: any) => {
    const agent = s.action.replace('run_', '');
    if (['fini', 'searcher', 'veri'].includes(agent)) {
      await triggerAgent(agent, [s.company], s.params);
    }
  };

  // ---------------------------------------------------------------------------
  // Filtering
  // ---------------------------------------------------------------------------
  const filteredCompanies = stageFilter
    ? companies.filter(c => c.stage === stageFilter)
    : companies;

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  return (
    <div className="h-screen overflow-y-auto no-scrollbar px-6 pt-4 pb-20 max-w-[1600px] mx-auto font-sans">

      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <div className="text-[9px] font-bold text-white/30 uppercase tracking-[0.35em] mb-1">Pipeline Control</div>
          <h1 className="text-lg font-bold text-white tracking-tight">Command Center</h1>
        </div>
        <div className="flex items-center gap-3">
          {/* Start row filter */}
          <div className="flex items-center gap-1.5">
            <span className="text-[9px] text-white/40 uppercase tracking-widest">From row</span>
            <input
              type="number"
              min={1}
              value={startRow}
              onChange={e => {
                const v = parseInt(e.target.value) || 1;
                store.setState({ startRow: v });
              }}
              onBlur={fetchStatus}
              onKeyDown={e => { if (e.key === 'Enter') fetchStatus(); }}
              className="w-16 px-2 py-1 rounded-md bg-black/30 border border-white/10 text-xs text-white text-center font-mono outline-none focus:border-white/25"
            />
          </div>
          <div className="h-5 w-[1px] bg-white/10" />
          {/* Auto Mode toggle */}
          <div className="flex items-center gap-2">
            <button
              onClick={toggleAutoMode}
              className={`relative w-9 h-5 rounded-full transition-all duration-300 ${autoMode.enabled ? 'bg-emerald-500/70' : 'bg-white/10'}`}
            >
              <div className={`absolute top-[3px] w-3.5 h-3.5 rounded-full shadow-sm transition-all duration-300 ${autoMode.enabled ? 'left-[19px] bg-white' : 'left-[3px] bg-white/40'}`} />
            </button>
            <span className="text-[10px] text-white/60 font-bold uppercase tracking-widest">Auto</span>
          </div>
          <button
            onClick={fetchStatus}
            disabled={loading}
            className="px-3 py-1.5 rounded-lg border border-white/10 text-[10px] font-bold text-white/60 uppercase tracking-widest hover:bg-white/5 transition-colors disabled:opacity-30"
          >
            {loading ? 'Loading...' : 'Refresh'}
          </button>
          <button
            onClick={fetchAiAnalysis}
            disabled={aiLoading}
            className="px-3 py-1.5 rounded-lg border border-blue-500/30 bg-blue-500/10 text-[10px] font-bold text-blue-400 uppercase tracking-widest hover:bg-blue-500/20 transition-colors disabled:opacity-30"
          >
            {aiLoading ? 'Analyzing...' : 'AI Analyze'}
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-3 px-4 py-2 rounded-lg bg-red-950/30 border border-red-500/20 text-[11px] text-red-400">{error}</div>
      )}

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-2 mb-4">
          {[
            { label: 'Companies', value: summary.total_companies, color: 'text-white' },
            { label: 'Enriched', value: summary.fully_enriched, color: 'text-blue-400' },
            { label: 'Has Contacts', value: summary.contacts_found, color: 'text-teal-400' },
            { label: 'Verified', value: summary.fully_verified, color: 'text-emerald-400' },
            { label: 'Needs Attention', value: summary.needs_attention, color: 'text-amber-400' },
            { label: 'Rejected', value: summary.rejected_count, color: 'text-red-400' },
          ].map(s => (
            <div key={s.label} className="border border-white/[0.06] rounded-xl bg-white/[0.02] px-3 py-2.5">
              <div className={`text-lg font-bold ${s.color}`}>{s.value}</div>
              <div className="text-[9px] text-white/40 uppercase tracking-widest">{s.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Main grid */}
      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">

        {/* Left: Company table */}
        <div className="xl:col-span-3">
          <div className="border border-white/[0.06] rounded-2xl bg-white/[0.02] overflow-hidden">
            {/* Table header */}
            <div className="px-4 py-3 border-b border-white/[0.06] flex items-center justify-between">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.3em]">Company Pipeline</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => store.setState({ stageFilter: null })}
                  className={`px-2 py-0.5 rounded text-[8px] font-bold uppercase tracking-widest transition-all ${!stageFilter ? 'bg-white/10 text-white/80' : 'text-white/30 hover:text-white/50'}`}
                >
                  All
                </button>
                {Object.entries(STAGE_CONFIG).map(([key, cfg]) => (
                  <button
                    key={key}
                    onClick={() => store.setState({ stageFilter: stageFilter === key ? null : key })}
                    className={`px-2 py-0.5 rounded text-[8px] font-bold uppercase tracking-widest transition-all ${stageFilter === key ? cfg.bg + ' ' + cfg.color : 'text-white/30 hover:text-white/50'}`}
                  >
                    {cfg.label.split(' ')[0]}
                  </button>
                ))}
              </div>
            </div>

            {/* Table rows */}
            <div className="divide-y divide-white/[0.04]">
              {filteredCompanies.length === 0 && (
                <div className="px-4 py-8 text-center text-[11px] text-white/30">
                  {loading ? 'Loading...' : 'No companies found. Run Fini first.'}
                </div>
              )}
              {filteredCompanies.map((c, idx) => {
                const stage = STAGE_CONFIG[c.stage] || STAGE_CONFIG.enrichment_pending;
                const isExpanded = expandedCompany === c.company_name;
                return (
                  <div key={`${c.company_name}-${idx}`}>
                    {/* Row */}
                    <div
                      className="px-4 py-2.5 flex items-center gap-3 hover:bg-white/[0.02] cursor-pointer transition-colors"
                      onClick={() => store.setState({ expandedCompany: isExpanded ? null : c.company_name })}
                    >
                      {/* Company name */}
                      <div className="flex-1 min-w-0">
                        <div className="text-xs font-bold text-white truncate">{c.company_name}</div>
                        <div className="text-[9px] text-white/30 truncate">{c.domain || 'no domain'}</div>
                      </div>

                      {/* Stage pill */}
                      <span className={`px-2 py-0.5 rounded-md border text-[8px] font-bold uppercase tracking-widest ${stage.bg} ${stage.color}`}>
                        {stage.label}
                      </span>

                      {/* Contacts count */}
                      <div className="text-right w-10">
                        <div className="text-[11px] font-bold text-white">{c.total_contacts}</div>
                        <div className="text-[8px] text-white/30">contacts</div>
                      </div>

                      {/* Role checkmarks */}
                      {(['ceo_md', 'cto_cio', 'cso_sales'] as const).map(tier => {
                        const role = c.role_coverage[tier];
                        return (
                          <div key={tier} className="w-8 text-center" title={role?.label}>
                            <div className="text-[8px] text-white/30 uppercase">{role?.label?.split('/')[0] || tier}</div>
                            {role?.filled ? (
                              <span className="text-emerald-400 text-xs">&#10003;</span>
                            ) : (
                              <span className="text-red-400/50 text-xs">&#10007;</span>
                            )}
                          </div>
                        );
                      })}

                      {/* Action buttons */}
                      <div className="flex items-center gap-1">
                        <button
                          onClick={e => { e.stopPropagation(); triggerAgent('searcher', [c.company_name]); }}
                          disabled={!!triggerLoading[c.company_name]}
                          className="px-2 py-1 rounded text-[8px] font-bold text-blue-400/70 bg-blue-500/5 border border-blue-500/10 hover:bg-blue-500/15 transition-colors uppercase tracking-widest disabled:opacity-30"
                          title="Run Searcher"
                        >
                          Search
                        </button>
                        <button
                          onClick={e => { e.stopPropagation(); triggerAgent('veri', [c.company_name]); }}
                          disabled={!!triggerLoading[c.company_name]}
                          className="px-2 py-1 rounded text-[8px] font-bold text-emerald-400/70 bg-emerald-500/5 border border-emerald-500/10 hover:bg-emerald-500/15 transition-colors uppercase tracking-widest disabled:opacity-30"
                          title="Run Veri"
                        >
                          Verify
                        </button>
                      </div>
                    </div>

                    {/* Expanded detail */}
                    {isExpanded && (
                      <div className="px-4 pb-3 bg-white/[0.01]">
                        <div className="grid grid-cols-3 gap-2 mb-2">
                          <div className="text-[9px] text-white/30"><span className="text-white/50">Domain:</span> {c.domain || '—'}</div>
                          <div className="text-[9px] text-white/30"><span className="text-white/50">Email:</span> <span className="font-mono">{c.email_format || '—'}</span></div>
                          <div className="text-[9px] text-white/30"><span className="text-white/50">Size:</span> {c.account_size || '—'} / {c.account_type || '—'}</div>
                        </div>

                        {/* Role coverage detail */}
                        <div className="flex gap-2 mb-2">
                          {Object.entries(c.role_coverage).map(([key, role]) => (
                            <div key={key} className={`flex-1 px-2 py-1.5 rounded-lg border ${role.filled ? 'border-emerald-500/20 bg-emerald-500/5' : 'border-red-500/10 bg-red-500/5'}`}>
                              <div className="text-[8px] text-white/40 uppercase tracking-widest">{role.label}</div>
                              {role.filled ? (
                                <>
                                  <div className="text-[10px] text-white/80 font-medium truncate">{role.contact_name}</div>
                                  <div className="text-[8px] text-white/30 truncate">{role.title}</div>
                                </>
                              ) : (
                                <div className="text-[10px] text-red-400/60">Missing</div>
                              )}
                            </div>
                          ))}
                        </div>

                        {/* Contacts list */}
                        {c.contacts.length > 0 && (
                          <div className="space-y-0.5">
                            <div className="text-[8px] text-white/30 uppercase tracking-widest mb-1">Contacts ({c.contacts.length})</div>
                            {c.contacts.slice(0, 10).map((ct, i) => (
                              <div key={i} className="flex items-center gap-2 text-[10px]">
                                <span className={`w-1.5 h-1.5 rounded-full ${ct.overall_status === 'VERIFIED' ? 'bg-emerald-400' : ct.overall_status === 'REVIEW' ? 'bg-amber-400' : 'bg-white/20'}`} />
                                <span className="text-white/70 w-28 truncate">{ct.full_name}</span>
                                <span className="text-white/30 flex-1 truncate">{ct.title}</span>
                                <span className="text-white/20 font-mono text-[9px]">{ct.overall_status || 'PENDING'}</span>
                              </div>
                            ))}
                            {c.contacts.length > 10 && (
                              <div className="text-[9px] text-white/20">+{c.contacts.length - 10} more</div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Right: AI + Auto + Runs */}
        <div className="xl:col-span-2 flex flex-col gap-3">

          {/* AI Analysis panel */}
          <div className="border border-white/[0.06] rounded-2xl bg-white/[0.02] p-4">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.3em] mb-3">AI Analysis</div>
            {aiLoading && <div className="text-[11px] text-white/30 animate-pulse">Analyzing pipeline...</div>}
            {!aiLoading && !aiAnalysis && (
              <div className="text-[11px] text-white/20">Click "AI Analyze" to get smart suggestions</div>
            )}
            {aiAnalysis && (
              <>
                <div className="text-[11px] text-white/60 mb-3 leading-relaxed">{aiAnalysis.summary}</div>
                <div className="space-y-2">
                  {(aiAnalysis.suggestions || []).map((s, i) => {
                    const pColor = PRIORITY_COLORS[s.priority] || PRIORITY_COLORS.low;
                    return (
                      <div key={i} className="px-3 py-2 rounded-lg border border-white/[0.06] bg-white/[0.02]">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-[10px] font-bold text-white/80">{s.company}</span>
                          <span className={`px-1.5 py-0.5 rounded text-[7px] font-bold uppercase tracking-widest border ${pColor}`}>
                            {s.priority}
                          </span>
                        </div>
                        <div className="text-[10px] text-white/40 mb-1.5">{s.reason}</div>
                        {s.action !== 'complete' && s.action !== 'review' && (
                          <button
                            onClick={() => executeAiSuggestion(s)}
                            className="px-2.5 py-1 rounded-md bg-blue-500/10 border border-blue-500/20 text-[9px] font-bold text-blue-400 uppercase tracking-widest hover:bg-blue-500/20 transition-colors"
                          >
                            Execute: {s.action.replace('run_', '')}
                          </button>
                        )}
                        {s.action === 'complete' && (
                          <span className="text-[9px] text-emerald-400/60 font-bold uppercase tracking-widest">Ready for outreach</span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </div>

          {/* Auto-mode panel */}
          <div className="border border-white/[0.06] rounded-2xl bg-white/[0.02] p-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-[9px] font-bold text-white/50 uppercase tracking-[0.3em]">Auto Mode</span>
              <div className="flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${autoMode.enabled ? 'bg-emerald-400 animate-pulse' : 'bg-white/10'}`} />
                <span className="text-[10px] text-white/40">{autoMode.enabled ? 'Running' : 'Off'}</span>
              </div>
            </div>
            {autoModeStatus && autoMode.enabled && (
              <div className="space-y-1">
                <div className="text-[10px] text-white/30">
                  Last check: {autoModeStatus.last_check ? new Date(autoModeStatus.last_check).toLocaleTimeString() : 'never'}
                </div>
                {autoModeStatus.actions_taken.length > 0 && (
                  <div className="mt-2">
                    <div className="text-[8px] text-white/20 uppercase tracking-widest mb-1">Recent Actions</div>
                    {autoModeStatus.actions_taken.slice(-5).map((a, i) => (
                      <div key={i} className="text-[9px] text-white/30 truncate">
                        <span className="text-white/15 font-mono">{new Date(a.ts).toLocaleTimeString()}</span> {a.action}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Active runs */}
          <div className="border border-white/[0.06] rounded-2xl bg-white/[0.02] p-4">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.3em] mb-3">Active Runs</div>
            <ActiveRunsMonitor />
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Active Runs Monitor
// ---------------------------------------------------------------------------
function ActiveRunsMonitor() {
  const [runs, setRuns] = React.useState<any[]>([]);

  useEffect(() => {
    const fetchRuns = async () => {
      try {
        const resp = await fetch(apiUrl('/api/runs'));
        const data = await resp.json();
        setRuns(data.filter((r: any) => r.status === 'running'));
      } catch { /* ignore */ }
    };
    fetchRuns();
    const interval = setInterval(fetchRuns, 5000);
    return () => clearInterval(interval);
  }, []);

  if (runs.length === 0) {
    return <div className="text-[10px] text-white/20">No active runs</div>;
  }

  return (
    <div className="space-y-1.5">
      {runs.map(r => (
        <div key={r.thread_id} className="flex items-center gap-2 text-[10px]">
          <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
          <span className="text-white/60 font-medium">{r.agent}</span>
          <span className="text-white/30 flex-1 truncate">{r.company || r.thread_id?.slice(0, 8)}</span>
          <span className="text-white/15 font-mono text-[8px]">{r.thread_id?.slice(0, 8)}</span>
        </div>
      ))}
    </div>
  );
}
