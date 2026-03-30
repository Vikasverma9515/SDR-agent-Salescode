'use client';

import React, { useState, useCallback, useRef } from 'react';
import { useSearcherStore, useVeriStore } from '@/lib/stores';
import { useRouter } from 'next/navigation';
import { apiUrl } from '@/lib/api';
import LogStream from '@/components/LogStream';

const DEFAULT_DM_ROLES = 'VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales';

const DISCOVERY_STEPS = [
  { step: '01', label: 'Gap Analysis', op: 'Sheet Dedup' },
  { step: '02', label: 'Role Expansion', op: 'Multilingual LLM' },
  { step: '03', label: 'LinkedIn Search', op: '20+ Parallel Queries' },
  { step: '04', label: 'Sales Navigator', op: 'Full Scrape (all people)' },
  { step: '05', label: 'Web + Perplexity', op: 'Exec Discovery + Filings' },
  { step: '06', label: 'Role Selection', op: 'SDR Picks Departments' },
  { step: '07', label: 'AI Rank + Notes', op: 'Score + Why Important' },
  { step: '08', label: 'Validation', op: 'LinkedIn + ZeroBounce' },
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface ContactData {
  full_name: string;
  company: string;
  role_title?: string;
  role_bucket?: string;
  linkedin_url?: string;
  linkedin_verified?: boolean;
  email?: string;
  email_status?: string;
  domain?: string;
}

interface DiscoveredCandidate {
  index: number;
  full_name: string;
  role_title: string;
  company: string;
  linkedin_url: string;
  linkedin_verified: boolean;
  source: string;
  pre_selected: boolean;
  group: 'matched' | 'bonus';
  importance_note?: string;
  is_new?: boolean;
}

interface ContactSelectionEvent {
  company: string;
  candidates: DiscoveredCandidate[];
  matched_count: number;
  bonus_count: number;
  total: number;
  timeout_secs: number;
}

interface RoleBucket {
  id: string;
  label: string;
  count: number;
  sample_roles: string[];
  pre_selected: boolean;
  priority_rank?: number;
  priority_reason?: string;
}

interface RoleSelectionEvent {
  company: string;
  buckets: RoleBucket[];
  total_found: number;
}

// ---------------------------------------------------------------------------
// CandidateRow sub-component
// ---------------------------------------------------------------------------
function CandidateRow({
  c, selected, onToggle, accent,
}: {
  c: DiscoveredCandidate;
  selected: boolean;
  onToggle: (idx: number) => void;
  accent: 'teal' | 'amber';
}) {
  const accentClass = accent === 'teal'
    ? { bg: 'bg-teal-400/10', text: 'text-teal-400/80', check: 'border-teal-400 bg-teal-400', selectedBg: 'bg-teal-400/[0.07]' }
    : { bg: 'bg-amber-400/10', text: 'text-amber-400/80', check: 'border-amber-400 bg-amber-400', selectedBg: 'bg-amber-500/[0.07]' };

  return (
    <div
      onClick={() => onToggle(c.index)}
      className={`px-6 py-3 flex items-center gap-3 cursor-pointer transition-colors duration-150 ${selected ? accentClass.selectedBg : 'hover:bg-white/[0.02]'}`}
    >
      <div className={`w-4 h-4 rounded border flex items-center justify-center flex-shrink-0 transition-colors ${selected ? accentClass.check : 'border-white/20 bg-transparent'
        }`}>
        {selected && (
          <svg className="w-2.5 h-2.5 text-black" fill="none" viewBox="0 0 10 10">
            <path d="M1.5 5l2.5 2.5 4.5-4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        )}
      </div>
      <div className={`w-7 h-7 rounded-lg ${accentClass.bg} flex items-center justify-center text-[9px] font-bold ${accentClass.text} flex-shrink-0`}>
        {c.full_name.split(' ').map((n: string) => n[0]).join('').slice(0, 2)}
      </div>
      <div className="min-w-0 flex-1">
        <div className="text-xs font-semibold text-white/85 truncate">{c.full_name}</div>
        <div className="text-[9px] text-white/40 truncate uppercase tracking-wide">{c.role_title}</div>
        {c.importance_note && (
          <div className="text-[9px] text-white/30 italic truncate mt-0.5">{c.importance_note}</div>
        )}
      </div>
      <div className="flex items-center gap-1.5 flex-shrink-0">
        {c.is_new && (
          <span className="text-[7px] font-bold px-1.5 py-0.5 rounded bg-emerald-400/20 text-emerald-400 uppercase tracking-widest border border-emerald-400/20">
            NEW
          </span>
        )}
        {c.linkedin_url && (
          <a
            href={c.linkedin_url}
            target="_blank"
            rel="noopener noreferrer"
            onClick={e => e.stopPropagation()}
            className="text-[8px] font-bold text-white/25 hover:text-teal-400 uppercase tracking-widest transition-colors px-1.5 py-0.5 rounded border border-white/10 hover:border-teal-400/30"
          >
            LI
          </a>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Decision-maker tier classifier (client-side, no extra LLM call)
// ---------------------------------------------------------------------------
function getDMTier(roleTitle: string): 0 | 1 | 2 | 3 {
  const t = (roleTitle || '').toLowerCase();
  if (/\b(ceo|cfo|cmo|cto|cdo|coo|cpo|cro|ciso|chief)\b/.test(t)) return 0;
  if (/\b(vp|vice president|head of|president|gm|general manager)\b/.test(t)) return 1;
  if (/\bdirector\b/.test(t)) return 2;
  return 3;
}
const TIER_META = [
  { label: 'C-Suite', color: 'text-amber-400', border: 'border-amber-400/20', bg: 'bg-amber-400/5' },
  { label: 'VP & Head', color: 'text-violet-400', border: 'border-violet-400/20', bg: 'bg-violet-400/5' },
  { label: 'Director', color: 'text-teal-400', border: 'border-teal-400/20', bg: 'bg-teal-400/5' },
  { label: 'Others', color: 'text-white/35', border: 'border-white/10', bg: 'bg-white/[0.02]' },
] as const;

// ---------------------------------------------------------------------------
// Scout chat types
// ---------------------------------------------------------------------------
interface ScoutCandidate {
  full_name: string;
  role_title: string;
  company: string;
  linkedin_url: string;
  linkedin_verified: boolean;
  linkedin_status?: string;
  employment_verified?: string;
  actual_title?: string;
  email?: string;
  email_status?: string;
  buying_role?: string;
  source: string;
  confidence: string;
  exists_in_sheet?: boolean;
  sheet_name?: string;
  sheet_row?: number;
  company_domain?: string;
  company_account_type?: string;
  company_account_size?: string;
  added?: boolean;
  sendStatus?: 'idle' | 'sending' | 'sent' | 'error' | 'duplicate';
}
interface ScoutMessage {
  role: 'user' | 'assistant';
  content: string;
  candidates?: ScoutCandidate[];
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function SearcherPage() {
  const router = useRouter();
  const {
    companies, dmRoles, autoMode, autoVeri,
    running, threadId, error, result, veriThreadId,
    scanProgress, elapsedSecs, scanDone,
    liveContacts, recentActivity,
    roleEvent, selectedBuckets: selectedBucketsArr, roleSubmitting,
    selectionEvent, selectedIndices: selectedIndicesArr, selSubmitting,
    contactTab, leftTab,
    findMorePrompt, findMoreLoading,
    scoutMessages, scoutAdded,
  } = useSearcherStore();

  // Convert arrays → Sets for existing code compatibility
  const selectedBuckets = new Set(selectedBucketsArr);
  const selectedIndices = new Set(selectedIndicesArr);

  const setCompanies    = (v: string) => useSearcherStore.setState({ companies: v });
  const setDmRoles      = (v: string) => useSearcherStore.setState({ dmRoles: v });
  const setAutoMode     = (fn: boolean | ((v: boolean) => boolean)) =>
    useSearcherStore.setState(s => ({ autoMode: typeof fn === 'function' ? fn(s.autoMode) : fn }));
  const setAutoVeri     = (fn: boolean | ((v: boolean) => boolean)) =>
    useSearcherStore.setState(s => ({ autoVeri: typeof fn === 'function' ? fn(s.autoVeri) : fn }));
  const setRunning      = (v: boolean) => useSearcherStore.setState({ running: v });
  const setThreadId     = (v: string | null) => useSearcherStore.setState({ threadId: v });
  const setError        = (v: string | null) => useSearcherStore.setState({ error: v });
  const setResult       = (v: any) => useSearcherStore.setState({ result: v });
  const setVeriThreadId = (v: string | null) => useSearcherStore.setState({ veriThreadId: v });
  const setScanProgress = (fn: Record<string, string> | ((p: Record<string, string>) => Record<string, string>)) =>
    useSearcherStore.setState(s => ({ scanProgress: typeof fn === 'function' ? fn(s.scanProgress) : fn }));
  const setElapsedSecs  = (fn: number | ((n: number) => number)) =>
    useSearcherStore.setState(s => ({ elapsedSecs: typeof fn === 'function' ? fn(s.elapsedSecs) : fn }));
  const setScanDone     = (v: boolean) => useSearcherStore.setState({ scanDone: v });
  const setLiveContacts = (fn: ContactData[] | ((p: ContactData[]) => ContactData[])) =>
    useSearcherStore.setState(s => ({ liveContacts: typeof fn === 'function' ? fn(s.liveContacts) : fn }));
  const setRecentActivity = (fn: string[] | ((p: string[]) => string[])) =>
    useSearcherStore.setState(s => ({ recentActivity: typeof fn === 'function' ? fn(s.recentActivity) : fn }));
  const setRoleEvent    = (v: RoleSelectionEvent | null) => useSearcherStore.setState({ roleEvent: v });
  const setSelectedBuckets = (fn: Set<string> | ((p: Set<string>) => Set<string>)) =>
    useSearcherStore.setState(s => {
      const prev = new Set(s.selectedBuckets);
      const next = typeof fn === 'function' ? fn(prev) : fn;
      return { selectedBuckets: Array.from(next) };
    });
  const setRoleSubmitting = (v: boolean) => useSearcherStore.setState({ roleSubmitting: v });
  const setSelectionEvent = (v: ContactSelectionEvent | null) => useSearcherStore.setState({ selectionEvent: v });
  const setSelectedIndices = (fn: Set<number> | ((p: Set<number>) => Set<number>)) =>
    useSearcherStore.setState(s => {
      const prev = new Set(s.selectedIndices);
      const next = typeof fn === 'function' ? fn(prev) : fn;
      return { selectedIndices: Array.from(next) };
    });
  const setSelSubmitting  = (v: boolean) => useSearcherStore.setState({ selSubmitting: v });
  const setContactTab     = (v: 'matched' | 'bonus' | 'all') => useSearcherStore.setState({ contactTab: v });
  const setLeftTab        = (v: 'config' | 'scout') => useSearcherStore.setState({ leftTab: v });
  const setFindMorePrompt = (v: string) => useSearcherStore.setState({ findMorePrompt: v });
  const setFindMoreLoading = (v: boolean) => useSearcherStore.setState({ findMoreLoading: v });
  const setScoutMessages  = (fn: ScoutMessage[] | ((p: ScoutMessage[]) => ScoutMessage[])) =>
    useSearcherStore.setState(s => ({ scoutMessages: typeof fn === 'function' ? fn(s.scoutMessages) : fn }));
  const setScoutAdded     = (fn: ScoutCandidate[] | ((p: ScoutCandidate[]) => ScoutCandidate[])) =>
    useSearcherStore.setState(s => ({ scoutAdded: typeof fn === 'function' ? fn(s.scoutAdded) : fn }));

  const elapsedRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [scoutInput, setScoutInput] = useState('');
  const [scoutLoading, setScoutLoading] = useState(false);
  const scoutScrollRef = useRef<HTMLDivElement>(null);

  const handleEvent = useCallback((msg: any) => {
    // Capture log messages for the live activity panel
    if (msg.type === 'log') {
      const text: string = msg.data?.message || msg.message || '';
      if (text) {
        setRecentActivity(prev => [text, ...prev].slice(0, 8));
      }
    }
    if (msg.type === 'completed') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setScanProgress(prev => {
        const next = { ...prev };
        Object.keys(next).forEach(k => { if (next[k] !== 'error') next[k] = 'done'; });
        return next;
      });
      setRunning(false);
      setThreadId(null);
      setResult(msg.data);
      setScanDone(true);
      setSelectionEvent(null);
      if (msg.data?.veri_thread_id) {
        setVeriThreadId(msg.data.veri_thread_id);
        // Sync to Veri store so navigating to /veri (or being already there) auto-connects
        useVeriStore.setState({
          threadId: msg.data.veri_thread_id,
          running: true,
          result: null,
          error: null,
          cancelled: false,
          paused: false,
        });
      }
    } else if (msg.type === 'paused') {
      useSearcherStore.setState({ paused: true });
    } else if (msg.type === 'resumed') {
      useSearcherStore.setState({ paused: false });
    } else if (msg.type === 'cancelled') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setRunning(false);
      setSelectionEvent(null);
      setRoleEvent(null);
      useSearcherStore.setState({ cancelled: true, paused: false });
    } else if (msg.type === 'error') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setRunning(false);
      if (typeof window !== 'undefined') localStorage.removeItem('searcher_thread_id');
      setError(msg.data.error);
    } else if (msg.type === 'company_progress') {
      const { company, status } = msg.data;
      setScanProgress(prev => ({ ...prev, [company]: status }));
    } else if (msg.type === 'contact_written') {
      setLiveContacts(prev => [msg.data, ...prev]);
    } else if (msg.type === 'role_selection_required') {
      const evt = msg.data as RoleSelectionEvent;
      setRoleEvent(evt);
      // Pre-select default buckets
      setSelectedBuckets(new Set(evt.buckets.filter(b => b.pre_selected).map(b => b.id)));
    } else if (msg.type === 'contact_selection_required') {
      const evt = msg.data as ContactSelectionEvent;
      // Clear role selection panel (step 1 done) and unlock the button
      setRoleSubmitting(false);
      setRoleEvent(null);
      // Pre-select all matched contacts; keep previously selected + add new ones
      setSelectedIndices(prev => {
        const next = new Set(prev);
        evt.candidates.filter(c => c.pre_selected || c.is_new).forEach(c => next.add(c.index));
        return next;
      });
      setSelectionEvent(evt);
      setFindMoreLoading(false);
      setFindMorePrompt('');
    }
  }, []);

  const handleSelectionSubmit = async (skipAll = false) => {
    if (!threadId || !selectionEvent) return;
    setSelSubmitting(true);
    try {
      const indices = skipAll
        ? selectionEvent.candidates.filter(c => c.pre_selected).map(c => c.index)
        : Array.from(selectedIndices);
      await fetch(apiUrl(`/api/searcher/${threadId}/select-dms`), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_indices: indices }),
      });
      setSelectionEvent(null);
      setSelectedIndices(new Set());
    } catch (_) { /* silent — backend will timeout gracefully */ }
    finally { setSelSubmitting(false); }
  };

  const handleFindMore = async () => {
    if (!threadId || !findMorePrompt.trim() || findMoreLoading) return;
    setFindMoreLoading(true);
    try {
      await fetch(apiUrl(`/api/searcher/${threadId}/find-more`), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: findMorePrompt.trim() }),
      });
      // findMoreLoading stays true until backend re-emits contact_selection_required
    } catch (_) {
      setFindMoreLoading(false);
    }
  };

  const toggleCandidate = (idx: number) => {
    setSelectedIndices(prev => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx); else next.add(idx);
      return next;
    });
  };

  const handleRoleSelectionSubmit = async () => {
    if (!threadId || !roleEvent || roleSubmitting) return;
    setRoleSubmitting(true);
    try {
      await fetch(apiUrl(`/api/searcher/${threadId}/select-roles`), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected_bucket_ids: Array.from(selectedBuckets) }),
      });
      // Keep roleSubmitting=true — button stays locked until contact_selection_required clears it
    } catch (_) {
      setRoleSubmitting(false); // only reset on error so user can retry
    }
  };

  const toggleBucket = (id: string) => {
    setSelectedBuckets(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const handleScout = async () => {
    const q = scoutInput.trim();
    if (!q || scoutLoading) return;
    const userMsg: ScoutMessage = { role: 'user', content: q };
    setScoutMessages(prev => [...prev, userMsg]);
    setScoutInput('');
    setScoutLoading(true);
    try {
      const resp = await fetch(apiUrl('/api/searcher/prospect-chat'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: q,
          company: companies.trim(),
          history: scoutMessages.map(m => ({ role: m.role, content: m.content })),
        }),
      });
      if (!resp.ok) {
        let detail = `Server error (${resp.status})`;
        try {
          const errBody = await resp.json();
          detail = errBody.detail || detail;
        } catch {
          // response wasn't JSON (e.g. proxy failure)
        }
        throw new Error(detail);
      }
      const data = await resp.json();
      // Attach company context to each candidate so commit has it
      const candidates: ScoutCandidate[] = (data.candidates || []).map((c: ScoutCandidate) => ({
        ...c,
        company_domain: c.company_domain || data.company_domain || '',
        company_account_type: c.company_account_type || data.company_account_type || '',
        company_account_size: c.company_account_size || data.company_account_size || '',
      }));
      const assistantMsg: ScoutMessage = {
        role: 'assistant',
        content: data.message || 'Done.',
        candidates,
      };
      setScoutMessages(prev => [...prev, assistantMsg]);
      setTimeout(() => {
        if (scoutScrollRef.current) {
          scoutScrollRef.current.scrollTop = scoutScrollRef.current.scrollHeight;
        }
      }, 50);
    } catch (e: any) {
      setScoutMessages(prev => [...prev, { role: 'assistant', content: `Error: ${e.message}` }]);
    } finally {
      setScoutLoading(false);
    }
  };

  const handleScoutAdd = (candidate: ScoutCandidate) => {
    setScoutAdded(prev => {
      if (prev.some(c => c.full_name === candidate.full_name)) return prev;
      return [...prev, { ...candidate, added: true, sendStatus: 'idle' }];
    });
  };

  const handleScoutSendToSheet = async (idx: number) => {
    const candidate = scoutAdded[idx];
    if (!candidate || candidate.sendStatus === 'sending' || candidate.sendStatus === 'sent') return;
    setScoutAdded(prev => prev.map((c, i) => i === idx ? { ...c, sendStatus: 'sending' } : c));
    try {
      const resp = await fetch(apiUrl('/api/searcher/scout-commit'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          full_name: candidate.full_name,
          role_title: candidate.role_title,
          company: candidate.company,
          linkedin_url: candidate.linkedin_url,
          linkedin_verified: candidate.linkedin_verified,
          linkedin_status: candidate.linkedin_status || '',
          employment_verified: candidate.employment_verified || '',
          title_match: candidate.title_match || '',
          actual_title: candidate.actual_title || '',
          email: candidate.email || '',
          email_status: candidate.email_status || '',
          buying_role: candidate.buying_role || '',
          source: candidate.source,
          confidence: candidate.confidence,
          company_domain: candidate.company_domain || '',
          company_account_type: candidate.company_account_type || '',
          company_account_size: candidate.company_account_size || '',
        }),
      });
      const data = resp.ok ? await resp.json() : null;
      if (data?.status === 'duplicate') {
        setScoutAdded(prev => prev.map((c, i) => i === idx ? { ...c, sendStatus: 'duplicate' } : c));
      } else if (resp.ok) {
        setScoutAdded(prev => prev.map((c, i) => i === idx ? { ...c, sendStatus: 'sent' } : c));
      } else {
        setScoutAdded(prev => prev.map((c, i) => i === idx ? { ...c, sendStatus: 'error' } : c));
      }
    } catch {
      setScoutAdded(prev => prev.map((c, i) => i === idx ? { ...c, sendStatus: 'error' } : c));
    }
  };

  const handleStop = async () => {
    if (!threadId) return;
    try { await fetch(apiUrl(`/api/runs/${threadId}/cancel`), { method: 'POST' }); } catch { /* ignore */ }
  };

  const handlePauseResume = async () => {
    if (!threadId) return;
    const paused = useSearcherStore.getState().paused;
    try { await fetch(apiUrl(`/api/runs/${threadId}/${paused ? 'resume' : 'pause'}`), { method: 'POST' }); } catch { /* ignore */ }
  };

  const handleRun = async () => {
    useSearcherStore.setState({ cancelled: false });
    if (!companies.trim()) return;
    setRunning(true);
    setResult(null);
    setError(null);
    setScanDone(false);
    setVeriThreadId(null);
    setLiveContacts([]);
    setRecentActivity([]);

    const list = companies.split(',').map(c => c.trim()).filter(Boolean);
    const initial: Record<string, string> = {};
    list.forEach(c => { initial[c] = 'queued'; });
    setScanProgress(initial);
    setElapsedSecs(0);
    if (elapsedRef.current) clearInterval(elapsedRef.current);
    elapsedRef.current = setInterval(() => setElapsedSecs(s => s + 1), 1000);

    try {
      const resp = await fetch(apiUrl('/api/searcher/run'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companies: companies.trim(),
          dm_roles: dmRoles.trim() || DEFAULT_DM_ROLES,
          auto_approve: autoMode,
          auto_trigger_veri: autoVeri,
        }),
      });
      const data = await resp.json();
      setThreadId(data.thread_id);
    } catch (e: any) {
      setError(e.message);
      setRunning(false);
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
    }
  };

  const companyList = companies.split(',').map(c => c.trim()).filter(Boolean);
  const contacts: ContactData[] = result?.contacts || [];
  const totalWritten: number = result?.contacts_appended || contacts.length;
  const doneCount = Object.values(scanProgress).filter(s => s === 'done' || s === 'error').length;

  return (
    <div className="h-screen overflow-hidden flex flex-col px-6 pt-4 -mb-20 max-w-[1600px] mx-auto font-sans">

      {/* ── Header — compact single row ── */}
      <div className="flex items-center justify-between mb-3 pb-3 border-b border-white/[0.06] flex-shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.4em]">Module 02 · Contact Discovery</span>
          <span className="h-3 w-[1px] bg-white/10" />
          <h1 className="text-base font-bold text-white tracking-tight">
            Pipeline<span className="text-white/50 font-light"> / </span>Searcher
          </h1>
        </div>
        <div className="flex items-center gap-4">
          {running && (
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <div className="relative flex h-1.5 w-1.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-teal-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-teal-400" />
                </div>
                <span className="text-[9px] font-bold text-white/75 uppercase tracking-[0.25em]">Discovering</span>
              </div>
              <button
                onClick={handlePauseResume}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-lg border text-[9px] font-bold uppercase tracking-widest transition-colors ${
                  useSearcherStore.getState().paused
                    ? 'border-teal-500/30 bg-teal-500/10 text-teal-400 hover:bg-teal-500/20'
                    : 'border-amber-500/30 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20'
                }`}
              >
                <span>{useSearcherStore.getState().paused ? '▶' : '⏸'}</span>
                <span>{useSearcherStore.getState().paused ? 'Resume' : 'Pause'}</span>
              </button>
              <button
                onClick={handleStop}
                className="flex items-center gap-1.5 px-3 py-1 rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 text-[9px] font-bold uppercase tracking-widest hover:bg-red-500/20 transition-colors"
              >
                <span>■</span><span>Stop</span>
              </button>
            </div>
          )}
          {useSearcherStore.getState().cancelled && !running && (
            <span className="text-[9px] font-bold text-red-400/70 uppercase tracking-widest">Stopped</span>
          )}
          {scanDone && result && (
            <div className="flex items-center gap-3">
              <div className="text-right">
                <div className="text-xs font-bold text-white">{companyList.length}</div>
                <div className="text-[9px] text-white/50 uppercase tracking-widest">Scanned</div>
              </div>
              <div className="h-4 w-[1px] bg-white/10" />
              <div className="text-right">
                <div className="text-xs font-bold text-teal-400">{totalWritten}</div>
                <div className="text-[9px] text-white/50 uppercase tracking-widest">Written</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── Main 2-col grid — fills remaining height ── */}
      <div className="flex-1 min-h-0 overflow-hidden grid grid-cols-1 xl:grid-cols-5 gap-4">

        {/* Left: Tab bar → Config or AI Scout */}
        <div className="xl:col-span-2 flex flex-col min-h-0 overflow-hidden border border-white/[0.07] rounded-2xl bg-white/[0.02]">

          {/* Tab bar */}
          <div className="flex-shrink-0 flex border-b border-white/[0.06]">
            <button
              onClick={() => setLeftTab('config')}
              className={`flex-1 py-2.5 text-[9px] font-bold uppercase tracking-[0.2em] transition-colors border-b-2 ${leftTab === 'config' ? 'text-white border-white' : 'text-white/30 border-transparent hover:text-white/55'}`}
            >
              Config
            </button>
            <button
              onClick={() => setLeftTab('scout')}
              className={`flex-1 py-2.5 text-[9px] font-bold uppercase tracking-[0.2em] transition-colors border-b-2 flex items-center justify-center gap-1.5 ${leftTab === 'scout' ? 'text-violet-400 border-violet-400' : 'text-white/30 border-transparent hover:text-white/55'}`}
            >
              <span>AI Scout</span>
              {scoutAdded.length > 0 && (
                <span className="text-[8px] bg-violet-400/20 text-violet-400 px-1.5 py-0.5 rounded font-mono">{scoutAdded.length}</span>
              )}
            </button>
          </div>

          {/* ── Config tab ── */}
          {leftTab === 'config' && (
            <div className="flex flex-col flex-1 min-h-0 overflow-hidden gap-3 p-4">

              {/* Target accounts */}
              <div className="flex-shrink-0">
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-[9px] text-white/55 uppercase tracking-widest">Target Accounts</span>
                  <span className="text-[8px] text-teal-400/50 font-mono">sourced from Fini</span>
                </div>
                <textarea
                  className="w-full bg-black/30 border border-white/[0.06] rounded-xl px-3 py-2 text-xs text-white placeholder-white/15 outline-none focus:border-white/20 transition-colors resize-none leading-relaxed"
                  rows={2}
                  placeholder="Company A, Company B, ..."
                  value={companies}
                  onChange={e => setCompanies(e.target.value)}
                  disabled={running}
                />
              </div>

              {/* DM roles */}
              <div className="flex-shrink-0">
                <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1.5">Gap-Fill Persona Stack</div>
                <textarea
                  className="w-full bg-black/30 border border-white/[0.06] rounded-xl px-3 py-2 text-[11px] text-white/80 outline-none focus:border-white/20 transition-colors resize-none leading-relaxed"
                  rows={2}
                  value={dmRoles}
                  onChange={e => setDmRoles(e.target.value)}
                  disabled={running}
                />
              </div>

              {/* Mode toggles + Run button */}
              <div className="flex-shrink-0 flex flex-col gap-2">
                <div className="flex items-center gap-2">
                  {/* Searcher: Auto / Manual toggle */}
                  <button
                    onClick={() => setAutoMode(v => !v)}
                    disabled={running}
                    className="flex items-center gap-2 px-3 py-1.5 rounded-xl border transition-all duration-200 disabled:opacity-40"
                    style={autoMode
                      ? { borderColor: 'rgba(52,211,153,0.35)', background: 'rgba(52,211,153,0.07)' }
                      : { borderColor: 'rgba(255,255,255,0.08)', background: 'transparent' }}
                  >
                    <div className={`w-7 h-3.5 rounded-full relative transition-colors duration-200 ${autoMode ? 'bg-teal-400/60' : 'bg-white/10'}`}>
                      <div className={`absolute top-0.5 w-2.5 h-2.5 rounded-full transition-all duration-200 ${autoMode ? 'left-[14px] bg-teal-300' : 'left-0.5 bg-white/40'}`} />
                    </div>
                    <span className={`text-[9px] font-bold uppercase tracking-widest transition-colors ${autoMode ? 'text-teal-400' : 'text-white/35'}`}>
                      Searcher: {autoMode ? 'Auto' : 'Manual'}
                    </span>
                  </button>
                  <span className={`text-[8px] ${autoMode ? 'text-teal-400/50' : 'text-white/20'}`}>
                    {autoMode ? 'Writes all matched — no pauses' : 'Pauses for role & contact approval'}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  {/* Veri: Auto / Manual toggle */}
                  <button
                    onClick={() => setAutoVeri(v => !v)}
                    disabled={running}
                    className="flex items-center gap-2 px-3 py-1.5 rounded-xl border transition-all duration-200 disabled:opacity-40"
                    style={autoVeri
                      ? { borderColor: 'rgba(167,139,250,0.35)', background: 'rgba(167,139,250,0.07)' }
                      : { borderColor: 'rgba(255,255,255,0.08)', background: 'transparent' }}
                  >
                    <div className={`w-7 h-3.5 rounded-full relative transition-colors duration-200 ${autoVeri ? 'bg-violet-400/60' : 'bg-white/10'}`}>
                      <div className={`absolute top-0.5 w-2.5 h-2.5 rounded-full transition-all duration-200 ${autoVeri ? 'left-[14px] bg-violet-300' : 'left-0.5 bg-white/40'}`} />
                    </div>
                    <span className={`text-[9px] font-bold uppercase tracking-widest transition-colors ${autoVeri ? 'text-violet-400' : 'text-white/35'}`}>
                      Veri: {autoVeri ? 'Auto' : 'Skip'}
                    </span>
                  </button>
                  <span className={`text-[8px] ${autoVeri ? 'text-violet-400/50' : 'text-white/20'}`}>
                    {autoVeri ? 'Auto-verifies contacts after discovery' : 'Veri skipped — run manually later'}
                  </span>
                </div>
                <div className="flex justify-end">
                  <button
                    onClick={handleRun}
                    disabled={running || !companies.trim()}
                    className="flex items-center gap-2 px-5 py-2 rounded-xl bg-white text-black text-[10px] font-bold uppercase tracking-[0.12em] hover:bg-white/90 disabled:bg-white/10 disabled:text-white/55 transition-all duration-200"
                  >
                    {running ? (
                      <>
                        <div className="w-2.5 h-2.5 border-2 border-black/20 border-t-black rounded-full animate-spin" />
                        <span>Scanning…</span>
                      </>
                    ) : (
                      <span>Run Discovery</span>
                    )}
                  </button>
                </div>
              </div>

              {/* Pipeline log — fills remaining space */}
              {threadId ? (
                <div className="flex-1 min-h-0 border border-white/[0.06] rounded-xl bg-black/20 overflow-hidden flex flex-col">
                  <LogStream threadId={threadId} onEvent={handleEvent} />
                  {error && (
                    <div className="px-4 py-2 border-t border-red-900/40 bg-red-950/30 text-[11px] text-red-400 flex-shrink-0">{error}</div>
                  )}
                </div>
              ) : (
                <div className="flex-1 min-h-0 overflow-y-auto no-scrollbar">
                  <div className="border border-white/[0.05] rounded-xl overflow-hidden">
                    <div className="px-4 py-2.5 border-b border-white/[0.04]">
                      <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.3em]">Discovery Engine</span>
                    </div>
                    <div className="grid grid-cols-2 divide-x divide-y divide-white/[0.03]">
                      {DISCOVERY_STEPS.map((s, i) => (
                        <div key={i} className="px-4 py-3">
                          <div className="text-[9px] font-mono text-white/20 mb-0.5">{s.step}</div>
                          <div className="text-[10px] text-white/65 font-medium">{s.label}</div>
                          <div className="text-[8px] font-mono text-white/30 uppercase tracking-tight mt-0.5">{s.op}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {/* Veri auto-triggered */}
              {veriThreadId && (
                <div className="flex-shrink-0 border border-emerald-400/15 rounded-xl bg-emerald-400/[0.03] overflow-hidden">
                  <div className="px-4 py-3 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className="relative flex h-1.5 w-1.5">
                        <span className="animate-ping absolute inset-0 rounded-full bg-emerald-400 opacity-75" />
                        <span className="relative block h-1.5 w-1.5 rounded-full bg-emerald-400" />
                      </div>
                      <div>
                        <div className="text-[9px] font-bold text-emerald-400/80 uppercase tracking-[0.2em]">Veri Auto-Triggered</div>
                        <div className="text-[10px] text-white/40 mt-0.5">Verification initiated for all discovered prospects.</div>
                      </div>
                    </div>
                    <button
                      onClick={() => router.push(`/veri?threadId=${veriThreadId}`)}
                      className="px-3 py-1.5 rounded-lg border border-emerald-400/20 bg-emerald-400/10 text-[10px] font-bold text-emerald-400/80 uppercase tracking-wider hover:bg-emerald-400/15 transition-colors flex-shrink-0"
                    >
                      Open Veri →
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* ── AI Scout tab ── */}
          {leftTab === 'scout' && (
            <div className="flex flex-col flex-1 min-h-0 overflow-hidden">

              {/* Empty state / intro */}
              {scoutMessages.length === 0 && (
                <div className="flex-shrink-0 px-5 pt-4 pb-3">
                  <div className="rounded-xl border border-violet-400/15 bg-violet-400/[0.04] p-3.5">
                    <div className="text-[10px] font-bold text-violet-300/80 uppercase tracking-widest mb-1.5">AI Prospect Scout</div>
                    <div className="text-[10px] text-white/45 leading-relaxed">
                      Chat to find specific contacts. Try:<br />
                      <span className="text-violet-400/70">&ldquo;Find the CMO and VP Sales at Diageo España&rdquo;</span><br />
                      <span className="text-violet-400/70">&ldquo;Who&apos;s the Head of Digital at LVMH?&rdquo;</span><br />
                      <span className="text-violet-400/70">&ldquo;CFO and CTO at Unilever UK&rdquo;</span>
                    </div>
                  </div>
                </div>
              )}

              {/* Chat messages */}
              <div ref={scoutScrollRef} className="flex-1 min-h-0 overflow-y-auto no-scrollbar px-4 py-3 space-y-3">
                {scoutMessages.map((msg, i) => (
                  <div key={i} className={`flex flex-col gap-2 ${msg.role === 'user' ? 'items-end' : 'items-start'}`}>
                    <div className={`max-w-[85%] px-3 py-2 rounded-xl text-[11px] leading-relaxed ${msg.role === 'user'
                      ? 'bg-white/10 text-white/90 rounded-br-sm'
                      : 'bg-violet-400/10 border border-violet-400/15 text-white/75 rounded-bl-sm'
                      }`}>
                      {msg.content}
                    </div>
                    {/* Candidate cards from assistant */}
                    {msg.role === 'assistant' && msg.candidates && msg.candidates.length > 0 && (
                      <div className="w-full space-y-2">
                        {msg.candidates.map((c, ci) => {
                          const alreadyAdded = scoutAdded.some(a => a.full_name === c.full_name);
                          const liConfirmed = c.linkedin_status === 'CONFIRMED';
                          const dmRole = c.buying_role === 'Decision Maker';
                          const emailOk = c.email_status === 'valid' || c.email_status === 'catch-all';
                          return (
                            <div key={ci} className={`rounded-xl overflow-hidden border ${c.exists_in_sheet ? 'border-amber-500/30 bg-amber-500/[0.03]' : 'border-white/[0.07] bg-white/[0.02]'}`}>
                              {/* Duplicate warning banner */}
                              {c.exists_in_sheet && (
                                <div className="px-3 py-1.5 border-b border-amber-500/20 flex items-center gap-1.5">
                                  <span className="text-amber-400 text-[9px]">⚠</span>
                                  <span className="text-[8px] text-amber-400/80 uppercase tracking-wide font-bold">
                                    Already in {c.sheet_name}{c.sheet_row ? ` · row ${c.sheet_row}` : ''}
                                  </span>
                                </div>
                              )}
                              <div className="flex items-start gap-2.5 px-3 py-2.5">
                                <div className="w-7 h-7 rounded-lg bg-violet-400/15 flex items-center justify-center text-[9px] font-bold text-violet-400/80 flex-shrink-0 mt-0.5">
                                  {c.full_name.split(' ').map((n: string) => n[0]).join('').slice(0, 2)}
                                </div>
                                <div className="min-w-0 flex-1">
                                  <div className="flex items-center gap-1.5 flex-wrap mb-0.5">
                                    <span className="text-[11px] font-semibold text-white/85">{c.full_name}</span>
                                    {/* Buying role badge */}
                                    {c.buying_role && (
                                      <span className={`text-[7px] font-bold px-1 py-0.5 rounded uppercase tracking-widest ${dmRole ? 'bg-violet-400/20 text-violet-400' : 'bg-white/[0.06] text-white/30'}`}>
                                        {dmRole ? 'DM' : 'Inf'}
                                      </span>
                                    )}
                                  </div>
                                  <div className="text-[9px] text-white/40 uppercase tracking-wide truncate">{c.role_title}</div>
                                  {/* Email row */}
                                  {c.email && (
                                    <div className="flex items-center gap-1 mt-1">
                                      <div className={`w-1 h-1 rounded-full flex-shrink-0 ${emailOk ? 'bg-emerald-400' : c.email_status === 'unknown' ? 'bg-amber-400' : 'bg-red-400'}`} />
                                      <span className="text-[9px] font-mono text-white/40 truncate">{c.email}</span>
                                    </div>
                                  )}
                                </div>
                                <div className="flex flex-col items-end gap-1 flex-shrink-0">
                                  <div className="flex items-center gap-1">
                                    {/* LinkedIn button + verified badge */}
                                    {c.linkedin_url && (
                                      <a
                                        href={c.linkedin_url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        onClick={e => e.stopPropagation()}
                                        className={`text-[8px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded border transition-colors ${liConfirmed ? 'border-emerald-400/30 text-emerald-400 hover:bg-emerald-400/10' : 'border-white/10 text-white/25 hover:text-teal-400 hover:border-teal-400/30'}`}
                                      >
                                        {liConfirmed ? '✓ LI' : 'LI'}
                                      </a>
                                    )}
                                    {/* Confidence */}
                                    <span className={`text-[7px] font-bold px-1.5 py-0.5 rounded uppercase tracking-widest ${c.confidence === 'high' ? 'bg-emerald-400/15 text-emerald-400' : c.confidence === 'medium' ? 'bg-amber-400/15 text-amber-400' : 'bg-white/[0.06] text-white/30'}`}>
                                      {c.confidence}
                                    </span>
                                  </div>
                                  <button
                                    onClick={() => handleScoutAdd(c)}
                                    disabled={alreadyAdded || !!c.exists_in_sheet}
                                    className={`text-[8px] font-bold uppercase tracking-widest px-2.5 py-1 rounded-lg transition-all ${
                                      alreadyAdded ? 'bg-white/5 text-white/20 cursor-default'
                                      : c.exists_in_sheet ? 'bg-amber-400/10 text-amber-400/50 cursor-default'
                                      : 'bg-violet-400/20 text-violet-400 hover:bg-violet-400/30 border border-violet-400/20'
                                    }`}
                                  >
                                    {alreadyAdded ? '✓' : c.exists_in_sheet ? 'Exists' : '+ Add'}
                                  </button>
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                ))}
                {scoutLoading && (
                  <div className="flex items-start">
                    <div className="bg-violet-400/10 border border-violet-400/15 rounded-xl rounded-bl-sm px-3 py-2.5 flex items-center gap-2">
                      <div className="flex gap-1">
                        <div className="w-1 h-1 rounded-full bg-violet-400/60 animate-bounce" style={{ animationDelay: '0ms' }} />
                        <div className="w-1 h-1 rounded-full bg-violet-400/60 animate-bounce" style={{ animationDelay: '150ms' }} />
                        <div className="w-1 h-1 rounded-full bg-violet-400/60 animate-bounce" style={{ animationDelay: '300ms' }} />
                      </div>
                      <span className="text-[10px] text-violet-400/60">Searching…</span>
                    </div>
                  </div>
                )}
              </div>

              {/* Added candidates summary */}
              {scoutAdded.length > 0 && (
                <div className="flex-shrink-0 border-t border-white/[0.06] px-4 py-2.5 bg-violet-400/[0.03]">
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-[8px] font-bold text-violet-400/70 uppercase tracking-[0.3em]">{scoutAdded.length} Added to List</span>
                    <button onClick={() => setScoutAdded([])} className="text-[8px] text-white/25 hover:text-white/50 uppercase tracking-widest transition-colors">Clear</button>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {scoutAdded.map((c, i) => (
                      <span key={i} className="text-[8px] bg-violet-400/10 border border-violet-400/15 text-violet-300/70 px-2 py-0.5 rounded-full truncate max-w-[140px]">{c.full_name}</span>
                    ))}
                  </div>
                </div>
              )}

              {/* Chat input */}
              <div className="flex-shrink-0 border-t border-white/[0.06] p-3 flex items-end gap-2">
                <textarea
                  className="flex-1 bg-black/30 border border-white/[0.08] rounded-xl px-3 py-2 text-[11px] text-white/80 placeholder-white/20 outline-none focus:border-violet-400/30 transition-colors resize-none leading-relaxed"
                  placeholder={'Find the CMO at Diageo España…'}
                  rows={2}
                  value={scoutInput}
                  onChange={e => setScoutInput(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleScout(); } }}
                  disabled={scoutLoading}
                />
                <button
                  onClick={handleScout}
                  disabled={!scoutInput.trim() || scoutLoading}
                  className="flex-shrink-0 w-8 h-8 rounded-xl bg-violet-400 hover:bg-violet-300 disabled:bg-white/10 disabled:text-white/20 text-black flex items-center justify-center transition-all"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 16 16">
                    <path d="M2 8h12M9 3l5 5-5 5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Right: Queue Panel — fills full height */}
        <div className="xl:col-span-3 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden flex flex-col min-h-0">
          <div className="px-6 py-4 border-b border-white/[0.05] flex items-center justify-between">
            <div className="text-[10px] font-bold text-white/60 uppercase tracking-[0.35em]">
              {scanDone ? 'Discovery Results' : 'Pipeline Queue'}
            </div>
            {companyList.length > 0 && !scanDone && (
              <span className="text-[10px] font-bold text-white/55 uppercase tracking-widest">{companyList.length} queued</span>
            )}
            {scanDone && result && (
              <div className="flex items-center gap-4 text-right">
                <div><div className="text-sm font-bold text-teal-400">{totalWritten}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Written</div></div>
                <div className="h-4 w-[1px] bg-white/10" />
                <div><div className="text-sm font-bold text-white">{contacts.length}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Found</div></div>
                {veriThreadId && (
                  <>
                    <div className="h-4 w-[1px] bg-white/10" />
                    <div><div className="text-sm font-bold text-emerald-400">✓</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Veri On</div></div>
                  </>
                )}
              </div>
            )}
          </div>

          {/* ── Contact Selection Panel (Step 2) — fixed-footer layout ── */}
          {selectionEvent && (
            <div className="flex-1 min-h-0 overflow-hidden flex flex-col border-t border-white/[0.08] bg-slate-900/60 animate-[fadeIn_0.35s_ease]">

              {/* Header */}
              <div className="px-5 pt-3 pb-2.5 border-b border-white/[0.05] flex-shrink-0">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="flex items-center gap-2 mb-0.5">
                      <div className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />
                      <span className="text-[9px] font-bold text-white/80 uppercase tracking-[0.3em]">Review Discovery — {selectionEvent.company}</span>
                    </div>
                    <p className="text-[10px] text-white/40">
                      <span className="text-teal-400 font-bold">{selectionEvent.matched_count}</span> matched
                      {selectionEvent.bonus_count > 0 && <> + <span className="text-amber-400 font-bold">{selectionEvent.bonus_count}</span> bonus</>}
                      {' '}· choose who to enrich
                    </p>
                  </div>
                  <button
                    onClick={() => {
                      const all = new Set(selectionEvent.candidates.map(c => c.index));
                      setSelectedIndices(prev => prev.size === all.size ? new Set() : all);
                    }}
                    className="text-[9px] font-bold text-white/40 hover:text-white/70 uppercase tracking-widest px-2 py-1 rounded border border-white/10 hover:border-white/20 transition-colors flex-shrink-0"
                  >
                    {selectedIndices.size === selectionEvent.candidates.length ? 'None' : 'All'}
                  </button>
                </div>
              </div>

              {/* Tab bar */}
              <div className="flex-shrink-0 flex border-b border-white/[0.06] bg-black/20">
                {([
                  { key: 'matched', label: 'Matched', count: selectionEvent.matched_count, color: 'text-teal-400 border-teal-400' },
                  { key: 'bonus', label: 'Bonus', count: selectionEvent.bonus_count, color: 'text-amber-400 border-amber-400' },
                  { key: 'all', label: 'All', count: selectionEvent.total, color: 'text-white/60 border-white/40' },
                ] as const).map(tab => (
                  <button
                    key={tab.key}
                    onClick={() => setContactTab(tab.key)}
                    className={`flex items-center gap-1.5 px-4 py-2.5 text-[9px] font-bold uppercase tracking-[0.2em] border-b-2 transition-all ${contactTab === tab.key
                      ? `${tab.color} bg-white/[0.02]`
                      : 'text-white/30 border-transparent hover:text-white/50'
                      }`}
                  >
                    {tab.label}
                    <span className={`text-[8px] px-1.5 py-0.5 rounded font-mono ${contactTab === tab.key ? 'bg-white/10' : 'bg-white/[0.04] text-white/20'}`}>
                      {tab.count}
                    </span>
                  </button>
                ))}
                <div className="flex-1" />
                <button
                  onClick={() => {
                    const visible = selectionEvent.candidates.filter(c =>
                      contactTab === 'all' ? true : c.group === contactTab
                    );
                    const visibleSet = new Set(visible.map(c => c.index));
                    const allSelected = visible.every(c => selectedIndices.has(c.index));
                    setSelectedIndices(prev => {
                      const next = new Set(prev);
                      if (allSelected) visible.forEach(c => next.delete(c.index));
                      else visible.forEach(c => next.add(c.index));
                      return next;
                    });
                    void visibleSet;
                  }}
                  className="px-3 py-2 text-[8px] font-bold text-white/25 hover:text-white/55 uppercase tracking-widest transition-colors"
                >
                  {(() => {
                    const visible = selectionEvent.candidates.filter(c =>
                      contactTab === 'all' ? true : c.group === contactTab
                    );
                    return visible.every(c => selectedIndices.has(c.index)) ? 'None' : 'All';
                  })()}
                </button>
              </div>

              {/* Scrollable candidate list — grouped by DM tier */}
              <div className="flex-1 min-h-0 overflow-y-auto no-scrollbar">
                {(() => {
                  const visible = selectionEvent.candidates.filter(c =>
                    contactTab === 'all' ? true : c.group === contactTab
                  );
                  // Group by tier
                  const groups: DiscoveredCandidate[][] = [[], [], [], []];
                  visible.forEach(c => groups[getDMTier(c.role_title)].push(c));
                  const accent = (c: DiscoveredCandidate): 'teal' | 'amber' =>
                    c.group === 'matched' ? 'teal' : 'amber';
                  return groups.map((group, tier) => {
                    if (group.length === 0) return null;
                    const meta = TIER_META[tier];
                    return (
                      <div key={tier}>
                        {/* Tier header */}
                        <div className={`px-4 py-1.5 flex items-center gap-2 ${meta.bg} border-b ${meta.border}`}>
                          <span className={`text-[8px] font-black uppercase tracking-[0.35em] ${meta.color}`}>{meta.label}</span>
                          <span className={`text-[8px] font-mono ${meta.color} opacity-60`}>· {group.length}</span>
                          <div className="flex-1" />
                          <button
                            onClick={() => {
                              const allSel = group.every(c => selectedIndices.has(c.index));
                              setSelectedIndices(prev => {
                                const next = new Set(prev);
                                if (allSel) group.forEach(c => next.delete(c.index));
                                else group.forEach(c => next.add(c.index));
                                return next;
                              });
                            }}
                            className={`text-[7px] font-bold uppercase tracking-widest px-2 py-0.5 rounded border transition-colors ${meta.color} ${meta.border} hover:opacity-100 opacity-60`}
                          >
                            {group.every(c => selectedIndices.has(c.index)) ? 'Deselect' : 'Select All'}
                          </button>
                        </div>
                        {/* Rows */}
                        <div className="divide-y divide-white/[0.04]">
                          {group.map(c => (
                            <CandidateRow key={c.index} c={c} selected={selectedIndices.has(c.index)} onToggle={toggleCandidate} accent={accent(c)} />
                          ))}
                        </div>
                      </div>
                    );
                  });
                })()}
              </div>

              {/* ── Fixed footer: Find More + Process + Live Activity ── */}
              <div className="flex-shrink-0 border-t border-white/[0.08] bg-black/30">

                {/* Live Activity — compact, max 3 lines */}
                {recentActivity.length > 0 && (
                  <div className="px-4 pt-2 pb-1 flex flex-col gap-0.5 border-b border-white/[0.05]">
                    {recentActivity.slice(0, 3).map((msg, i) => {
                      const isSuccess = msg.includes('✓') || msg.includes('written to sheet');
                      return (
                        <div key={i} className={`text-[8px] font-mono leading-snug truncate transition-opacity ${i === 0 ? 'opacity-100' : i === 1 ? 'opacity-55' : 'opacity-30'
                          } ${isSuccess ? 'text-teal-400' : 'text-white/40'}`}>{msg}</div>
                      );
                    })}
                  </div>
                )}

                {/* Find More */}
                <div className="px-4 py-2.5 flex items-center gap-2 border-b border-white/[0.05]">
                  <input
                    type="text"
                    placeholder='e.g. "find the CEO and CFO" or "Head of Digital"'
                    value={findMorePrompt}
                    onChange={e => setFindMorePrompt(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') handleFindMore(); }}
                    disabled={findMoreLoading || selSubmitting}
                    className="flex-1 bg-black/30 border border-white/[0.08] rounded-lg px-3 py-1.5 text-[10px] text-white/70 placeholder-white/20 outline-none focus:border-white/20 transition-colors disabled:opacity-40"
                  />
                  <button
                    onClick={handleFindMore}
                    disabled={!findMorePrompt.trim() || findMoreLoading || selSubmitting}
                    className="flex items-center gap-1.5 text-[9px] font-bold text-white/60 hover:text-white uppercase tracking-widest px-3 py-1.5 rounded-lg border border-white/10 hover:border-white/25 disabled:opacity-30 transition-all flex-shrink-0"
                  >
                    {findMoreLoading ? (
                      <><div className="w-2.5 h-2.5 border border-white/30 border-t-white/70 rounded-full animate-spin" /><span>Searching…</span></>
                    ) : <span>Find More</span>}
                  </button>
                </div>

                {/* Process / Skip */}
                <div className="px-4 py-2.5 flex items-center justify-between">
                  <span className="text-[9px] text-white/30">
                    {selectedIndices.size} of {selectionEvent.total} selected
                  </span>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => handleSelectionSubmit(true)}
                      disabled={selSubmitting || findMoreLoading}
                      className="text-[9px] font-bold text-white/30 hover:text-white/55 uppercase tracking-widest px-3 py-1.5 rounded transition-colors disabled:opacity-40"
                    >Skip All</button>
                    <button
                      onClick={() => handleSelectionSubmit(false)}
                      disabled={selSubmitting || findMoreLoading || selectedIndices.size === 0}
                      className="text-[9px] font-bold text-black uppercase tracking-widest px-5 py-2 rounded-lg bg-white hover:bg-white/90 disabled:bg-white/10 disabled:text-white/20 transition-all"
                    >
                      {selSubmitting ? 'Starting…' : `Process ${selectedIndices.size} Contact${selectedIndices.size !== 1 ? 's' : ''}`}
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* ── Regular scrollable area (no contact selection active) ── */}
          {!selectionEvent && (
            <div className="flex-1 overflow-y-auto no-scrollbar">

              {/* ── Scout Queue — people added from AI Scout ── */}
              {scoutAdded.length > 0 && (
                <div className="border-b border-violet-400/15 bg-violet-400/[0.03]">
                  <div className="px-5 py-3 flex items-center justify-between border-b border-violet-400/10">
                    <div className="flex items-center gap-2">
                      <div className="w-1.5 h-1.5 rounded-full bg-violet-400" />
                      <span className="text-[9px] font-bold text-violet-400/80 uppercase tracking-[0.3em]">Scout Queue</span>
                      <span className="text-[8px] bg-violet-400/15 text-violet-400 px-1.5 py-0.5 rounded font-mono">{scoutAdded.length}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={async () => {
                          for (let i = 0; i < scoutAdded.length; i++) {
                            if (scoutAdded[i].sendStatus !== 'sent') {
                              await handleScoutSendToSheet(i);
                            }
                          }
                        }}
                        className="text-[8px] font-bold text-violet-400/60 hover:text-violet-400 uppercase tracking-widest px-2.5 py-1 rounded border border-violet-400/20 hover:border-violet-400/40 transition-all"
                      >
                        Send All to Sheet
                      </button>
                      <button onClick={() => setScoutAdded([])} className="text-[8px] text-white/25 hover:text-white/50 uppercase tracking-widest transition-colors">Clear</button>
                    </div>
                  </div>
                  <div className="divide-y divide-violet-400/[0.06]">
                    {scoutAdded.map((c, idx) => {
                      const liOk = c.linkedin_status === 'CONFIRMED';
                      const emailOk = c.email_status === 'valid' || c.email_status === 'catch-all';
                      return (
                        <div key={idx} className="px-4 py-3 flex items-start gap-3">
                          <div className="w-7 h-7 rounded-lg bg-violet-400/15 flex items-center justify-center text-[9px] font-bold text-violet-400/70 flex-shrink-0 mt-0.5">
                            {c.full_name.split(' ').map((n: string) => n[0]).join('').slice(0, 2)}
                          </div>
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-1.5 flex-wrap">
                              <span className="text-[11px] font-semibold text-white/85 truncate">{c.full_name}</span>
                              {c.buying_role && (
                                <span className={`text-[7px] font-bold px-1 py-0.5 rounded uppercase tracking-widest flex-shrink-0 ${c.buying_role === 'Decision Maker' ? 'bg-violet-400/20 text-violet-400' : 'bg-white/[0.06] text-white/30'}`}>
                                  {c.buying_role === 'Decision Maker' ? 'DM' : 'Inf'}
                                </span>
                              )}
                            </div>
                            <div className="text-[9px] text-white/40 uppercase tracking-wide truncate">{c.role_title}</div>
                            <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                              {c.linkedin_url && (
                                <a href={c.linkedin_url} target="_blank" rel="noopener noreferrer"
                                  className={`text-[8px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded border transition-colors ${liOk ? 'border-emerald-400/30 text-emerald-400' : 'border-white/10 text-white/25 hover:text-teal-400'}`}>
                                  {liOk ? '✓ LI' : 'LI'}
                                </a>
                              )}
                              {c.email && (
                                <span className="flex items-center gap-1">
                                  <div className={`w-1 h-1 rounded-full ${emailOk ? 'bg-emerald-400' : 'bg-amber-400'}`} />
                                  <span className="text-[8px] font-mono text-white/35 truncate max-w-[120px]">{c.email}</span>
                                </span>
                              )}
                            </div>
                          </div>
                          <div className="flex-shrink-0 mt-0.5">
                            <button
                              onClick={() => handleScoutSendToSheet(idx)}
                              disabled={c.sendStatus === 'sending' || c.sendStatus === 'sent' || c.sendStatus === 'duplicate'}
                              className={`text-[8px] font-bold uppercase tracking-widest px-3 py-1.5 rounded-lg transition-all ${
                                c.sendStatus === 'sent' ? 'bg-emerald-400/10 text-emerald-400 border border-emerald-400/20 cursor-default'
                                : c.sendStatus === 'duplicate' ? 'bg-amber-400/10 text-amber-400 border border-amber-400/20 cursor-default'
                                : c.sendStatus === 'error' ? 'bg-red-400/10 text-red-400 border border-red-400/20'
                                : c.sendStatus === 'sending' ? 'bg-white/5 text-white/30 cursor-not-allowed'
                                : 'bg-violet-400/15 text-violet-400 border border-violet-400/20 hover:bg-violet-400/25'
                              }`}
                            >
                              {c.sendStatus === 'sent' ? '✓ Sent'
                                : c.sendStatus === 'duplicate' ? '⚠ Exists'
                                : c.sendStatus === 'sending' ? '…'
                                : c.sendStatus === 'error' ? 'Retry'
                                : '↑ Sheet'}
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Empty state */}
              {!scanDone && companyList.length === 0 && scoutAdded.length === 0 && (
                <div className="flex flex-col items-center justify-center h-full min-h-[300px] gap-3">
                  <div className="w-10 h-10 rounded-full border border-white/[0.06] flex items-center justify-center">
                    <div className="w-1.5 h-1.5 rounded-full bg-white/10 animate-pulse" />
                  </div>
                  <p className="text-[11px] text-white/55 uppercase tracking-[0.25em]">Awaiting Input</p>
                  <p className="text-[10px] text-white/30 max-w-xs text-center leading-relaxed">
                    Enter target accounts and persona stack to begin discovery, or use AI Scout to find contacts.
                  </p>
                </div>
              )}

              {/* Progress tracker — shown while running */}
              {!scanDone && companyList.length > 0 && (
                <>
                  {running && (
                    <div className="px-6 pt-5 pb-3">
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-[9px] font-bold text-white/50 uppercase tracking-[0.3em]">
                          {doneCount} / {companyList.length} complete
                        </span>
                        <span className="text-[9px] font-mono text-white/35 tabular-nums">
                          {Math.floor(elapsedSecs / 60).toString().padStart(2, '0')}:{(elapsedSecs % 60).toString().padStart(2, '0')}
                        </span>
                      </div>
                      <div className="h-[2px] bg-white/[0.06] rounded-full overflow-hidden">
                        <div
                          className="h-full bg-teal-400/60 rounded-full transition-all duration-700"
                          style={{ width: companyList.length > 0 ? `${(doneCount / companyList.length) * 100}%` : '0%' }}
                        />
                      </div>
                    </div>
                  )}

                  {/* Company status rows */}
                  <div className="divide-y divide-white/[0.04]">
                    {companyList.map((name, i) => {
                      const rawStatus = scanProgress[name] || 'queued';
                      const status = running && rawStatus === 'queued' ? 'processing' : rawStatus;
                      return (
                        <div key={i} className="px-6 py-3.5 flex items-center justify-between">
                          <div className="flex items-center gap-3 min-w-0">
                            <span className="text-[10px] font-bold text-white/35 w-5 text-right flex-shrink-0">{i + 1}</span>
                            <div className="flex-shrink-0 w-4 h-4 flex items-center justify-center">
                              {status === 'queued' && <div className="w-1.5 h-1.5 rounded-full bg-white/20" />}
                              {(status === 'processing' || status === 'validating') && (
                                <div className="relative flex w-2 h-2">
                                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-teal-400/60 opacity-75" style={{ animationDelay: `${i * 0.12}s` }} />
                                  <span className="relative inline-flex rounded-full w-2 h-2 bg-teal-400/80" />
                                </div>
                              )}
                              {status === 'done' && <span className="text-teal-400/70 text-[11px] leading-none">✓</span>}
                              {status === 'error' && <span className="text-red-400/70 text-[11px] leading-none">✕</span>}
                            </div>
                            <span className={`text-sm font-medium truncate transition-colors duration-300 ${status === 'done' ? 'text-white/75' :
                              status === 'processing' || status === 'validating' ? 'text-white' :
                                status === 'error' ? 'text-red-400/70' :
                                  'text-white/45'
                              }`}>{name}</span>
                          </div>
                          <span className={`text-[9px] font-bold uppercase tracking-widest flex-shrink-0 transition-colors duration-300 ${status === 'done' ? 'text-teal-400/60' :
                            status === 'processing' || status === 'validating' ? 'text-teal-400/80' :
                              status === 'error' ? 'text-red-400/60' :
                                'text-white/20'
                            }`}>
                            {status === 'queued' && 'Queued'}
                            {status === 'processing' && 'Scanning…'}
                            {status === 'validating' && 'Validating…'}
                            {status === 'done' && 'Done'}
                            {status === 'error' && 'Error'}
                          </span>
                        </div>
                      );
                    })}
                  </div>

                  {/* ── Role Bucket Selection Panel (Step 1) ── */}
                  {roleEvent && !selectionEvent && (
                    <div className="border-t border-white/[0.08] bg-slate-900/60 animate-[fadeIn_0.35s_ease]">

                      {/* Header */}
                      <div className="px-6 pt-4 pb-3 border-b border-white/[0.05]">
                        <div className="flex items-center gap-2 mb-1">
                          <div className="w-1.5 h-1.5 rounded-full bg-violet-400 animate-pulse" />
                          <span className="text-[9px] font-bold text-violet-300/80 uppercase tracking-[0.3em]">Step 1 of 2 — Role Selection · {roleEvent.company}</span>
                        </div>
                        <p className="text-[10px] text-white/45 mt-1">
                          Found <span className="text-violet-400 font-bold">{roleEvent.total_found}</span> people across {roleEvent.buckets.length} departments.
                          {' '}Pick which functions matter — we'll show the people next.
                        </p>
                      </div>

                      {/* Bucket grid */}
                      <div className="px-4 py-3 grid grid-cols-2 gap-2">
                        {roleEvent.buckets.map(b => {
                          const selected = selectedBuckets.has(b.id);
                          const rank = b.priority_rank ?? 99;
                          const isTop3 = rank <= 3;
                          return (
                            <div
                              key={b.id}
                              onClick={() => toggleBucket(b.id)}
                              className={`relative cursor-pointer rounded-xl border px-3 py-2.5 transition-all duration-150 ${selected
                                ? 'border-violet-400/50 bg-violet-400/10'
                                : 'border-white/[0.06] bg-white/[0.02] hover:border-white/15'
                                }`}
                            >
                              {/* Priority rank badge (top-left) */}
                              <div className={`absolute top-2 left-2.5 text-[8px] font-black tabular-nums leading-none ${isTop3 ? 'text-amber-400/90' : 'text-white/20'
                                }`}>
                                #{rank}
                              </div>
                              {/* Checkmark */}
                              <div className={`absolute top-2 right-2 w-3.5 h-3.5 rounded border flex items-center justify-center transition-colors ${selected ? 'border-violet-400 bg-violet-400' : 'border-white/20'
                                }`}>
                                {selected && (
                                  <svg className="w-2 h-2 text-white" fill="none" viewBox="0 0 10 10">
                                    <path d="M1.5 5l2.5 2.5 4.5-4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                )}
                              </div>
                              <div className={`text-[10px] font-bold mb-0.5 pl-5 pr-4 ${selected ? 'text-violet-300' : 'text-white/70'}`}>{b.label}</div>
                              <div className={`text-[11px] font-bold tabular-nums pl-5 ${selected ? 'text-violet-400' : 'text-white/40'}`}>{b.count} people</div>
                              {b.priority_reason && (
                                <div className={`mt-1 text-[8px] truncate pl-5 ${isTop3 ? 'text-amber-400/50' : 'text-white/20'}`}>{b.priority_reason}</div>
                              )}
                              {!b.priority_reason && b.sample_roles.length > 0 && (
                                <div className="mt-1 text-[8px] text-white/25 truncate pl-5">{b.sample_roles.slice(0, 2).join(' · ')}</div>
                              )}
                            </div>
                          );
                        })}
                      </div>

                      {/* Footer */}
                      <div className="px-6 py-3 border-t border-white/[0.05] flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => setSelectedBuckets(new Set(roleEvent.buckets.map(b => b.id)))}
                            className="text-[9px] font-bold text-white/35 hover:text-white/60 uppercase tracking-widest px-2 py-1 rounded border border-white/10 hover:border-white/20 transition-colors"
                          >All</button>
                          <button
                            onClick={() => setSelectedBuckets(new Set())}
                            className="text-[9px] font-bold text-white/35 hover:text-white/60 uppercase tracking-widest px-2 py-1 rounded border border-white/10 hover:border-white/20 transition-colors"
                          >None</button>
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-[9px] text-white/30">{selectedBuckets.size} of {roleEvent.buckets.length} selected</span>
                          <button
                            onClick={handleRoleSelectionSubmit}
                            disabled={roleSubmitting || selectedBuckets.size === 0}
                            className="text-[9px] font-bold text-black uppercase tracking-widest px-5 py-2 rounded-lg bg-violet-400 hover:bg-violet-300 disabled:bg-white/10 disabled:text-white/20 transition-all"
                          >
                            {roleSubmitting ? 'Filtering…' : `Show ${roleEvent.buckets.filter(b => selectedBuckets.has(b.id)).reduce((a, b) => a + b.count, 0)} People →`}
                          </button>
                        </div>
                      </div>
                    </div>
                  )}


                  {/* Live activity feed — real-time log messages from the backend */}
                  {(running || liveContacts.length > 0) && recentActivity.length > 0 && (
                    <div className="border-t border-white/[0.05]">
                      <div className="px-6 py-2.5 flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {running && (
                            <div className="relative flex h-1.5 w-1.5">
                              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-teal-400/60 opacity-75" />
                              <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-teal-400/80" />
                            </div>
                          )}
                          <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.3em]">Live Activity</span>
                        </div>
                      </div>
                      <div className="px-4 pb-3 flex flex-col gap-1">
                        {recentActivity.map((msg, i) => {
                          const isSuccess = msg.includes('✓') || msg.includes('written to sheet');
                          const isWarning = msg.includes('no ') || msg.includes('not found');
                          return (
                            <div
                              key={i}
                              className={`px-3 py-1.5 rounded-lg text-[9px] font-mono leading-relaxed transition-opacity ${i === 0 ? 'opacity-100' : i === 1 ? 'opacity-70' : i === 2 ? 'opacity-50' : 'opacity-30'
                                } ${isSuccess ? 'text-teal-400 bg-teal-400/5' :
                                  isWarning ? 'text-amber-400/70 bg-amber-400/5' :
                                    'text-white/50 bg-white/[0.02]'
                                }`}
                            >
                              {msg}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}

                  {/* Live contact feed — appears as each contact is written to sheet */}
                  {liveContacts.length > 0 && (
                    <div className="border-t border-white/[0.05]">
                      <div className="px-6 py-2.5 flex items-center justify-between">
                        <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.3em]">Written to Sheet</span>
                        <span className="text-[9px] font-bold text-teal-400/60 uppercase tracking-widest">{liveContacts.length} contacts</span>
                      </div>
                      <div className="divide-y divide-white/[0.03]">
                        {liveContacts.map((c, i) => (
                          <div key={i} className="px-6 py-2.5 flex items-center gap-3 animate-[fadeIn_0.3s_ease]">
                            <div className="w-6 h-6 rounded-md bg-teal-400/10 flex items-center justify-center text-[9px] font-bold text-teal-400/70 flex-shrink-0">
                              {c.full_name?.split(' ').map((n: string) => n[0]).join('').slice(0, 2)}
                            </div>
                            <div className="min-w-0 flex-1">
                              <div className="text-xs font-medium text-white/80 truncate">{c.full_name}</div>
                              <div className="text-[9px] text-white/35 truncate uppercase tracking-wide">{c.role_title}</div>
                            </div>
                            <div className="flex items-center gap-2 flex-shrink-0">
                              <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded uppercase tracking-tight ${c.role_bucket === 'DM' ? 'bg-teal-400/15 text-teal-400' : 'bg-white/[0.06] text-white/35'
                                }`}>{c.role_bucket}</span>
                              <span className="text-[8px] font-bold text-teal-400/50 uppercase tracking-widest">→ sheet</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* After scan: contacts list */}
              {scanDone && contacts.length > 0 && (
                <div className="divide-y divide-white/[0.04]">
                  {contacts.map((c, i) => (
                    <div key={i} className="px-6 py-3.5 flex items-center justify-between">
                      <div className="flex items-center gap-3 min-w-0">
                        <span className="text-[10px] font-bold text-white/35 w-5 text-right flex-shrink-0">{i + 1}</span>
                        <div className="w-7 h-7 rounded-lg bg-white/[0.06] flex items-center justify-center text-[10px] font-bold text-white/40 flex-shrink-0">
                          {c.full_name?.split(' ').map((n: string) => n[0]).join('').slice(0, 2)}
                        </div>
                        <div className="min-w-0">
                          <div className="text-sm font-medium text-white/80 truncate">{c.full_name}</div>
                          <div className="text-[10px] text-white/40 truncate uppercase tracking-wide">{c.role_title}</div>
                        </div>
                      </div>
                      <div className="flex items-center gap-2 flex-shrink-0">
                        <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded uppercase tracking-tight ${c.role_bucket === 'DM' ? 'bg-teal-400/15 text-teal-400' : 'bg-white/[0.06] text-white/35'
                          }`}>{c.role_bucket}</span>
                        {c.linkedin_verified && <span className="text-teal-400/60 text-[10px]">✓</span>}
                        {c.email && <span className="text-[9px] font-mono text-white/30 truncate max-w-[120px]">{c.email}</span>}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* After scan: no contacts */}
              {scanDone && contacts.length === 0 && (
                <div className="flex flex-col items-center justify-center h-full min-h-[200px] gap-3">
                  <div className="text-[10px] font-bold text-white/30 uppercase tracking-widest">No contacts discovered</div>
                  <div className="text-[11px] text-white/20 max-w-xs text-center leading-relaxed">
                    All roles may already be covered, or no verified contacts were found.
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

    </div>
  );
}
