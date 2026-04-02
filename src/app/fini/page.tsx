'use client';

import React, { useState, useCallback, useRef, useEffect } from 'react';
import { useFiniStore } from '@/lib/stores';
import { apiUrl } from '@/lib/api';
import LogStream from '@/components/LogStream';

const REGIONS = [
  { label: 'Africa', value: 'Africa' },
  { label: 'Asia', value: 'Asia' },
  { label: 'Europe', value: 'Europe' },
  { label: 'North America', value: 'North America' },
  { label: 'South America', value: 'South America' },
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
];

interface RegionSelectProps {
  value: string;
  onChange: (val: string) => void;
  disabled?: boolean;
}

function RegionSelect({ value, onChange, disabled }: RegionSelectProps) {
  const [search, setSearch] = useState('');
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const filtered = REGIONS.filter(r =>
    r.label.toLowerCase().includes(search.toLowerCase())
  );
  const isKnown = REGIONS.some(r => r.value.toLowerCase() === value.toLowerCase());
  const showWarning = value && !isKnown;

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const select = (val: string) => { onChange(val); setSearch(''); setOpen(false); };
  const clear = () => { onChange(''); setSearch(''); setOpen(false); };

  return (
    <div ref={ref} className="relative font-sans">
      <div
        className={`input-field flex items-center justify-between cursor-pointer ${disabled ? 'opacity-50 pointer-events-none' : ''}`}
        onClick={() => !disabled && setOpen(v => !v)}
      >
        <span className={value ? 'text-slate-100' : 'text-slate-500'}>
          {value || 'Select region…'}
        </span>
        <div className="flex items-center gap-1">
          {value && (
            <button
              onClick={e => { e.stopPropagation(); clear(); }}
              className="text-slate-500 hover:text-slate-300 text-xs px-1"
            >✕</button>
          )}
          <span className="text-slate-500 text-xs translate-y-[1px]">▾</span>
        </div>
      </div>

      {showWarning && (
        <div className="mt-2 text-[10px] font-bold text-amber-500/80 uppercase tracking-widest px-1">
          ⚠ Custom Value Active
        </div>
      )}

      {open && (
        <div className="absolute z-50 top-full mt-2 w-full bg-slate-900 border border-white/10 rounded-xl shadow-2xl overflow-hidden backdrop-blur-xl bg-opacity-95">
          <div className="p-3 border-b border-white/10">
            <input
              autoFocus
              className="w-full bg-slate-950/50 border border-white/5 rounded-lg px-3 py-2 text-sm text-slate-100 placeholder-slate-600 outline-none"
              placeholder="Filter regions..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              onClick={e => e.stopPropagation()}
            />
          </div>
          <div className="max-h-60 overflow-y-auto no-scrollbar">
            {filtered.length === 0 ? (
              <div className="px-4 py-4 text-xs text-slate-500 font-medium whitespace-normal">
                No matching regions.
                <button
                  className="block mt-2 text-amber-500 hover:text-amber-400 font-bold uppercase tracking-widest"
                  onClick={() => select(search)}
                >&quot;{search}&quot; — use anyway</button>
              </div>
            ) : (
              filtered.map(r => (
                <div
                  key={r.value}
                  className={`px-4 py-3 text-sm cursor-pointer transition-colors ${value === r.value ? 'bg-blue-600/20 text-blue-400 border-l-2 border-blue-500' : 'text-slate-300 hover:bg-white/5'}`}
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
  );
}

// ---------------------------------------------------------------------------
// CompanyData returned from the backend
// ---------------------------------------------------------------------------
interface CompanyData {
  raw_name: string;
  company_name: string;
  sales_nav_url: string;
  domain: string;
  sdr_assigned: string;
  email_format: string;
  account_type: string;
  account_size: string;
  linkedin_org_id: string;
  // Option 2: confidence scores
  linkedin_confidence?: 'high' | 'medium' | 'low';
  domain_confidence?: 'high' | 'medium' | 'low';
  email_confidence?: 'high' | 'medium' | 'low';
  size_confidence?: 'high' | 'medium' | 'low';
  // Option 3: agent notes
  agent_notes?: string;
  // Alternative LinkedIn candidates — each independently enriched
  linkedin_candidates?: {
    org_id: string; name: string; slug: string; how: string;
    domain?: string; email_format?: string; account_size?: string; account_type?: string;
    sales_nav_url?: string; domain_confidence?: string; email_confidence?: string; size_confidence?: string;
  }[];
  // Auto-mode fields
  auto_committed?: boolean;
  selection_reasoning?: string;
  // n8n status (updated via WebSocket)
  n8n_status?: 'submitting' | 'submitted' | 'failed';
}

// ---------------------------------------------------------------------------
// Status per card
// ---------------------------------------------------------------------------
type CardStatus = 'pending' | 'sending' | 'sent' | 'skipped' | 'error';

interface ReviewCard {
  data: CompanyData;
  status: CardStatus;
  errorMsg?: string;
  // editable fields
  editCompanyName: string;
  editRawName: string;
  editSalesNavUrl: string;
  editDomain: string;
  editSdrAssigned: string;
  editEmailFormat: string;
  editAccountType: string;
  editAccountSize: string;
  // multi-match grouping
  groupId?: string;       // shared across all candidate cards for the same raw company
  isCandidate?: boolean;  // true = awaiting SDR pick; false = promoted to normal card
}

// ---------------------------------------------------------------------------
// ReviewCardItem
// ---------------------------------------------------------------------------
// Confidence dot component
function ConfDot({ level }: { level?: string }) {
  if (!level) return null;
  const cls = level === 'high'
    ? 'bg-emerald-400'
    : level === 'medium'
    ? 'bg-amber-400'
    : 'bg-red-400';
  const tip = level === 'high' ? 'High confidence' : level === 'medium' ? 'Medium confidence — verify' : 'Low confidence — needs review';
  return (
    <span title={tip} className={`inline-block w-1.5 h-1.5 rounded-full ${cls} opacity-80 flex-shrink-0`} />
  );
}

function ReviewCardItem({
  card,
  index,
  submitN8n,
  onSend,
  onSendN8n,
  onSendBoth,
  onSkip,
  onFieldChange,
  onReenrich,
}: {
  card: ReviewCard;
  index: number;
  submitN8n: boolean;
  onSend: (index: number) => void;
  onSendN8n: (index: number) => void;
  onSendBoth: (index: number) => void;
  onSkip: (index: number) => void;
  onFieldChange: (index: number, field: keyof ReviewCard, value: string) => void;
  onReenrich: (index: number, newName: string) => void;
}) {
  const isSent = card.status === 'sent';
  const isSkipped = card.status === 'skipped';
  const isDone = isSent || isSkipped;
  const isSending = card.status === 'sending';
  const isReenriching = card.status === 'reenriching' as any;

  const hasRealLink = !!card.data.linkedin_org_id;
  const [notesOpen, setNotesOpen] = React.useState(true);
  const [reenrichInput, setReenrichInput] = React.useState('');
  const [reenrichOpen, setReenrichOpen] = React.useState(false);


  const fieldCls = `w-full bg-black/20 border border-white/[0.06] rounded-lg px-3 py-1.5 text-xs text-white/90 outline-none focus:border-white/20 transition-colors placeholder-white/15`;

  return (
    <div className={`relative rounded-2xl border transition-all duration-500 overflow-hidden ${
      isSent ? 'border-white/[0.08] bg-white/[0.015]' : isSkipped ? 'border-white/[0.04] opacity-30' : 'border-white/[0.07] bg-white/[0.02]'
    }`}>
      {/* Confidence bar at top — colour reflects LinkedIn confidence */}
      <div className={`h-[2px] w-full ${
        card.data.linkedin_confidence === 'high' ? 'bg-emerald-500/60' :
        card.data.linkedin_confidence === 'medium' ? 'bg-amber-500/60' :
        card.data.linkedin_confidence === 'low' ? 'bg-red-500/50' :
        hasRealLink ? 'bg-white/30' : 'bg-white/8'
      }`} />

      <div className="p-5 space-y-4">
        {/* Header */}
        <div>
          <div className="flex items-center gap-2 mb-2">
            <ConfDot level={card.data.linkedin_confidence} />
            <span className={`text-[9px] font-bold uppercase tracking-widest ${hasRealLink ? 'text-white/65' : 'text-white/50'}`}>
              {hasRealLink ? 'Matched' : '⚠ Fallback'}
            </span>
            {card.data.auto_committed && <span className="text-[9px] font-bold uppercase tracking-widest text-emerald-400/80">· Auto-committed</span>}
            {isSent && !card.data.auto_committed && <span className="text-[9px] font-bold uppercase tracking-widest text-white/65">· Synced</span>}
            {isSkipped && <span className="text-[9px] uppercase tracking-widest text-white/50">· Skipped</span>}
            {(card.data.agent_notes || card.data.selection_reasoning) && (
              <button
                onClick={() => setNotesOpen(o => !o)}
                className="ml-auto text-[9px] text-white/40 hover:text-white/70 uppercase tracking-widest transition-colors"
              >
                {notesOpen ? 'Hide notes' : 'Show notes'}
              </button>
            )}
          </div>

          {/* Agent notes panel */}
          {notesOpen && (card.data.agent_notes || card.data.selection_reasoning) && (
            <div className="mb-3 px-3 py-2.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-[10px] text-white/60 leading-relaxed space-y-1.5">
              {card.data.agent_notes && <div>{card.data.agent_notes}</div>}
              {card.data.selection_reasoning && (
                <div className="text-blue-300/70 italic">AI reasoning: {card.data.selection_reasoning}</div>
              )}
            </div>
          )}

          <input
            className="w-full bg-transparent text-sm font-bold text-white outline-none border-b border-transparent hover:border-white/10 focus:border-white/20 transition-colors pb-0.5 mb-0.5"
            value={card.editCompanyName}
            onChange={e => onFieldChange(index, 'editCompanyName', e.target.value)}
            disabled={isDone}
            placeholder="Company Name"
          />
          <input
            className="w-full bg-transparent text-[11px] text-white/65 outline-none border-b border-transparent hover:border-white/8 focus:border-white/15 transition-colors pb-0.5"
            value={card.editRawName}
            onChange={e => onFieldChange(index, 'editRawName', e.target.value)}
            disabled={isDone}
            placeholder="Parent / Raw Name"
          />
        </div>

        {/* Sales Nav URL */}
        <div>
          <div className="flex items-center gap-1.5 mb-1.5">
            <ConfDot level={card.data.linkedin_confidence} />
            <span className="text-[9px] text-white/55 uppercase tracking-widest">Sales Navigator</span>
          </div>
          <div className="flex items-center gap-2">
            <input
              className={`flex-1 min-w-0 text-[10px] font-mono bg-black/20 border rounded-lg px-3 py-2 outline-none transition-colors ${card.editSalesNavUrl ? 'border-white/10 text-white/50 focus:border-white/20' : 'border-white/[0.05] text-white/55 focus:border-white/10'}`}
              value={card.editSalesNavUrl}
              onChange={e => onFieldChange(index, 'editSalesNavUrl', e.target.value)}
              disabled={isDone}
              placeholder="https://www.linkedin.com/sales/..."
            />
            {card.editSalesNavUrl && (
              <a
                href={card.editSalesNavUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-shrink-0 px-3 py-2 border border-white/10 rounded-lg text-[10px] font-bold text-white/75 hover:text-white/90 hover:border-white/20 uppercase tracking-wider transition-colors"
              >
                ↗
              </a>
            )}
          </div>

        </div>

        {/* Info grid */}
        <div className="grid grid-cols-2 gap-2.5">
          <div>
            <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1">SDR</div>
            <input className={fieldCls} value={card.editSdrAssigned} onChange={e => onFieldChange(index, 'editSdrAssigned', e.target.value)} disabled={isDone} placeholder="—" />
          </div>
          <div className="col-span-2">
            <div className="flex items-center gap-1.5 mb-1">
              <ConfDot level={card.data.email_confidence} />
              <span className="text-[9px] text-white/55 uppercase tracking-widest">Email Format</span>
            </div>
            <input className={`${fieldCls} font-mono`} value={card.editEmailFormat} onChange={e => onFieldChange(index, 'editEmailFormat', e.target.value)} disabled={isDone} placeholder="—" />
          </div>
          <div>
            <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1">Account Type</div>
            <input className={fieldCls} value={card.editAccountType} onChange={e => onFieldChange(index, 'editAccountType', e.target.value)} disabled={isDone} placeholder="—" />
          </div>
          <div>
            <div className="flex items-center gap-1.5 mb-1">
              <ConfDot level={card.data.size_confidence} />
              <span className="text-[9px] text-white/55 uppercase tracking-widest">Account Size</span>
            </div>
            <select className={`${fieldCls} cursor-pointer`} value={card.editAccountSize} onChange={e => onFieldChange(index, 'editAccountSize', e.target.value)} disabled={isDone}>
              <option value="">—</option>
              <option value="Small">Small</option>
              <option value="Medium">Medium</option>
              <option value="Large">Large</option>
            </select>
          </div>
          <div className="col-span-2">
            <div className="flex items-center gap-1.5 mb-1">
              <ConfDot level={card.data.domain_confidence} />
              <span className="text-[9px] text-white/55 uppercase tracking-widest">Domain</span>
            </div>
            <input className={`${fieldCls} font-mono`} value={card.editDomain} onChange={e => onFieldChange(index, 'editDomain', e.target.value)} disabled={isDone} placeholder="—" />
          </div>
        </div>

        {/* Option 4: Re-enrich with corrected name */}
        {!isDone && (
          <div>
            {!reenrichOpen ? (
              <button
                onClick={() => setReenrichOpen(true)}
                className="text-[9px] text-white/35 hover:text-white/60 uppercase tracking-widest transition-colors"
              >
                ↺ Re-search with different name
              </button>
            ) : (
              <div className="flex gap-2">
                <input
                  autoFocus
                  className="flex-1 bg-black/20 border border-white/10 rounded-lg px-3 py-1.5 text-xs text-white/90 outline-none focus:border-white/25 placeholder-white/20"
                  placeholder="Enter correct company name…"
                  value={reenrichInput}
                  onChange={e => setReenrichInput(e.target.value)}
                  onKeyDown={e => {
                    if (e.key === 'Enter' && reenrichInput.trim()) {
                      onReenrich(index, reenrichInput.trim());
                      setReenrichInput('');
                      setReenrichOpen(false);
                    }
                    if (e.key === 'Escape') setReenrichOpen(false);
                  }}
                />
                <button
                  onClick={() => {
                    if (reenrichInput.trim()) {
                      onReenrich(index, reenrichInput.trim());
                      setReenrichInput('');
                      setReenrichOpen(false);
                    }
                  }}
                  className="px-3 py-1.5 rounded-lg bg-white/10 hover:bg-white/15 text-white/80 text-xs font-bold transition-colors"
                >
                  Go
                </button>
                <button
                  onClick={() => setReenrichOpen(false)}
                  className="px-2 py-1.5 text-white/35 hover:text-white/60 text-xs transition-colors"
                >
                  ✕
                </button>
              </div>
            )}
          </div>
        )}

        {card.status === 'error' && card.errorMsg && (
          <div className="px-3 py-2 border border-red-900/40 bg-red-950/20 rounded-lg text-[11px] text-red-400/80">{card.errorMsg}</div>
        )}

        {/* Actions */}
        {!isDone && (
          <div className="flex flex-col gap-2 pt-1">
            {/* When n8n relay is ON: single "Send to Sheet" button (sends to both) */}
            {submitN8n ? (
              <div className="flex gap-2">
                <button
                  id={`fini-send-${index}`}
                  onClick={() => onSend(index)}
                  disabled={isSending || isReenriching}
                  className="flex-1 py-2.5 rounded-xl bg-white hover:bg-white/90 disabled:bg-white/10 text-black disabled:text-white/55 text-[11px] font-bold uppercase tracking-wider transition-all duration-200 flex items-center justify-center gap-1.5"
                >
                  {isSending ? (
                    <><div className="w-3 h-3 border-2 border-black/20 border-t-black rounded-full animate-spin" /><span>Sending…</span></>
                  ) : isReenriching ? (
                    <><div className="w-3 h-3 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" /><span>Re-searching…</span></>
                  ) : (
                    <><span>↑</span><span>Send to Sheet + n8n</span></>
                  )}
                </button>
                <button
                  id={`fini-skip-${index}`}
                  onClick={() => onSkip(index)}
                  disabled={isSending || isReenriching}
                  className="px-4 py-2.5 rounded-xl border border-white/[0.07] text-white/65 hover:text-white/60 hover:border-white/15 text-[11px] font-bold uppercase tracking-wider transition-all duration-200"
                >
                  Skip
                </button>
              </div>
            ) : (
              /* When n8n relay is OFF: show 3 options */
              <div className="flex gap-2">
                <button
                  id={`fini-send-${index}`}
                  onClick={() => onSend(index)}
                  disabled={isSending || isReenriching}
                  className="flex-1 py-2.5 rounded-xl bg-white hover:bg-white/90 disabled:bg-white/10 text-black disabled:text-white/55 text-[10px] font-bold uppercase tracking-wider transition-all duration-200 flex items-center justify-center gap-1"
                >
                  {isSending ? (
                    <><div className="w-3 h-3 border-2 border-black/20 border-t-black rounded-full animate-spin" /><span>Sending…</span></>
                  ) : isReenriching ? (
                    <><div className="w-3 h-3 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" /><span>Re-searching…</span></>
                  ) : (
                    <><span>↑</span><span>Sheet</span></>
                  )}
                </button>
                <button
                  id={`fini-send-n8n-${index}`}
                  onClick={() => onSendBoth(index)}
                  disabled={isSending || isReenriching}
                  className="flex-1 py-2.5 rounded-xl bg-emerald-600 hover:bg-emerald-500 disabled:bg-white/10 text-white disabled:text-white/55 text-[10px] font-bold uppercase tracking-wider transition-all duration-200 flex items-center justify-center gap-1"
                >
                  <span>↑</span><span>Sheet + n8n</span>
                </button>
                <button
                  id={`fini-skip-${index}`}
                  onClick={() => onSkip(index)}
                  disabled={isSending || isReenriching}
                  className="px-3 py-2.5 rounded-xl border border-white/[0.07] text-white/65 hover:text-white/60 hover:border-white/15 text-[10px] font-bold uppercase tracking-wider transition-all duration-200"
                >
                  Skip
                </button>
              </div>
            )}
          </div>
        )}

        {isSent && (
          <div className="flex items-center gap-2 pt-1 flex-wrap">
            <span className="text-[11px] text-white/65 font-medium">Synchronized to Google Sheets</span>
            {card.data.n8n_status === 'submitted' && (
              <span className="text-[9px] text-emerald-400/70 uppercase tracking-widest font-bold">· n8n sent</span>
            )}
            {card.data.n8n_status === 'submitting' && (
              <span className="text-[9px] text-amber-400/70 uppercase tracking-widest font-bold animate-pulse">· n8n pending...</span>
            )}
            {card.data.n8n_status === 'failed' && (
              <span className="text-[9px] text-red-400/70 uppercase tracking-widest font-bold">· n8n failed</span>
            )}
            {!card.data.n8n_status && submitN8n && (
              <span className="ml-auto text-[9px] text-white/50 uppercase tracking-widest">+ n8n</span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Pipeline Tracker — shows Fini → n8n → Veri → Searcher → Veri chain status
// ---------------------------------------------------------------------------
type AgentRun = {
  agent: string;
  status: string;
  thread_id: string;
  started_at: string;
  auto_triggered_by?: string;
  contacts_appended?: number;
  contacts_discovered?: number;
  missing_roles?: string[];
  error?: string;
  verified?: number;
  review?: number;
  rejected?: number;
};

type PipelineStep = {
  label: string;
  agent: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  detail: string;
  time: string;
};

type PipelineData = {
  running: boolean;
  current_company: string;
  current_step: string;
  companies: string[];
  results: Array<{
    company: string;
    contacts: number;
    status: string;
    steps: Record<string, string>;
  }>;
  buffer: {
    companies: string[];
    total_contacts: number;
    timer_active: boolean;
  };
};

const STEP_LABELS: Record<string, string> = {
  sheet_write: 'Write to Sheet',
  veri_r1: 'Verify (R1)',
  searcher: 'Searcher',
  veri_r2: 'Verify (R2)',
};

const STEP_KEYS = ['sheet_write', 'veri_r1', 'searcher', 'veri_r2'];

function PipelineTracker() {
  const [data, setData] = useState<PipelineData | null>(null);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      while (!cancelled) {
        try {
          const resp = await fetch(apiUrl('/api/n8n/pipeline'));
          const d: PipelineData = await resp.json();
          setData(d);
        } catch { /* ignore */ }
        await new Promise(r => setTimeout(r, 3000));
      }
    };
    poll();
    return () => { cancelled = true; };
  }, []);

  if (!data) return null;

  const hasActivity = data.running || data.results.length > 0 || data.buffer.companies.length > 0;
  if (!hasActivity) return null;

  const stepIcon = (s: string) => {
    if (s === 'running') return '◌';
    if (s?.startsWith('✅') || s === 'done') return '✓';
    if (s?.startsWith('❌') || s?.startsWith('crashed') || s?.startsWith('failed')) return '✕';
    if (s === 'pending') return '○';
    return '○';
  };

  const stepColor = (s: string) => {
    if (s === 'running') return 'text-blue-400 bg-blue-500/10 border-blue-500/30 animate-pulse';
    if (s?.startsWith('✅') || s === 'done') return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30';
    if (s?.startsWith('❌') || s?.startsWith('crashed') || s?.startsWith('failed')) return 'text-red-400 bg-red-500/10 border-red-500/30';
    return 'text-white/20 bg-white/[0.02] border-white/10';
  };

  const companyStatus = (r: PipelineData['results'][0]) => {
    if (r.status === 'done') return 'text-emerald-400';
    if (r.status === 'running') return 'text-blue-400';
    if (r.status.startsWith('crashed') || r.status.startsWith('failed')) return 'text-red-400';
    return 'text-white/30';
  };

  return (
    <div className="border border-white/[0.06] rounded-2xl bg-black/20 p-4 mt-3">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em]">Pipeline Tracker</span>
        {data.running && <span className="text-[8px] text-blue-400 animate-pulse font-mono">RUNNING</span>}
        {!data.running && data.results.length > 0 && <span className="text-[8px] text-emerald-400 font-mono">DONE</span>}
        <div className="flex-1 h-[1px] bg-white/[0.06]" />
        {data.buffer.companies.length > 0 && (
          <span className="text-[8px] text-amber-400 font-mono">
            BUFFER: {data.buffer.total_contacts} contacts, {data.buffer.companies.length} companies
            {data.buffer.timer_active && ' (waiting...)'}
          </span>
        )}
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-[200px_repeat(4,1fr)] gap-1 mb-1">
        <div className="text-[8px] font-bold text-white/30 uppercase tracking-wider pl-2">Company</div>
        {STEP_KEYS.map(k => (
          <div key={k} className="text-[8px] font-bold text-white/30 uppercase tracking-wider text-center">{STEP_LABELS[k]}</div>
        ))}
      </div>

      {/* Per-company rows */}
      {data.results.map((r, i) => (
        <div key={i} className={`grid grid-cols-[200px_repeat(4,1fr)] gap-1 py-1.5 border-t border-white/[0.04] ${r.status === 'running' ? 'bg-blue-500/[0.03]' : ''}`}>
          <div className="flex items-center gap-2 pl-2">
            <span className={`text-[11px] font-bold truncate ${companyStatus(r)}`}>{r.company}</span>
            <span className="text-[8px] text-white/20 font-mono">{r.contacts}</span>
          </div>
          {STEP_KEYS.map(k => {
            const val = r.steps[k] || 'pending';
            return (
              <div key={k} className="flex items-center justify-center">
                <div className={`w-6 h-6 rounded-full border flex items-center justify-center text-[10px] font-bold ${stepColor(val)}`}>
                  {stepIcon(val)}
                </div>
              </div>
            );
          })}
        </div>
      ))}

      {/* Pending companies (in buffer, not yet started) */}
      {data.buffer.companies.filter(c => !data.results.find(r => r.company === c)).map(c => (
        <div key={c} className="grid grid-cols-[200px_repeat(4,1fr)] gap-1 py-1.5 border-t border-white/[0.04] opacity-40">
          <div className="pl-2 text-[11px] text-white/30 truncate">{c}</div>
          {STEP_KEYS.map(k => (
            <div key={k} className="flex items-center justify-center">
              <div className="w-6 h-6 rounded-full border border-white/10 flex items-center justify-center text-[10px] text-white/20">○</div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Fini Page
// ---------------------------------------------------------------------------
export default function FiniPage() {
  const {
    companies, sdr, region, submitN8n, autoMode,
    running, threadId, error,
    reviewCards, enrichmentDone, enrichmentStats,
    enrichProgress, elapsedSecs, isSendingAll,
  } = useFiniStore();
  const setCompanies   = (v: string)  => useFiniStore.setState({ companies: v });
  const setSdr         = (v: string)  => useFiniStore.setState({ sdr: v });
  const setRegion      = (v: string)  => useFiniStore.setState({ region: v });
  const setSubmitN8n   = (fn: boolean | ((v: boolean) => boolean)) =>
    useFiniStore.setState(s => ({ submitN8n: typeof fn === 'function' ? fn(s.submitN8n) : fn }));
  const setAutoMode    = (fn: boolean | ((v: boolean) => boolean)) =>
    useFiniStore.setState(s => ({ autoMode: typeof fn === 'function' ? fn(s.autoMode) : fn }));
  const setRunning     = (v: boolean) => useFiniStore.setState({ running: v });
  const setThreadId    = (v: string | null) => useFiniStore.setState({ threadId: v });
  const setError       = (v: string | null) => useFiniStore.setState({ error: v });
  const setEnrichmentDone  = (v: boolean) => useFiniStore.setState({ enrichmentDone: v });
  const setEnrichmentStats = (v: any)     => useFiniStore.setState({ enrichmentStats: v });
  const setElapsedSecs     = (fn: ((s: number) => number) | number) =>
    useFiniStore.setState(s => ({ elapsedSecs: typeof fn === 'function' ? fn(s.elapsedSecs) : fn }));
  const setIsSendingAll = (v: boolean) => useFiniStore.setState({ isSendingAll: v });
  const setEnrichProgress = (fn: ((p: Record<string, string>) => Record<string, string>) | Record<string, string>) =>
    useFiniStore.setState(s => ({ enrichProgress: typeof fn === 'function' ? fn(s.enrichProgress) : fn }));
  const setReviewCards = (fn: ((p: ReviewCard[]) => ReviewCard[]) | ReviewCard[]) =>
    useFiniStore.setState(s => ({ reviewCards: typeof fn === 'function' ? fn(s.reviewCards) : fn }));
  const elapsedRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const applyCompletedData = useCallback((data: any) => {
    const comps: CompanyData[] = data?.companies || [];
    const stats = {
      processed: data?.companies_processed || comps.length,
      errors: data?.errors || [],
    };
    setEnrichmentStats(stats);
    setEnrichmentDone(true);
    setRunning(false);
    setReviewCards(() => {
      const cards: ReviewCard[] = [];
      comps.forEach((c: CompanyData) => {
        // Auto-committed companies show as already sent
        if (c.auto_committed) {
          cards.push({
            data: c,
            status: 'sent',
            editCompanyName: c.company_name,
            editRawName: c.raw_name,
            editSalesNavUrl: c.sales_nav_url,
            editDomain: c.domain,
            editSdrAssigned: c.sdr_assigned,
            editEmailFormat: c.email_format,
            editAccountType: c.account_type || region || 'Global',
            editAccountSize: c.account_size,
          });
          return;
        }

        const candidates = c.linkedin_candidates || [];
        // If the backend already resolved with HIGH confidence, treat as single match
        const alreadyResolved = c.linkedin_confidence === 'high' && c.selection_reasoning;
        const isMultiMatch = candidates.length > 1 && !alreadyResolved;

        if (isMultiMatch) {
          // Generate one card per candidate — each with independently enriched data
          candidates.forEach(cand => {
            const navUrl = cand.sales_nav_url || `https://www.linkedin.com/sales/search/people?query=(recentSearchParam%3A(doLogHistory%3Atrue)%2Cfilters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A${cand.org_id}%2Ctext%3A${encodeURIComponent(cand.name || cand.slug)}%2CselectionType%3AINCLUDED%2Cparent%3A(id%3A0))))))`;
            cards.push({
              data: { ...c, linkedin_org_id: cand.org_id, linkedin_candidates: candidates },
              status: 'pending',
              groupId: c.raw_name,
              isCandidate: true,
              editCompanyName: cand.name || c.company_name,
              editRawName: c.raw_name,
              editSalesNavUrl: navUrl,
              // Use per-candidate enriched data instead of cloning parent
              editDomain: cand.domain || c.domain,
              editSdrAssigned: c.sdr_assigned,
              editEmailFormat: cand.email_format || c.email_format,
              editAccountType: cand.account_type || c.account_type || region || 'Global',
              editAccountSize: cand.account_size || c.account_size,
            });
          });
        } else {
          // Normal single-match card
          cards.push({
            data: c,
            status: 'pending',
            editCompanyName: c.company_name,
            editRawName: c.raw_name,
            editSalesNavUrl: c.sales_nav_url,
            editDomain: c.domain,
            editSdrAssigned: c.sdr_assigned,
            editEmailFormat: c.email_format,
            editAccountType: c.account_type || region || 'Global',
            editAccountSize: c.account_size,
          });
        }
      });
      return cards;
    });
  }, [region]);

  const handleEvent = useCallback((msg: any) => {
    if (msg.type === 'completed') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setEnrichProgress(prev => {
        const next = { ...prev };
        Object.keys(next).forEach(k => { if (next[k] !== 'error') next[k] = 'done'; });
        return next;
      });
      // If cards already exist (WS reconnect after navigation), preserve their statuses — don't rebuild
      const existingCards = useFiniStore.getState().reviewCards;
      if (existingCards.length === 0) {
        applyCompletedData(msg.data);
      } else {
        setEnrichmentDone(true);
        setRunning(false);
      }
    } else if (msg.type === 'paused') {
      useFiniStore.setState({ paused: true });
    } else if (msg.type === 'resumed') {
      useFiniStore.setState({ paused: false });
    } else if (msg.type === 'cancelled') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setRunning(false);
      useFiniStore.setState({ cancelled: true, paused: false });
    } else if (msg.type === 'error') {
      if (elapsedRef.current) { clearInterval(elapsedRef.current); elapsedRef.current = null; }
      setRunning(false);
      setError(msg.data?.error || 'Pipeline error');
    } else if (msg.type === 'company_progress') {
      const { company, status } = msg.data;
      setEnrichProgress(prev => ({ ...prev, [company]: status }));
    } else if (msg.type === 'company_enriched') {
      // Stream card as soon as enrichment completes — don't wait for all
      const c = msg.data as CompanyData & { card_status?: string };
      const cardStatus = (c.card_status === 'sent' ? 'sent' : 'pending') as CardStatus;
      const candidates = c.linkedin_candidates || [];
      const alreadyResolved = c.linkedin_confidence === 'high' && c.selection_reasoning;
      const isMultiMatch = candidates.length > 1 && !alreadyResolved;

      setReviewCards(prev => {
        // Don't add duplicate
        if (prev.some(card => card.editRawName === c.raw_name)) return prev;

        if (isMultiMatch) {
          const newCards: ReviewCard[] = candidates.map(cand => ({
            data: { ...c, linkedin_org_id: cand.org_id, linkedin_candidates: candidates },
            status: 'pending' as CardStatus,
            groupId: c.raw_name,
            isCandidate: true,
            editCompanyName: cand.name || c.company_name,
            editRawName: c.raw_name,
            editSalesNavUrl: cand.sales_nav_url || c.sales_nav_url,
            editDomain: cand.domain || c.domain,
            editSdrAssigned: c.sdr_assigned,
            editEmailFormat: cand.email_format || c.email_format,
            editAccountType: cand.account_type || c.account_type || region || 'Global',
            editAccountSize: cand.account_size || c.account_size,
          }));
          return [...prev, ...newCards];
        }

        return [...prev, {
          data: c,
          status: cardStatus,
          editCompanyName: c.company_name,
          editRawName: c.raw_name,
          editSalesNavUrl: c.sales_nav_url,
          editDomain: c.domain,
          editSdrAssigned: c.sdr_assigned,
          editEmailFormat: c.email_format,
          editAccountType: c.account_type || region || 'Global',
          editAccountSize: c.account_size,
        }];
      });
    } else if (msg.type === 'n8n_status') {
      // Update n8n badge on the card
      const { raw_name, status: n8nStatus } = msg.data;
      setReviewCards(prev => prev.map(card =>
        card.editRawName === raw_name
          ? { ...card, data: { ...card.data, n8n_status: n8nStatus } }
          : card
      ));
    }
  }, [applyCompletedData, region]);

  // Fallback: if WS closes without delivering the completed event, poll REST
  useEffect(() => {
    if (!threadId || !running) return;
    let cancelled = false;
    const poll = async () => {
      // wait 3s then start polling every 4s for up to 10 minutes
      await new Promise(r => setTimeout(r, 3000));
      while (!cancelled) {
        try {
          const resp = await fetch(apiUrl(`/api/fini/results/${threadId}`));
          if (resp.ok) {
            const data = await resp.json();
            if (data.status === 'completed' && data.companies?.length > 0) {
              if (!cancelled) {
                const existingCards = useFiniStore.getState().reviewCards;
                if (existingCards.length === 0) applyCompletedData(data);
                else { setEnrichmentDone(true); setRunning(false); }
              }
              break;
            }
          }
        } catch { /* ignore */ }
        await new Promise(r => setTimeout(r, 4000));
      }
    };
    poll();
    return () => { cancelled = true; };
  }, [threadId, running, applyCompletedData]);

  const handleStop = async () => {
    if (!threadId) return;
    try { await fetch(apiUrl(`/api/runs/${threadId}/cancel`), { method: 'POST' }); } catch { /* ignore */ }
  };

  const handlePauseResume = async () => {
    if (!threadId) return;
    const paused = useFiniStore.getState().paused;
    try {
      await fetch(apiUrl(`/api/runs/${threadId}/${paused ? 'resume' : 'pause'}`), { method: 'POST' });
    } catch { /* ignore */ }
  };

  const handleRun = async () => {
    if (!companies.trim()) return;
    setRunning(true);
    setError(null);
    setReviewCards([]);
    setEnrichmentDone(false);
    setEnrichmentStats(null);

    // Initialise per-company progress
    const list = companies.split(',').map(c => c.trim()).filter(Boolean);
    const initial: Record<string, string> = {};
    list.forEach(c => { initial[c] = 'queued'; });
    setEnrichProgress(initial);
    setElapsedSecs(0);
    if (elapsedRef.current) clearInterval(elapsedRef.current);
    elapsedRef.current = setInterval(() => setElapsedSecs(s => s + 1), 1000);

    try {
      // Always use /api/fini/run — Fini shows review cards, submits to n8n.
      // Veri + Searcher chain is triggered automatically when n8n calls
      // POST /api/n8n/contacts with the enriched contacts JSON.
      const resp = await fetch(apiUrl('/api/fini/run'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companies: companies.trim(),
          sdr: sdr.trim(),
          region: region.trim(),
          submit_n8n: submitN8n,
          auto_mode: autoMode,
        }),
      });
      const data = await resp.json();
      setThreadId(data.thread_id);
    } catch (e: any) {
      setError(e.message);
      setRunning(false);
    }
  };

  // Send to sheet only (no n8n)
  const handleSend = async (index: number, forceN8n?: boolean) => {
    const card = reviewCards[index];
    setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'sending' } : c));

    try {
      const resp = await fetch(apiUrl('/api/fini/commit'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          company_name: card.editCompanyName,
          raw_name: card.editRawName,
          sales_nav_url: card.editSalesNavUrl,
          domain: card.editDomain,
          sdr_assigned: card.editSdrAssigned,
          email_format: card.editEmailFormat,
          account_type: card.editAccountType,
          account_size: card.editAccountSize,
          submit_n8n: forceN8n ?? submitN8n,
        }),
      });
      if (!resp.ok) {
        const err = await resp.json();
        throw new Error(err.detail || 'Commit failed');
      }
      setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'sent' } : c));
    } catch (e: any) {
      setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'error', errorMsg: e.message } : c));
    }
  };

  // Send to n8n only (also writes to sheet — sheet is always compulsory)
  const handleSendN8n = async (index: number) => handleSend(index, true);

  // Send to both sheet + n8n
  const handleSendBoth = async (index: number) => handleSend(index, true);

  const handleSkip = (index: number) => {
    setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'skipped' } : c));
  };

  // Keep a candidate — promote it to a normal review card, leave others in the group
  const handleKeepCandidate = (index: number) => {
    setReviewCards(prev => prev.map((c, i) =>
      i === index ? { ...c, isCandidate: false } : c
    ));
  };

  // Skip one candidate — just removes it from the group, others stay for SDR to decide
  const handleSkipCandidate = (index: number) => {
    setReviewCards(prev => prev.map((c, i) =>
      i === index ? { ...c, status: 'skipped' as CardStatus } : c
    ));
  };

  const handleSendAll = async () => {
    if (isSendingAll) return;
    setIsSendingAll(true);
    try {
      const pending = reviewCards
        .map((c, i) => ({ card: c, index: i }))
        .filter(({ card }) => card.status === 'pending' || card.status === 'error');
      for (const { index } of pending) {
        await handleSend(index);
        // Small delay between commits to avoid Google Sheets 429 rate limit
        await new Promise(res => setTimeout(res, 500));
      }
    } finally {
      setIsSendingAll(false);
    }
  };

  const handleFieldChange = (index: number, field: keyof ReviewCard, value: string) => {
    setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, [field]: value } : c));
  };

  const handleReenrich = async (index: number, newName: string) => {
    setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'reenriching' as any } : c));
    try {
      const resp = await fetch(apiUrl('/api/fini/reenrich'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_name: newName, region }),
      });
      if (!resp.ok) throw new Error('Re-enrich failed');
      const data: CompanyData = await resp.json();
      setReviewCards(prev => prev.map((c, i) => i === index ? {
        ...c,
        status: 'pending',
        data,
        editCompanyName: data.company_name || newName,
        editRawName: newName,
        editSalesNavUrl: data.sales_nav_url || '',
        editDomain: data.domain || '',
        editSdrAssigned: data.sdr_assigned || c.editSdrAssigned,
        editEmailFormat: data.email_format || '',
        editAccountType: data.account_type || c.editAccountType,
        editAccountSize: data.account_size || c.editAccountSize,
      } : c));
    } catch {
      setReviewCards(prev => prev.map((c, i) => i === index ? { ...c, status: 'error', errorMsg: 'Re-search failed' } : c));
    }
  };

  const sentCount = reviewCards.filter(c => c.status === 'sent').length;
  const skippedCount = reviewCards.filter(c => c.status === 'skipped').length;
  const pendingCount = reviewCards.filter(c => c.status === 'pending' || c.status === 'error').length;

  const companyList = companies.split(',').map(c => c.trim()).filter(Boolean);

  const matchedCount = reviewCards.filter(c => !!c.data.linkedin_org_id).length;
  const fallbackCount = reviewCards.filter(c => !c.data.linkedin_org_id).length;

  return (
    <div className="flex flex-col px-6 pt-4 pb-10 max-w-[1600px] mx-auto font-sans">

      {/* ── Header — compact single row ── */}
      <div className="flex items-center justify-between mb-3 pb-3 border-b border-white/[0.06] flex-shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.4em]">Module 01 · Target Enrichment</span>
          <span className="h-3 w-[1px] bg-white/10" />
          <h1 className="text-base font-bold text-white tracking-tight">
            Pipeline<span className="text-white/50 font-light"> / </span>Fini
          </h1>
        </div>
        <div className="flex items-center gap-4">
          {running && (
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <div className="relative flex h-1.5 w-1.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-blue-500" />
                </div>
                <span className="text-[9px] font-bold text-white/75 uppercase tracking-[0.25em]">Enriching</span>
              </div>
              <button
                onClick={handlePauseResume}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-lg border text-[9px] font-bold uppercase tracking-widest transition-colors ${
                  useFiniStore.getState().paused
                    ? 'border-teal-500/30 bg-teal-500/10 text-teal-400 hover:bg-teal-500/20'
                    : 'border-amber-500/30 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20'
                }`}
              >
                <span>{useFiniStore.getState().paused ? '▶' : '⏸'}</span>
                <span>{useFiniStore.getState().paused ? 'Resume' : 'Pause'}</span>
              </button>
              <button
                onClick={handleStop}
                className="flex items-center gap-1.5 px-3 py-1 rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 text-[9px] font-bold uppercase tracking-widest hover:bg-red-500/20 transition-colors"
              >
                <span>■</span><span>Stop</span>
              </button>
            </div>
          )}
          {useFiniStore.getState().cancelled && !running && (
            <span className="text-[9px] font-bold text-red-400/70 uppercase tracking-widest">Stopped</span>
          )}
          {reviewCards.length > 0 && (
            <div className="flex items-center gap-3">
              <div className="text-right">
                <div className="text-xs font-bold text-white">{reviewCards.length}</div>
                <div className="text-[9px] text-white/50 uppercase tracking-widest">Processed</div>
              </div>
              <div className="h-4 w-[1px] bg-white/10" />
              <div className="text-right">
                <div className="text-xs font-bold text-white">{matchedCount}</div>
                <div className="text-[9px] text-white/50 uppercase tracking-widest">Matched</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── Main 2-col grid ── */}
      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">

        {/* Left: Configuration + Log */}
        <div className="xl:col-span-2 flex flex-col gap-3">

          {/* Config panel — compact, fixed height */}
          <div className="border border-white/[0.07] rounded-2xl bg-white/[0.02] p-4 flex flex-col gap-3 flex-shrink-0">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em]">Configuration</div>

            <div>
              <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1.5">Input Entities</div>
              <textarea
                className="w-full bg-black/30 border border-white/[0.06] rounded-xl px-3 py-2 text-xs text-white placeholder-white/15 outline-none focus:border-white/20 transition-colors resize-none leading-relaxed"
                rows={2}
                placeholder="Paste company names separated by commas..."
                value={companies}
                onChange={e => setCompanies(e.target.value)}
                disabled={running}
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1.5">SDR Assignment</div>
                <input
                  className="w-full bg-black/30 border border-white/[0.06] rounded-xl px-3 py-2 text-xs text-white placeholder-white/15 outline-none focus:border-white/20 transition-colors"
                  placeholder="Name"
                  value={sdr}
                  onChange={e => setSdr(e.target.value)}
                  disabled={running}
                />
              </div>
              <div>
                <div className="text-[9px] text-white/55 uppercase tracking-widest mb-1.5">Priority Region</div>
                <RegionSelect value={region} onChange={setRegion} disabled={running} />
              </div>
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2.5">
                <button
                  onClick={() => setSubmitN8n(v => !v)}
                  disabled={running}
                  className={`relative w-8 h-4 rounded-full transition-all duration-300 ${submitN8n ? 'bg-white/80' : 'bg-white/10'}`}
                >
                  <div className={`absolute top-[2px] w-3 h-3 rounded-full shadow-sm transition-all duration-300 ${submitN8n ? 'left-[18px] bg-black' : 'left-[2px] bg-white/40'}`} />
                </button>
                <span className="text-[10px] text-white/60">n8n Relay</span>
              </div>
              <div className="flex items-center gap-2.5">
                <button
                  onClick={() => setAutoMode(v => !v)}
                  disabled={running}
                  className={`relative w-8 h-4 rounded-full transition-all duration-300 ${autoMode ? 'bg-emerald-400/80' : 'bg-white/10'}`}
                >
                  <div className={`absolute top-[2px] w-3 h-3 rounded-full shadow-sm transition-all duration-300 ${autoMode ? 'left-[18px] bg-black' : 'left-[2px] bg-white/40'}`} />
                </button>
                <span className="text-[10px] text-white/60">Auto Mode</span>
              </div>
              <button
                id="fini-run-btn"
                onClick={handleRun}
                disabled={running || !companies.trim()}
                className="flex items-center gap-2 px-5 py-2 rounded-xl bg-white text-black text-[10px] font-bold uppercase tracking-[0.12em] hover:bg-white/90 disabled:bg-white/10 disabled:text-white/55 transition-all duration-200"
              >
                {running ? (
                  <>
                    <div className="w-2.5 h-2.5 border-2 border-black/20 border-t-black rounded-full animate-spin" />
                    <span>Enriching…</span>
                  </>
                ) : (
                  <span>Run Enrichment</span>
                )}
              </button>
            </div>
          </div>

          {/* Pipeline log */}
          {threadId && (
            <div className="h-[340px] border border-white/[0.06] rounded-2xl bg-black/20 overflow-hidden flex flex-col">
              <LogStream threadId={threadId} onEvent={handleEvent} />
              {error && (
                <div className="px-4 py-2 border-t border-red-900/40 bg-red-950/30 text-[11px] text-red-400 flex-shrink-0">{error}</div>
              )}
            </div>
          )}

          {/* Pipeline Tracker — shows full agent chain status */}
          {(running || enrichmentDone) && <PipelineTracker />}
        </div>

        {/* Right: Queue Panel */}
        <div className="xl:col-span-3 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden flex flex-col">
          <div className="px-6 py-4 border-b border-white/[0.05] flex items-center justify-between">
            <div className="text-[10px] font-bold text-white/60 uppercase tracking-[0.35em]">
              {enrichmentDone ? 'Enrichment Results' : 'Pipeline Queue'}
            </div>
            {companyList.length > 0 && !enrichmentDone && (
              <span className="text-[10px] font-bold text-white/55 uppercase tracking-widest">{companyList.length} queued</span>
            )}
            {reviewCards.length > 0 && (
              <div className="flex items-center gap-4 text-right">
                <div><div className="text-sm font-bold text-white">{matchedCount}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Matched</div></div>
                <div className="h-4 w-[1px] bg-white/10" />
                <div><div className="text-sm font-bold text-white">{fallbackCount}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Fallback</div></div>
                <div className="h-4 w-[1px] bg-white/10" />
                <div><div className="text-sm font-bold text-white">{pendingCount}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Pending</div></div>
                <div className="h-4 w-[1px] bg-white/10" />
                <div><div className="text-sm font-bold text-white">{sentCount}</div><div className="text-[9px] text-white/60 uppercase tracking-widest">Synced</div></div>
              </div>
            )}
          </div>

          <div>
            {/* Empty state */}
            {!enrichmentDone && companyList.length === 0 && (
              <div className="flex flex-col items-center justify-center min-h-[300px] gap-3">
                <div className="w-10 h-10 rounded-full border border-white/[0.06] flex items-center justify-center">
                  <div className="w-1.5 h-1.5 rounded-full bg-white/10 animate-pulse" />
                </div>
                <p className="text-[11px] text-white/55 uppercase tracking-[0.25em]">Awaiting Input</p>
              </div>
            )}

            {/* Progress tracker — shown while running or before enrichment with companies */}
            {!enrichmentDone && companyList.length > 0 && (
              <>
                {/* Progress bar */}
                {running && (
                  <div className="px-6 pt-5 pb-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-[9px] font-bold text-white/50 uppercase tracking-[0.3em]">
                        {Object.values(enrichProgress).filter(s => s === 'done' || s === 'error').length} / {companyList.length} complete
                      </span>
                      <span className="text-[9px] font-mono text-white/35 tabular-nums">
                        {Math.floor(elapsedSecs / 60).toString().padStart(2, '0')}:{(elapsedSecs % 60).toString().padStart(2, '0')}
                      </span>
                    </div>
                    <div className="h-[2px] bg-white/[0.06] rounded-full overflow-hidden">
                      <div
                        className="h-full bg-white/40 rounded-full transition-all duration-700"
                        style={{
                          width: companyList.length > 0
                            ? `${(Object.values(enrichProgress).filter(s => s === 'done' || s === 'error').length / companyList.length) * 100}%`
                            : '0%'
                        }}
                      />
                    </div>
                  </div>
                )}

                {/* Company rows */}
                <div className="divide-y divide-white/[0.04]">
                  {companyList.map((name, i) => {
                    const rawStatus = enrichProgress[name] || 'queued';
                    // While running, treat unseen companies as processing (fallback for when
                    // backend events haven't arrived yet or backend is being restarted)
                    const status = running && rawStatus === 'queued' ? 'processing' : rawStatus;
                    return (
                      <div key={i} className="px-6 py-3.5 flex items-center justify-between">
                        <div className="flex items-center gap-3 min-w-0">
                          <span className="text-[10px] font-bold text-white/35 w-5 text-right flex-shrink-0">{i + 1}</span>
                          {/* Status dot */}
                          <div className="flex-shrink-0 w-4 h-4 flex items-center justify-center">
                            {status === 'queued' && (
                              <div className="w-1.5 h-1.5 rounded-full bg-white/20" />
                            )}
                            {status === 'processing' && (
                              <div className="relative flex w-2 h-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-white/50 opacity-75" style={{ animationDelay: `${i * 0.12}s` }} />
                                <span className="relative inline-flex rounded-full w-2 h-2 bg-white/70" />
                              </div>
                            )}
                            {status === 'done' && (
                              <span className="text-white/60 text-[11px] leading-none">✓</span>
                            )}
                            {status === 'error' && (
                              <span className="text-red-400/70 text-[11px] leading-none">✕</span>
                            )}
                          </div>
                          <span className={`text-sm font-medium truncate transition-colors duration-300 ${
                            status === 'done' ? 'text-white/75' :
                            status === 'processing' ? 'text-white' :
                            status === 'error' ? 'text-red-400/70' :
                            'text-white/45'
                          }`}>{name}</span>
                        </div>
                        <span className={`text-[9px] font-bold uppercase tracking-widest flex-shrink-0 transition-colors duration-300 ${
                          status === 'done' ? 'text-white/40' :
                          status === 'processing' ? 'text-white/65' :
                          status === 'error' ? 'text-red-400/60' :
                          'text-white/20'
                        }`}>
                          {status === 'queued' && 'Queued'}
                          {status === 'processing' && 'Enriching…'}
                          {status === 'done' && 'Done'}
                          {status === 'error' && 'Error'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </>
            )}

          </div>
        </div>
      </div>

      {/* ── Verify & Commit — full-width section below the grid ── */}
      {reviewCards.length > 0 && (
        <div className="mt-6 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden">

          {/* Section header + Send All */}
          <div className="px-6 py-4 border-b border-white/[0.05] flex items-center justify-between">
            <div>
              <div className="text-[10px] font-bold text-white/70 uppercase tracking-widest">Verify & Commit</div>
              <div className="text-[9px] text-white/35 mt-0.5 uppercase tracking-wider">Verify Sales Nav link, then send or skip</div>
            </div>
            {pendingCount > 0 && (
              <button
                onClick={handleSendAll}
                disabled={isSendingAll}
                className={`flex items-center gap-1.5 px-4 py-1.5 rounded-xl border text-[10px] font-bold uppercase tracking-wider transition-all ${
                  isSendingAll
                    ? 'border-white/5 bg-white/[0.03] text-white/30 cursor-not-allowed'
                    : 'border-white/10 bg-white/5 hover:bg-white/10 text-white cursor-pointer'
                }`}
              >
                {isSendingAll ? <><span className="animate-spin">↻</span><span>Sending…</span></> : <><span>↑</span><span>Send All</span></>}
                <span className="bg-white/10 px-1.5 py-0.5 rounded text-[9px]">{pendingCount}</span>
              </button>
            )}
          </div>

          {/* Multi-match groups */}
          {(() => {
            const groupMap = new Map<string, number[]>();
            reviewCards.forEach((c, i) => {
              if (c.isCandidate && c.status !== 'skipped') {
                const gid = c.groupId!;
                if (!groupMap.has(gid)) groupMap.set(gid, []);
                groupMap.get(gid)!.push(i);
              }
            });
            if (groupMap.size === 0) return null;
            return (
              <div className="p-6 space-y-4">
                {Array.from(groupMap.entries()).map(([groupId, indices]) => (
                  <div key={groupId} className="rounded-xl border border-amber-500/20 bg-amber-500/[0.03] overflow-hidden">
                    <div className="px-4 py-2.5 border-b border-amber-500/15 flex items-center gap-2">
                      <span className="text-amber-400 text-xs">⚠</span>
                      <div className="flex-1 min-w-0">
                        <div className="text-[9px] font-bold text-amber-400/90 uppercase tracking-[0.2em]">Multiple matches — "{groupId}"</div>
                      </div>
                      <div className="text-[8px] text-amber-400/50 uppercase tracking-widest">{indices.length} options</div>
                    </div>
                    <div className="p-3 grid grid-cols-1 xl:grid-cols-3 gap-3">
                      {indices.map(i => {
                        const card = reviewCards[i];
                        return (
                          <div key={i} className="rounded-xl border border-white/[0.07] bg-white/[0.02] overflow-hidden flex flex-col">
                            <div className={`h-[2px] w-full ${card.data.linkedin_confidence === 'high' ? 'bg-emerald-500/60' : card.data.linkedin_confidence === 'medium' ? 'bg-amber-500/60' : 'bg-red-500/50'}`} />
                            <div className="p-3 space-y-2 flex-1">
                              <div className="text-xs font-bold text-white leading-tight">{card.editCompanyName}</div>
                              <div className="flex items-center gap-2">
                                <div className="flex-1 min-w-0 text-[10px] font-mono text-white/35 truncate">{card.editSalesNavUrl ? 'linkedin.com/sales/…' : 'No link'}</div>
                                {card.editSalesNavUrl && (
                                  <a href={card.editSalesNavUrl} target="_blank" rel="noopener noreferrer" className="flex-shrink-0 px-2 py-0.5 border border-white/10 rounded text-[10px] text-white/60 hover:text-white/90 transition-colors">↗</a>
                                )}
                              </div>
                              {card.editDomain && <div className="text-[10px] font-mono text-white/40">{card.editDomain}</div>}
                            </div>
                            <div className="px-3 pb-3 flex gap-2">
                              <button onClick={() => handleKeepCandidate(i)} className="flex-1 py-1.5 rounded-lg bg-white text-black text-[10px] font-bold uppercase tracking-wider hover:bg-white/90 transition-all">Keep</button>
                              <button onClick={() => handleSkipCandidate(i)} className="px-3 py-1.5 rounded-lg border border-white/[0.07] text-white/55 hover:text-white/80 text-[10px] font-bold uppercase tracking-wider transition-all">Skip</button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            );
          })()}

          {/* Normal single-match cards */}
          <div className="p-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
            {reviewCards.map((card, index) => {
              if (card.isCandidate) return null;
              if (card.status === 'skipped') return null;
              return (
                <ReviewCardItem
                  key={`${card.data.raw_name}-${index}`}
                  card={card}
                  index={index}
                  submitN8n={submitN8n}
                  onSend={handleSend}
                  onSendN8n={handleSendN8n}
                  onSendBoth={handleSendBoth}
                  onSkip={handleSkip}
                  onFieldChange={handleFieldChange}
                  onReenrich={handleReenrich}
                />
              );
            })}
          </div>

          {pendingCount === 0 && (
            <div className="mx-6 mb-6 flex items-center gap-3 p-4 border border-white/[0.06] rounded-xl bg-white/[0.02]">
              <span className="text-white/50 text-xs">✓</span>
              <div>
                <div className="text-[10px] font-bold text-white/50 uppercase tracking-widest">Review Complete</div>
                <div className="text-[10px] text-white/35 mt-0.5">{sentCount} synced · {skippedCount} skipped</div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
