'use client';

import React, { useState, useCallback, useEffect, useRef, Suspense } from 'react';
import { useVeriStore } from '@/lib/stores';
import { useSearchParams } from 'next/navigation';
import { apiUrl } from '@/lib/api';
import LogStream from '@/components/LogStream';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ContactPhase = 'queued' | 'web' | 'linkedin_zb' | 'scoring' | 'done';
type ContactStatus = 'VERIFIED' | 'REVIEW' | 'REJECT' | '';

interface ContactSignals {
  linkedin?: string;  // confirmed | uncertain | moved | error | no_url
  linkedin_detail?: string;
  email?:    string;  // valid | catch-all | unknown | invalid | no_email | skipped
  email_detail?: string;
  web?:      string;  // positive | stale | inconclusive
  web_detail?: string;
  title?:    string;  // match | mismatch | unknown
  title_detail?: string;
}

interface ContactCard {
  name: string;
  company: string;
  phase: ContactPhase;
  status: ContactStatus;
  rejectReason?: string;
  reviewFlags?: string[];   // specific issues that caused REVIEW
  sheetRow?: number;        // row in Final Filtered List (VERIFIED/REVIEW)
  rejectSheetRow?: number;  // row in Reject profiles (REJECT)
  emailValidated?: boolean; // false when ZeroBounce was unavailable
  signals?: ContactSignals; // per-check signal for coloured blocks
}

interface ActivityEntry {
  ts: string;
  name?: string;
  step?: string;
  detail: string;
  level: 'info' | 'success' | 'warning' | 'error' | 'system';
  phase?: string;
}

// ---------------------------------------------------------------------------
// Step metadata — tool badges
// ---------------------------------------------------------------------------

const STEP_META: Record<string, { label: string; bg: string; text: string }> = {
  linkedin_discovery: { label: 'LI Search',  bg: 'bg-violet-500/20', text: 'text-violet-300' },
  ddg:               { label: 'DDG ×3',      bg: 'bg-violet-500/20', text: 'text-violet-300' },
  theorg:            { label: 'TheOrg',      bg: 'bg-violet-500/20', text: 'text-violet-300' },
  perplexity:        { label: 'Perplexity',  bg: 'bg-violet-500/20', text: 'text-violet-300' },
  tavily:            { label: 'Tavily ↩',    bg: 'bg-violet-500/20', text: 'text-violet-300' },
  linkedin:          { label: 'Unipile',     bg: 'bg-cyan-500/20',   text: 'text-cyan-300'   },
  zerobounce:        { label: 'ZeroBounce',  bg: 'bg-cyan-500/20',   text: 'text-cyan-300'   },
  signals:           { label: 'Signals',     bg: 'bg-amber-500/20',  text: 'text-amber-300'  },
  llm_title:         { label: 'LLM Title',   bg: 'bg-amber-500/20',  text: 'text-amber-300'  },
  llm_reason:        { label: 'LLM Reason',  bg: 'bg-amber-500/20',  text: 'text-amber-300'  },
  verdict:           { label: 'Verdict',     bg: 'bg-white/10',      text: 'text-white/60'   },
  sheet:             { label: 'Sheet Write', bg: 'bg-white/10',      text: 'text-white/50'   },
};

const LEVEL_COLOR: Record<string, string> = {
  info:    'text-white/50',
  success: 'text-emerald-400',
  warning: 'text-amber-400',
  error:   'text-red-400',
  system:  'text-white/25',
};

// ---------------------------------------------------------------------------
// Activity Feed
// ---------------------------------------------------------------------------

function ActivityFeed({ entries, running }: { entries: ActivityEntry[]; running: boolean }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, autoScroll]);

  const handleScroll = () => {
    if (!scrollRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = scrollRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 40);
  };

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="px-4 py-2.5 border-b border-white/[0.06] flex items-center justify-between flex-shrink-0 bg-white/[0.01]">
        <div className="flex items-center gap-2.5">
          <div className="relative w-1.5 h-1.5">
            {running && <span className="absolute inset-0 rounded-full bg-emerald-400 animate-ping opacity-75" />}
            <span className={`relative block w-1.5 h-1.5 rounded-full ${running ? 'bg-emerald-400' : 'bg-white/15'}`} />
          </div>
          <span className="text-[9px] font-mono text-white/35 uppercase tracking-[0.3em]">
            Agent Activity{entries.length > 0 ? ` · ${entries.length} events` : ''}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {!autoScroll && (
            <button
              onClick={() => { setAutoScroll(true); if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight; }}
              className="text-[8px] font-mono text-amber-400/60 hover:text-amber-400 uppercase tracking-wider px-2 py-0.5 rounded border border-amber-500/20 hover:bg-amber-500/10 transition-colors"
            >
              ↓ scroll to bottom
            </button>
          )}
        </div>
      </div>

      {/* Log rows */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 min-h-0 overflow-y-auto font-mono text-[10px] no-scrollbar"
      >
        {entries.length === 0 ? (
          <div className="h-full flex items-center justify-center">
            <span className={`text-[9px] text-white/20 uppercase tracking-widest ${running ? 'animate-pulse' : ''}`}>
              {running ? 'waiting for first event…' : 'run verification to see agent activity'}
            </span>
          </div>
        ) : (
          <div className="py-1">
            {entries.map((e, i) => {
              const stepMeta = e.step ? STEP_META[e.step] : null;
              const levelColor = LEVEL_COLOR[e.level] || 'text-white/40';
              const isVerdict = e.step === 'verdict';
              const isSheet = e.step === 'sheet';

              return (
                <div
                  key={i}
                  className={`flex items-start gap-0 px-3 py-[2px] hover:bg-white/[0.02] transition-colors ${
                    isVerdict ? 'border-t border-b border-white/[0.04] bg-white/[0.01] my-0.5' : ''
                  }`}
                >
                  {/* Timestamp */}
                  <span className="text-white/20 tabular-nums shrink-0 w-[52px] pt-[1px]">
                    {new Date(e.ts).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                  </span>

                  {/* Contact name */}
                  {e.name ? (
                    <span className="text-white/40 shrink-0 max-w-[90px] truncate mx-1.5 pt-[1px]" title={e.name}>
                      {e.name.split(' ')[0]}
                    </span>
                  ) : (
                    <span className="text-white/15 shrink-0 mx-1.5 pt-[1px]">sys</span>
                  )}

                  {/* Divider */}
                  <span className="text-white/10 shrink-0 pt-[1px]">·</span>

                  {/* Step badge */}
                  {stepMeta ? (
                    <span className={`shrink-0 mx-1.5 px-1.5 py-0 rounded text-[8px] font-bold ${stepMeta.bg} ${stepMeta.text}`}>
                      {stepMeta.label}
                    </span>
                  ) : (
                    <span className="shrink-0 mx-1.5 px-1.5 py-0 rounded text-[8px] font-bold bg-white/[0.06] text-white/30">
                      {e.step || 'log'}
                    </span>
                  )}

                  {/* Detail */}
                  <span className={`leading-relaxed break-all ${levelColor} ${isVerdict ? 'font-semibold' : ''} ${isSheet ? 'text-white/35' : ''}`}>
                    {e.detail}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Contact Tile
// ---------------------------------------------------------------------------

const PHASE_META: Record<ContactPhase, { label: string; color: string; pulse: boolean }> = {
  queued:      { label: 'Queued',        color: 'text-white/30',   pulse: false },
  web:         { label: 'Web Search',    color: 'text-violet-400', pulse: true  },
  linkedin_zb: { label: 'LinkedIn + ZB', color: 'text-cyan-400',   pulse: true  },
  scoring:     { label: 'Scoring',       color: 'text-amber-400',  pulse: true  },
  done:        { label: 'Done',          color: 'text-white/50',   pulse: false },
};

const STATUS_META: Record<string, { color: string; bg: string; border: string; dot: string }> = {
  VERIFIED: { color: 'text-emerald-400', bg: 'bg-emerald-400/[0.08]', border: 'border-emerald-400/20', dot: 'bg-emerald-400' },
  REVIEW:   { color: 'text-amber-400',   bg: 'bg-amber-400/[0.08]',   border: 'border-amber-400/20',   dot: 'bg-amber-400'   },
  REJECT:   { color: 'text-red-400',     bg: 'bg-red-400/[0.08]',     border: 'border-red-400/20',     dot: 'bg-red-400'     },
};

// ---------------------------------------------------------------------------
// Signal checklist — colours per status value
// ---------------------------------------------------------------------------

type SignalColor = 'green' | 'amber' | 'red' | 'gray';

const SIGNAL_COLORS: Record<SignalColor, { dot: string; label: string; detail: string }> = {
  green: { dot: 'bg-emerald-400', label: 'text-emerald-300/90', detail: 'text-emerald-300/60' },
  amber: { dot: 'bg-amber-400',   label: 'text-amber-300/90',   detail: 'text-amber-300/60'   },
  red:   { dot: 'bg-red-400',     label: 'text-red-300/90',     detail: 'text-red-300/60'     },
  gray:  { dot: 'bg-zinc-500',    label: 'text-zinc-400/90',    detail: 'text-zinc-400/60'    },
};

function signalColor(type: string, value: string | undefined): SignalColor {
  if (!value) return 'gray';
  const map: Record<string, Record<string, SignalColor>> = {
    linkedin: { confirmed: 'green', uncertain: 'amber', moved: 'red', error: 'amber', no_url: 'gray' },
    email:    { valid: 'green', 'catch-all': 'green', unknown: 'amber', invalid: 'red', no_email: 'red', skipped: 'gray' },
    web:      { positive: 'green', stale: 'red', inconclusive: 'amber' },
    title:    { match: 'green', mismatch: 'red', unknown: 'gray' },
  };
  return map[type]?.[value] ?? 'gray';
}

const SIGNAL_LABEL: Record<string, string> = {
  linkedin: 'LinkedIn',
  email:    'Email',
  web:      'Web',
  title:    'Title',
};

function SignalRow({ type, value, detail }: { type: string; value?: string; detail?: string }) {
  const color = signalColor(type, value);
  const c = SIGNAL_COLORS[color];
  return (
    <div className="flex items-start gap-1.5 min-w-0">
      <span className={`shrink-0 mt-[3px] w-1.5 h-1.5 rounded-full ${c.dot}`} />
      <span className={`shrink-0 text-[8px] font-bold w-[42px] ${c.label}`}>{SIGNAL_LABEL[type] || type}</span>
      <span className={`text-[8px] ${c.detail} truncate min-w-0`}>{detail || value || '—'}</span>
    </div>
  );
}

function ContactTile({ card }: { card: ContactCard }) {
  const phase = PHASE_META[card.phase];
  const statusM = card.status ? STATUS_META[card.status] : null;
  const isDone = card.phase === 'done';

  // For REVIEW cards — amber border + faint amber background
  const isReview   = isDone && card.status === 'REVIEW';
  const isReject   = isDone && card.status === 'REJECT';
  const isVerified = isDone && card.status === 'VERIFIED';

  const borderClass = isReview
    ? 'border-amber-400/30'
    : statusM ? statusM.border : 'border-white/[0.07]';
  const bgClass = isReview
    ? 'bg-amber-400/[0.05]'
    : statusM ? statusM.bg : 'bg-white/[0.02]';

  return (
    <div className={`relative rounded-xl border ${borderClass} ${bgClass} p-3 flex flex-col gap-1.5 overflow-hidden`}>

      {/* Top colour bar */}
      <div className="absolute top-0 left-0 right-0 h-[2px] overflow-hidden rounded-t-xl">
        {!isDone ? (
          <div className={`h-full w-full animate-pulse ${
            card.phase === 'web'         ? 'bg-violet-500' :
            card.phase === 'linkedin_zb' ? 'bg-cyan-500'   :
            card.phase === 'scoring'     ? 'bg-amber-500'  : 'bg-white/10'
          }`} />
        ) : (
          <div className={`h-full w-full ${statusM?.dot || 'bg-white/10'}`} />
        )}
      </div>

      {/* Name + status badge */}
      <div className="flex items-start justify-between gap-2 mt-0.5">
        <div className="min-w-0">
          <div className="text-[11px] font-semibold text-white/85 leading-tight truncate">{card.name}</div>
          <div className="text-[9px] text-white/30 truncate mt-0.5">{card.company}</div>
        </div>
        {card.status && (
          <span className={`shrink-0 text-[8px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded-md border ${statusM?.border} ${statusM?.color} ${statusM?.bg}`}>
            {card.status}
          </span>
        )}
      </div>

      {/* Phase pulse (while running) */}
      {!isDone && (
        <div className="flex items-center gap-1.5">
          {phase.pulse && (
            <span className="relative flex h-1.5 w-1.5 shrink-0">
              <span className={`animate-ping absolute inset-0 rounded-full opacity-75 ${
                card.phase === 'web' ? 'bg-violet-400' : card.phase === 'linkedin_zb' ? 'bg-cyan-400' : 'bg-amber-400'
              }`} />
              <span className={`relative block h-1.5 w-1.5 rounded-full ${
                card.phase === 'web' ? 'bg-violet-400' : card.phase === 'linkedin_zb' ? 'bg-cyan-400' : 'bg-amber-400'
              }`} />
            </span>
          )}
          <span className={`text-[9px] font-mono ${phase.color}`}>{phase.label}</span>
        </div>
      )}

      {/* ── Signal checklist (all done cards) ── */}
      {isDone && card.signals && (
        <div className="border-t border-white/[0.06] pt-1.5 mt-0.5 flex flex-col gap-[3px]">
          <SignalRow type="linkedin" value={card.signals.linkedin} detail={card.signals.linkedin_detail} />
          <SignalRow type="email"    value={card.signals.email}    detail={card.signals.email_detail}    />
          <SignalRow type="web"      value={card.signals.web}      detail={card.signals.web_detail}      />
          <SignalRow type="title"    value={card.signals.title}    detail={card.signals.title_detail}    />
        </div>
      )}

      {/* ── REJECT: routing line ── */}
      {isReject && (
        <div className="text-[8px] font-mono text-red-400/50 border-t border-red-400/10 pt-1.5 mt-0.5">
          {card.rejectSheetRow
            ? `→ Reject profiles · row ${card.rejectSheetRow} · removed from Filtered List`
            : 'Removed from Final Filtered List'}
        </div>
      )}

      {/* ── REVIEW: routing line ── */}
      {isReview && (
        <div className="text-[8px] font-mono text-amber-400/45 border-t border-amber-400/10 pt-1.5 mt-0.5">
          {card.sheetRow
            ? `Final Filtered List · row ${card.sheetRow} · awaiting review`
            : 'Final Filtered List · awaiting review'}
        </div>
      )}

      {/* ── VERIFIED: routing line ── */}
      {isVerified && (
        <div className="text-[8px] font-mono text-emerald-400/50 border-t border-emerald-400/10 pt-1.5 mt-0.5">
          {card.sheetRow
            ? `Final Filtered List · row ${card.sheetRow} · confirmed`
            : 'Final Filtered List · confirmed'}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// How It Works Modal
// ---------------------------------------------------------------------------

function HowItWorksModal({ onClose }: { onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" />
      <div
        className="relative z-10 w-full max-w-3xl max-h-[88vh] overflow-y-auto rounded-2xl border border-white/[0.1] bg-[#0d0d0d] shadow-2xl no-scrollbar"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between px-6 py-4 border-b border-white/[0.07] bg-[#0d0d0d]">
          <div className="flex items-center gap-3">
            <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.4em]">Module 03 · Quality Control</span>
            <span className="h-3 w-[1px] bg-white/10" />
            <h2 className="text-sm font-bold text-white tracking-tight">Veri — How It Works</h2>
          </div>
          <button onClick={onClose} className="text-white/30 hover:text-white/70 text-lg leading-none transition-colors">✕</button>
        </div>

        <div className="px-6 py-5 flex flex-col gap-6 text-[11px] leading-relaxed text-white/60">

          {/* Intro */}
          <p className="text-white/50">
            Veri runs a <span className="text-white/80 font-semibold">4-phase verification pipeline</span> for every contact in the Final Filtered List.
            Up to <span className="text-white/80 font-semibold">6 contacts are processed in parallel</span> (Semaphore(6)).
            All sheet writes are serialised via a lock to avoid row conflicts.
          </p>

          {/* Pipeline overview */}
          <Section title="Pipeline Overview">
            <div className="flex flex-col gap-1.5">
              {[
                { phase: '0', label: 'LinkedIn URL Discovery', desc: 'If no URL in sheet — Unipile name search, fuzzy match ≥ 85%' },
                { phase: '1', label: 'Web Intelligence', desc: 'DDG ×3 + TheOrg + Perplexity + Tavily fallback — all parallel' },
                { phase: '2', label: 'Deep Verify', desc: 'Unipile LinkedIn audit + ZeroBounce email check — both parallel' },
                { phase: '3', label: 'Scoring', desc: 'LLM title compare + LLM cross-reasoning (only when uncertain)' },
                { phase: '4', label: 'Write', desc: 'Update Final Filtered List cols O–U · REJECT → Reject profiles + delete row' },
              ].map(r => (
                <div key={r.phase} className="flex items-start gap-3">
                  <span className="shrink-0 w-5 h-5 flex items-center justify-center rounded-md bg-white/[0.06] text-[9px] font-bold text-white/40">{r.phase}</span>
                  <div>
                    <span className="text-white/80 font-semibold">{r.label}</span>
                    <span className="text-white/40"> — {r.desc}</span>
                  </div>
                </div>
              ))}
            </div>
          </Section>

          {/* Phase 1 detail */}
          <Section title="Phase 1 — Web Intelligence (4 sources)">
            <div className="grid grid-cols-2 gap-2">
              {[
                { tool: 'DDG ×3', color: 'text-violet-400', desc: '3 parallel queries: name+company, name+role+company, name+LinkedIn+company. Looks for positive presence or stale signals (ex-, former, moved to, left…)' },
                { tool: 'TheOrg', color: 'text-violet-400', desc: 'Org-chart lookup. If found: strong identity + title confirmation. Not found = no penalty.' },
                { tool: 'Perplexity', color: 'text-violet-400', desc: 'Structured question: "Is {name} currently at {company} in 2025?" Returns presence confirmation + 2024/25 recency tag.' },
                { tool: 'Tavily', color: 'text-violet-400', desc: 'Deep search fallback. Only fires when DDG returns NEITHER positive NOR stale signal. Skipped otherwise.' },
              ].map(s => (
                <div key={s.tool} className="border border-white/[0.06] rounded-xl p-3">
                  <div className={`text-[9px] font-bold uppercase tracking-widest mb-1 ${s.color}`}>{s.tool}</div>
                  <p className="text-white/40">{s.desc}</p>
                </div>
              ))}
            </div>
          </Section>

          {/* Phase 2 detail */}
          <Section title="Phase 2 — Deep Verify (2 sources in parallel)">
            <div className="grid grid-cols-2 gap-2">
              <div className="border border-white/[0.06] rounded-xl p-3">
                <div className="text-[9px] font-bold uppercase tracking-widest mb-1 text-cyan-400">Unipile LinkedIn Audit</div>
                <p className="text-white/40 mb-2">Reads the full LinkedIn profile via Unipile API. Extracts:</p>
                <ul className="flex flex-col gap-0.5 text-white/40">
                  <li><span className="text-white/60">current_company</span> — where they work now</li>
                  <li><span className="text-white/60">at_target_company</span> — matches our target?</li>
                  <li><span className="text-white/60">still_employed</span> — active position (not end-dated)?</li>
                  <li><span className="text-white/60">current_role</span> — live title from LinkedIn</li>
                </ul>
                <p className="text-white/30 mt-2 text-[10px]">301 redirect errors = profile URL changed. Falls back to LLM cross-reasoning.</p>
              </div>
              <div className="border border-white/[0.06] rounded-xl p-3">
                <div className="text-[9px] font-bold uppercase tracking-widest mb-1 text-cyan-400">ZeroBounce Email</div>
                <p className="text-white/40 mb-2">Validates email deliverability. Returns:</p>
                <ul className="flex flex-col gap-0.5 text-white/40">
                  <li><span className="text-emerald-400/70">valid</span> — definitely deliverable → email_ok ✓</li>
                  <li><span className="text-emerald-400/70">catch-all</span> — domain accepts all → email_ok ✓</li>
                  <li><span className="text-amber-400/70">unknown</span> — can't probe (corp domains) → email_ok ✗</li>
                  <li><span className="text-red-400/70">invalid</span> — bounces → email_ok ✗</li>
                  <li><span className="text-white/30">no_email</span> — no address in sheet</li>
                </ul>
                <p className="text-white/30 mt-2 text-[10px]">Corporate domains like @effem.com (Mars) often return "unknown" — this is NOT a failure, it means unverifiable.</p>
              </div>
            </div>
          </Section>

          {/* Scoring */}
          <Section title="Phase 3 — Scoring Logic">
            <div className="flex flex-col gap-3">
              <ScoreBlock label="Identity" color="text-violet-300">
                <ScoreRow signal="LinkedIn profile loaded (valid=True)" result="CONFIRMED" />
                <ScoreRow signal="Found on TheOrg" result="CONFIRMED" />
                <ScoreRow signal="2+ of [DDG, Tavily, Perplexity] positive" result="CONFIRMED" />
                <ScoreRow signal="None of the above" result="UNCONFIRMED" />
              </ScoreBlock>
              <ScoreBlock label="Employment" color="text-cyan-300">
                <ScoreRow signal="LinkedIn valid + at_target + still_employed" result="CONFIRMED" />
                <ScoreRow signal="LinkedIn valid + at_target + NOT still_employed" result="UNCERTAIN" />
                <ScoreRow signal="LinkedIn valid + current_company ≠ target" result="REJECTED" color="text-red-400" />
                <ScoreRow signal="LinkedIn inaccessible + 2+ stale web signals" result="REJECTED" color="text-red-400" />
                <ScoreRow signal="LinkedIn inaccessible + few signals" result="UNCERTAIN" />
              </ScoreBlock>
              <ScoreBlock label="Title Match" color="text-amber-300">
                <ScoreRow signal="Word overlap ≥ 50% between sheet title and LinkedIn title" result="MATCH" />
                <ScoreRow signal="Clearly different function (Marketing vs Finance etc.)" result="MISMATCH" color="text-red-400" />
                <ScoreRow signal="Ambiguous — LLM semantic comparison fires" result="MATCH / MISMATCH / UNKNOWN" />
                <ScoreRow signal="Only one title available" result="UNKNOWN (treated as MATCH)" />
              </ScoreBlock>
              <div className="border border-amber-500/15 rounded-xl p-3 bg-amber-500/[0.03]">
                <div className="text-[9px] font-bold text-amber-400/70 uppercase tracking-widest mb-1.5">LLM Cross-Reasoning (GPT-4.1-mini)</div>
                <p className="text-white/40">Fires <span className="text-white/60">only when</span> employment=UNCERTAIN or identity=UNCONFIRMED. Reads all evidence — LinkedIn audit, DDG/Perplexity/TheOrg/Tavily snippets, email status — and produces final identity/employment/role_match + explanation. Saves API cost by being selective.</p>
              </div>
            </div>
          </Section>

          {/* Decision tree */}
          <Section title="Verdict Decision Tree">
            <div className="flex flex-col gap-1.5">
              {[
                { cond: 'Identity UNCONFIRMED + email invalid/missing', verdict: 'REJECT', color: 'text-red-400', note: 'Ghost contact — can\'t verify exists' },
                { cond: 'Identity UNCONFIRMED + email valid', verdict: 'REVIEW', color: 'text-amber-400', note: 'Email works but no web/LinkedIn proof' },
                { cond: 'Employment REJECTED + LinkedIn shows different company', verdict: 'REJECT', color: 'text-red-400', note: '"now at X, not Company"' },
                { cond: 'Employment REJECTED + 2+ stale web signals', verdict: 'REJECT', color: 'text-red-400', note: 'Stale contact, person left' },
                { cond: 'Employment CONFIRMED + title MISMATCH (different dept)', verdict: 'REJECT', color: 'text-red-400', note: 'Wrong person for ICP' },
                { cond: 'Employment CONFIRMED + email valid/catch-all', verdict: 'VERIFIED', color: 'text-emerald-400', note: 'Full confidence — ready to outreach' },
                { cond: 'Employment CONFIRMED + email unknown/invalid', verdict: 'REVIEW', color: 'text-amber-400', note: 'Real person, uncertain email' },
                { cond: 'Employment UNCERTAIN (LinkedIn inaccessible)', verdict: 'REVIEW', color: 'text-amber-400', note: 'Needs manual check' },
              ].map((row, i) => (
                <div key={i} className="flex items-start gap-3 border border-white/[0.04] rounded-lg px-3 py-2">
                  <span className={`shrink-0 text-[8px] font-bold uppercase tracking-widest w-14 pt-[1px] ${row.color}`}>{row.verdict}</span>
                  <div className="min-w-0">
                    <span className="text-white/65">{row.cond}</span>
                    <span className="text-white/30"> — {row.note}</span>
                  </div>
                </div>
              ))}
            </div>
          </Section>

          {/* Sheet routing */}
          <Section title="Sheet Routing After Verdict">
            <div className="grid grid-cols-3 gap-2">
              <div className="border border-emerald-400/15 rounded-xl p-3 bg-emerald-400/[0.03]">
                <div className="text-[9px] font-bold text-emerald-400 uppercase tracking-widest mb-1.5">VERIFIED</div>
                <p className="text-white/40">Cols O–U written in-place to <span className="text-white/60">Final Filtered List</span>. Row stays. Status = VERIFIED.</p>
              </div>
              <div className="border border-amber-400/15 rounded-xl p-3 bg-amber-400/[0.03]">
                <div className="text-[9px] font-bold text-amber-400 uppercase tracking-widest mb-1.5">REVIEW</div>
                <p className="text-white/40">Cols O–U written in-place to <span className="text-white/60">Final Filtered List</span>. Row stays. Status = REVIEW. SDR decides.</p>
              </div>
              <div className="border border-red-400/15 rounded-xl p-3 bg-red-400/[0.03]">
                <div className="text-[9px] font-bold text-red-400 uppercase tracking-widest mb-1.5">REJECT</div>
                <p className="text-white/40">Full row A–U copied to <span className="text-white/60">Reject profiles</span> tab. Then <span className="text-red-400/70">row deleted</span> from Final Filtered List.</p>
              </div>
            </div>
            <div className="mt-2 border border-white/[0.05] rounded-xl p-3">
              <p className="text-white/35 text-[10px]">
                <span className="text-white/50 font-semibold">Sheet cols written by Veri (O–U):</span>{' '}
                O = LinkedIn Status · P = Employment Verified · Q = Title Match · R = Actual Title Found · S = Overall Status · T = Verification Notes · U = Verified On
              </p>
            </div>
          </Section>

          {/* Footer note */}
          <div className="border border-white/[0.05] rounded-xl p-3 bg-white/[0.01]">
            <p className="text-white/30 text-[10px]">
              <span className="text-white/45 font-semibold">Testing checklist:</span>{' '}
              ① LinkedIn audit fires and returns at_target/still_employed correctly ·
              ② ZeroBounce validates email and returns correct status ·
              ③ LLM cross-reasoning only fires for UNCERTAIN/UNCONFIRMED cases ·
              ④ REJECT rows copied to Reject profiles tab with reason ·
              ⑤ REJECT rows deleted from Final Filtered List ·
              ⑥ Row numbers in sheet match what's shown in activity feed ·
              ⑦ Row range selector (start/end) maps to exact sheet rows ·
              ⑧ All 6 parallel contacts show individual steps in Activity Feed
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-[9px] font-bold text-white/35 uppercase tracking-[0.3em] mb-2.5">{title}</div>
      {children}
    </div>
  );
}

function ScoreBlock({ label, color, children }: { label: string; color: string; children: React.ReactNode }) {
  return (
    <div className="border border-white/[0.06] rounded-xl p-3">
      <div className={`text-[9px] font-bold uppercase tracking-widest mb-2 ${color}`}>{label}</div>
      <div className="flex flex-col gap-1">{children}</div>
    </div>
  );
}

function ScoreRow({ signal, result, color = 'text-emerald-400' }: { signal: string; result: string; color?: string }) {
  return (
    <div className="flex items-start gap-2 text-[10px]">
      <span className="text-white/25 shrink-0 mt-[1px]">→</span>
      <span className="text-white/50 flex-1">{signal}</span>
      <span className={`shrink-0 font-semibold ${color}`}>{result}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Workflow Diagram (left panel)
// ---------------------------------------------------------------------------

function WorkflowDiagram({ activePhase }: { activePhase: ContactPhase | null }) {
  const PhaseRow = ({
    phaseId, label, sublabel, tools, colorClass, borderActive, borderIdle, textActive,
  }: {
    phaseId: string; label: string; sublabel: string;
    tools: { name: string; note: string }[];
    colorClass: string; borderActive: string; borderIdle: string; textActive: string;
  }) => {
    const active = activePhase === phaseId;
    return (
      <div className={`rounded-xl border transition-all duration-300 ${active ? borderActive : borderIdle} ${active ? 'bg-white/[0.04]' : 'bg-white/[0.01]'} p-3`}>
        <div className="flex items-center justify-between mb-2">
          <div>
            <div className={`text-[10px] font-bold ${active ? textActive : 'text-white/60'}`}>{label}</div>
            <div className="text-[9px] text-white/30 mt-0.5">{sublabel}</div>
          </div>
          {active && (
            <span className={`text-[8px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded-md border ${borderActive} ${textActive} bg-black/20 animate-pulse`}>
              active
            </span>
          )}
        </div>
        <div className="flex flex-wrap gap-1.5">
          {tools.map((t) => (
            <div key={t.name} className={`flex items-center gap-1 px-2 py-1 rounded-lg border ${active ? borderActive : 'border-white/[0.06]'} bg-black/20`}>
              <span className={`text-[9px] font-mono font-semibold ${active ? colorClass : 'text-white/40'}`}>{t.name}</span>
              <span className="text-[8px] text-white/20">· {t.note}</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-2 px-3">
        <div className="w-2 h-2 rounded-full bg-white/20" />
        <span className="text-[9px] font-mono text-white/25 uppercase tracking-widest">START</span>
      </div>
      <div className="flex justify-center"><div className="w-[1px] h-3 bg-white/10" /></div>

      {/* read_final_list */}
      <div className="rounded-xl border border-blue-500/20 bg-blue-500/[0.04] p-3">
        <div className="flex items-center gap-2 mb-1.5">
          <span className="text-[9px] font-mono text-blue-400/60 uppercase tracking-widest">node</span>
          <span className="text-[10px] font-bold text-white/70">read_final_list</span>
        </div>
        <div className="text-[9px] text-white/30 mb-2">Pull contacts · ensure Rejected Profiles tab</div>
        <div className="flex items-center gap-1.5">
          <div className="px-2 py-1 rounded-lg border border-blue-500/15 bg-black/20">
            <span className="text-[9px] font-mono text-blue-400/70">Google Sheets API</span>
          </div>
        </div>
      </div>

      <div className="flex justify-center"><div className="w-[1px] h-3 bg-white/10" /></div>

      {/* parallel_verify_all */}
      <div className="rounded-xl border border-violet-500/20 bg-violet-500/[0.03] p-3">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="text-[9px] font-mono text-violet-400/60 uppercase tracking-widest">node</span>
          <span className="text-[10px] font-bold text-white/70">parallel_verify_all</span>
        </div>
        <div className="text-[9px] text-white/30 mb-3">asyncio.Semaphore(6) — 6 contacts at once</div>

        <div className="flex flex-col gap-1.5 pl-3 border-l border-white/[0.06]">
          <PhaseRow phaseId="web" label="Phase 0+1 · Web Intelligence"
            sublabel="LinkedIn discovery → DDG ×3 + TheOrg + Perplexity → Tavily fallback"
            tools={[
              { name: 'LI Search', note: 'find URL by name' },
              { name: 'DDG ×3', note: '3 concurrent queries' },
              { name: 'TheOrg', note: 'org chart lookup' },
              { name: 'Perplexity', note: 'role extraction, 2025' },
              { name: 'Tavily ↩', note: 'if DDG inconclusive' },
            ]}
            colorClass="text-violet-400" borderActive="border-violet-400/30"
            borderIdle="border-white/[0.05]" textActive="text-violet-400" />

          <PhaseRow phaseId="linkedin_zb" label="Phase 2 · Deep Verify"
            sublabel="LinkedIn audit + ZeroBounce — both in parallel"
            tools={[
              { name: 'Unipile API', note: 'profile, employment, role' },
              { name: 'ZeroBounce', note: 'email deliverability' },
            ]}
            colorClass="text-cyan-400" borderActive="border-cyan-400/30"
            borderIdle="border-white/[0.05]" textActive="text-cyan-400" />

          <PhaseRow phaseId="scoring" label="Phase 3+4 · Reason + Route"
            sublabel="LLM title & cross-reasoning → verdict → sheet routing"
            tools={[
              { name: 'LLM Title', note: 'GPT semantic compare' },
              { name: 'LLM Reason', note: 'cross-signal synthesis' },
              { name: 'Verdict', note: 'VERIFIED / REVIEW / REJECT' },
              { name: 'Sheet', note: 'Final Filtered / Rejected Profiles' },
            ]}
            colorClass="text-amber-400" borderActive="border-amber-400/30"
            borderIdle="border-white/[0.05]" textActive="text-amber-400" />
        </div>
      </div>

      <div className="flex justify-center"><div className="w-[1px] h-3 bg-white/10" /></div>

      {/* Routing */}
      <div className="rounded-xl border border-white/[0.06] bg-white/[0.01] p-3">
        <div className="text-[9px] font-bold text-white/30 uppercase tracking-[0.3em] mb-2">Sheet Routing</div>
        <div className="flex flex-col gap-1.5">
          {[
            { s: 'VERIFIED', c: 'text-emerald-400', r: 'Final Filtered List · LinkedIn confirmed + email deliverable' },
            { s: 'REVIEW',   c: 'text-amber-400',   r: 'Final Filtered List · uncertain employment or email' },
            { s: 'REJECT',   c: 'text-red-400',     r: 'Rejected Profiles · title mismatch, left company, or no identity' },
          ].map(v => (
            <div key={v.s} className="flex items-start gap-2">
              <span className={`shrink-0 text-[9px] font-bold w-14 ${v.c}`}>{v.s}</span>
              <span className="text-[9px] text-white/30 leading-relaxed">{v.r}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="flex justify-center"><div className="w-[1px] h-3 bg-white/10" /></div>
      <div className="flex items-center gap-2 px-3">
        <div className="w-2 h-2 rounded-full bg-white/20" />
        <span className="text-[9px] font-mono text-white/25 uppercase tracking-widest">END</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

function VeriContent() {
  const searchParams = useSearchParams();
  const initialThreadId = searchParams.get('threadId');

  const { running, threadId, result, error, rowStart, rowEnd } = useVeriStore();
  const setRunning  = (v: boolean)      => useVeriStore.setState({ running: v });
  const setThreadId = (v: string|null)  => useVeriStore.setState({ threadId: v });
  const setResult   = (v: any)          => useVeriStore.setState({ result: v });
  const setError    = (v: string|null)  => useVeriStore.setState({ error: v });
  const setRowStart = (v: string)       => useVeriStore.setState({ rowStart: v });
  const setRowEnd   = (v: string)       => useVeriStore.setState({ rowEnd: v });
  const didAutoConnect = useRef(false);

  const [contacts, setContacts]     = useState<ContactCard[]>([]);
  const [activity, setActivity]     = useState<ActivityEntry[]>([]);
  const [activePhase, setActivePhase] = useState<ContactPhase | null>(null);
  const [zbWarning, setZbWarning]   = useState<string | null>(null);  // ZeroBounce credit warning

  // Auto-connect via URL param (from "Open Veri →" button)
  useEffect(() => {
    if (initialThreadId && !didAutoConnect.current) {
      didAutoConnect.current = true;
      setThreadId(initialThreadId);
      setRunning(true);
      setResult(null);
      setError(null);
    }
  }, [initialThreadId]);

  // Auto-discover any active Veri run on mount (covers auto-triggered case when
  // user navigates to /veri directly without a threadId in URL)
  useEffect(() => {
    if (initialThreadId || threadId || didAutoConnect.current) return;
    fetch(apiUrl('/api/runs'))
      .then(r => r.json())
      .then((runs: any[]) => {
        const active = runs.find(r => r.agent === 'veri' && r.status === 'running' && r.thread_id);
        if (active && !useVeriStore.getState().threadId) {
          setThreadId(active.thread_id);
          setRunning(true);
          setResult(null);
          setError(null);
          setActivity(prev => [...prev, {
            ts: new Date().toISOString(),
            detail: `Auto-connected to running Veri task (${active.thread_id.slice(0, 8)}…)`,
            level: 'system',
            step: 'log',
          }]);
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (running) {
      setContacts([]);
      setActivity([]);
      setActivePhase(null);
      setZbWarning(null);
    }
  }, [running]);

  const handleEvent = useCallback((msg: any) => {
    if (msg.type === 'system_warning') {
      if (msg.code === 'zb_no_credits') {
        setZbWarning(msg.message);
      }

    } else if (msg.type === 'veri_contact') {
      const { name, company, phase, status, reject_reason, sheet_row, reject_sheet_row, review_flags, email_validated, signals } = msg.data as {
        name: string; company: string; phase: ContactPhase; status: ContactStatus;
        reject_reason?: string; sheet_row?: number; reject_sheet_row?: number; review_flags?: string[];
        email_validated?: boolean; signals?: ContactSignals;
      };
      setContacts(prev => {
        const idx = prev.findIndex(c => c.name === name && c.company === company);
        const patch: Partial<ContactCard> = {
          phase,
          status: status || (prev[idx]?.status ?? ''),
          ...(reject_reason ? { rejectReason: reject_reason } : {}),
          ...(review_flags?.length ? { reviewFlags: review_flags } : {}),
          ...(sheet_row != null ? { sheetRow: sheet_row } : {}),
          ...(reject_sheet_row != null ? { rejectSheetRow: reject_sheet_row } : {}),
          ...(email_validated === false ? { emailValidated: false } : {}),
          ...(signals ? { signals } : {}),
        };
        if (idx === -1) return [...prev, { name, company, ...patch } as ContactCard];
        const updated = [...prev];
        updated[idx] = { ...updated[idx], ...patch };
        return updated;
      });
      if (phase !== 'done' && phase !== 'queued') setActivePhase(phase);

    } else if (msg.type === 'veri_step') {
      const { name, company, phase, step, detail, level } = msg.data;
      setActivity(prev => [...prev, {
        ts: msg.timestamp || new Date().toISOString(),
        name, company, phase, step, detail, level,
      }]);

    } else if (msg.type === 'log') {
      // Show errors + system messages in the activity feed too
      if (msg.level === 'error' || msg.level === 'warning') {
        setActivity(prev => [...prev, {
          ts: msg.timestamp || new Date().toISOString(),
          detail: msg.message,
          level: msg.level === 'error' ? 'error' : 'warning',
          step: 'log',
        }]);
      }

    } else if (msg.type === 'completed') {
      setRunning(false);
      setResult(msg.data);
      setActivePhase(null);
      setActivity(prev => [...prev, {
        ts: msg.timestamp || new Date().toISOString(),
        detail: `batch complete — verified=${msg.data?.verified ?? 0} review=${msg.data?.review ?? 0} rejected=${msg.data?.rejected ?? 0}`,
        level: 'success',
        step: 'log',
      }]);
    } else if (msg.type === 'paused') {
      useVeriStore.setState({ paused: true });
    } else if (msg.type === 'resumed') {
      useVeriStore.setState({ paused: false });
    } else if (msg.type === 'cancelled') {
      setRunning(false);
      useVeriStore.setState({ cancelled: true, paused: false });
      setActivePhase(null);
    } else if (msg.type === 'error') {
      setRunning(false);
      setError(msg.data?.error);
      setActivePhase(null);
      setActivity(prev => [...prev, {
        ts: msg.timestamp || new Date().toISOString(),
        detail: `pipeline error: ${msg.data?.error}`,
        level: 'error',
        step: 'log',
      }]);
    }
  }, []);

  const handleStop = async () => {
    if (!threadId) return;
    try { await fetch(apiUrl(`/api/runs/${threadId}/cancel`), { method: 'POST' }); } catch { /* ignore */ }
  };

  const handlePauseResume = async () => {
    if (!threadId) return;
    const paused = useVeriStore.getState().paused;
    try { await fetch(apiUrl(`/api/runs/${threadId}/${paused ? 'resume' : 'pause'}`), { method: 'POST' }); } catch { /* ignore */ }
  };

  const handleRun = async () => {
    useVeriStore.setState({ cancelled: false });
    setRunning(true); setResult(null); setError(null);
    setContacts([]); setActivity([]); setActivePhase(null);
    const body: any = {};
    if (rowStart) body.row_start = parseInt(rowStart);
    if (rowEnd)   body.row_end   = parseInt(rowEnd);
    try {
      const resp = await fetch(apiUrl('/api/veri/run'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      setThreadId(data.thread_id);
    } catch (e: any) {
      setError(e.message);
      setRunning(false);
    }
  };

  const doneCards   = contacts.filter(c => c.phase === 'done');
  const activeCards = contacts.filter(c => c.phase !== 'done');
  const verifiedN   = doneCards.filter(c => c.status === 'VERIFIED').length;
  const reviewN     = doneCards.filter(c => c.status === 'REVIEW').length;
  const rejectN     = doneCards.filter(c => c.status === 'REJECT').length;

  const paused    = useVeriStore(s => s.paused);
  const cancelled = useVeriStore(s => s.cancelled);
  const [showHelp, setShowHelp] = useState(false);

  return (
    <div className="h-screen overflow-hidden flex flex-col px-6 pt-4 -mb-20 max-w-[1600px] mx-auto font-sans">

      {/* How It Works Modal */}
      {showHelp && <HowItWorksModal onClose={() => setShowHelp(false)} />}

      {/* ZeroBounce credit warning banner */}
      {zbWarning && (
        <div className="flex items-start gap-3 mb-3 px-4 py-3 rounded-xl border border-amber-500/30 bg-amber-500/[0.08] flex-shrink-0">
          <span className="shrink-0 text-amber-400 text-[13px] mt-[1px]">⚠</span>
          <div className="flex-1 min-w-0">
            <div className="text-[10px] font-bold text-amber-300 uppercase tracking-widest mb-0.5">
              Email Validation Disabled — ZeroBounce Out of Credits
            </div>
            <div className="text-[9px] text-amber-300/70 leading-relaxed">
              Email addresses are <span className="font-semibold text-amber-300/90">not being validated</span> this run.
              Contacts are verified by <span className="font-semibold text-amber-300/90">LinkedIn signals only</span>.
              LinkedIn-confirmed contacts will still be marked <span className="font-semibold text-emerald-400/90">VERIFIED</span> — but their emails are unconfirmed.
              Refill credits at <span className="font-mono text-amber-300/80">zerobounce.com</span> before the next run.
            </div>
          </div>
          <button
            onClick={() => setZbWarning(null)}
            className="shrink-0 text-amber-400/40 hover:text-amber-400/80 text-[10px] transition-colors"
            title="Dismiss"
          >✕</button>
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between mb-3 pb-3 border-b border-white/[0.06] flex-shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.4em]">Module 03 · Quality Control</span>
          <span className="h-3 w-[1px] bg-white/10" />
          <h1 className="text-base font-bold text-white tracking-tight">
            Pipeline<span className="text-white/50 font-light"> / </span>Veri
          </h1>
          <button
            onClick={() => setShowHelp(true)}
            title="How Veri works"
            className="flex items-center justify-center w-4 h-4 rounded-full border border-white/20 bg-white/[0.04] text-[9px] font-bold text-white/40 hover:border-white/40 hover:text-white/70 hover:bg-white/[0.08] transition-all leading-none"
          >
            ?
          </button>
        </div>
        <div className="flex items-center gap-3">
          {running && (
            <>
              <div className="flex items-center gap-2">
                <div className="relative flex h-1.5 w-1.5">
                  <span className="animate-ping absolute inset-0 rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative block h-1.5 w-1.5 rounded-full bg-emerald-400" />
                </div>
                <span className="text-[9px] font-bold text-emerald-400/80 uppercase tracking-[0.2em]">
                  Verifying{contacts.length > 0 ? ` · ${contacts.length} contacts` : ''}
                </span>
              </div>
              <button onClick={handlePauseResume}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-lg border text-[9px] font-bold uppercase tracking-widest transition-colors ${
                  paused
                    ? 'border-teal-500/30 bg-teal-500/10 text-teal-400 hover:bg-teal-500/20'
                    : 'border-amber-500/30 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20'
                }`}>
                <span>{paused ? '▶' : '⏸'}</span>
                <span>{paused ? 'Resume' : 'Pause'}</span>
              </button>
              <button onClick={handleStop}
                className="flex items-center gap-1.5 px-3 py-1 rounded-lg border border-red-500/30 bg-red-500/10 text-red-400 text-[9px] font-bold uppercase tracking-widest hover:bg-red-500/20 transition-colors">
                <span>■</span><span>Stop</span>
              </button>
            </>
          )}
          {cancelled && !running && (
            <span className="text-[9px] font-bold text-red-400/70 uppercase tracking-widest">Stopped</span>
          )}
        </div>
      </div>

      {/* Main 2-col grid */}
      <div className="flex-1 min-h-0 overflow-hidden grid grid-cols-1 xl:grid-cols-5 gap-4">

        {/* ── Left: Config + Workflow ── */}
        <div className="xl:col-span-2 flex flex-col gap-3 min-h-0 overflow-y-auto no-scrollbar">

          {/* Config */}
          <div className="border border-white/[0.07] rounded-2xl bg-white/[0.02] p-4 flex flex-col gap-3 flex-shrink-0">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em]">Configuration</div>
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <span className="text-[9px] text-white/55 uppercase tracking-widest">Batch Scope</span>
                <span className="text-[8px] text-emerald-400/50 font-mono">auto-scans pending</span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <div className="text-[8px] text-white/35 uppercase tracking-widest mb-1">Start Row</div>
                  <input
                    className="w-full bg-black/20 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white/90 outline-none focus:border-white/20 transition-colors placeholder-white/20 text-center tabular-nums"
                    placeholder="e.g. 2" type="number" value={rowStart}
                    onChange={e => setRowStart(e.target.value)} disabled={running} />
                </div>
                <div>
                  <div className="text-[8px] text-white/35 uppercase tracking-widest mb-1">End Row</div>
                  <input
                    className="w-full bg-black/20 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white/90 outline-none focus:border-white/20 transition-colors placeholder-white/20 text-center tabular-nums"
                    placeholder="e.g. 50" type="number" value={rowEnd}
                    onChange={e => setRowEnd(e.target.value)} disabled={running} />
                </div>
              </div>
            </div>
            <button onClick={handleRun} disabled={running}
              className="w-full flex items-center justify-center gap-2 px-5 py-2 rounded-xl bg-white text-black text-[10px] font-bold uppercase tracking-[0.12em] hover:bg-white/90 disabled:bg-white/10 disabled:text-white/40 transition-all duration-200">
              {running ? (
                <><div className="w-2.5 h-2.5 border-2 border-black/20 border-t-black rounded-full animate-spin" /><span>Validating…</span></>
              ) : (
                <span>Initialize Verification</span>
              )}
            </button>
          </div>

          {/* Workflow Diagram */}
          <div className="border border-white/[0.07] rounded-2xl bg-white/[0.02] p-4 flex-shrink-0">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em] mb-3">Pipeline Architecture</div>
            <WorkflowDiagram activePhase={activePhase} />
          </div>
        </div>

        {/* ── Right: Contact Board + Activity Feed ── */}
        <div className="xl:col-span-3 flex flex-col gap-3 min-h-0 overflow-hidden">

          {/* Contact Board — compact */}
          <div className="flex-shrink-0 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden">
            <div className="px-4 py-2.5 border-b border-white/[0.05] flex items-center justify-between">
              <div className="flex items-center gap-2.5">
                <div className={`w-1.5 h-1.5 rounded-full ${running ? 'bg-emerald-400 animate-pulse' : 'bg-white/10'}`} />
                <span className="text-[9px] font-bold text-white/40 uppercase tracking-[0.25em]">Contact Board</span>
              </div>
              {contacts.length > 0 && (
                <div className="flex items-center gap-3">
                  {activeCards.length > 0 && <span className="text-[9px] text-white/35"><span className="text-white/60 font-bold">{activeCards.length}</span> active</span>}
                  {verifiedN > 0 && <span className="text-[9px] text-emerald-400/70"><span className="font-bold">{verifiedN}</span> verified</span>}
                  {reviewN > 0   && <span className="text-[9px] text-amber-400/70"><span className="font-bold">{reviewN}</span> review</span>}
                  {rejectN > 0   && <span className="text-[9px] text-red-400/70"><span className="font-bold">{rejectN}</span> reject</span>}
                </div>
              )}
            </div>
            <div className="p-3">
              {contacts.length > 0 ? (
                <div className="grid grid-cols-3 gap-2 max-h-[200px] overflow-y-auto no-scrollbar">
                  {contacts.map((c, i) => (
                    <ContactTile key={`${c.name}-${i}`} card={c} />
                  ))}
                </div>
              ) : (
                <div className="py-6 flex flex-col items-center justify-center text-center">
                  <div className={`w-1.5 h-1.5 rounded-full mb-2 ${running ? 'bg-emerald-400 animate-pulse' : 'bg-white/10'}`} />
                  <div className="text-[9px] text-white/25 uppercase tracking-widest">
                    {running ? 'Loading contacts…' : 'Standby — run to see contacts'}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Activity Feed — takes remaining space */}
          <div className="flex-1 min-h-0 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden flex flex-col">
            <ActivityFeed entries={activity} running={running} />
          </div>

          {/* Results summary */}
          {result && (
            <div className="flex-shrink-0 border border-emerald-400/15 rounded-2xl bg-emerald-400/[0.03] overflow-hidden">
              <div className="px-4 py-2.5 border-b border-emerald-400/10 flex items-center justify-between">
                <div className="flex items-center gap-2.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                  <span className="text-[10px] font-bold text-emerald-400/80 uppercase tracking-[0.25em]">Batch Complete</span>
                </div>
                <span className="text-[9px] font-mono text-white/25 uppercase tracking-tighter">
                  Final Filtered + Rejected Profiles updated
                </span>
              </div>
              <div className="p-4 grid grid-cols-3 gap-3">
                {[
                  { l: 'Verified', v: result.verified, color: 'text-emerald-400', border: 'border-emerald-400/15' },
                  { l: 'Review',   v: result.review,   color: 'text-amber-400',   border: 'border-amber-400/15'   },
                  { l: 'Rejected', v: result.rejected, color: 'text-red-400',     border: 'border-red-400/15'     },
                ].map((s, i) => (
                  <div key={i} className={`text-center p-3 rounded-xl border ${s.border} bg-black/20`}>
                    <div className={`text-2xl font-bold ${s.color} tabular-nums`}>{s.v || 0}</div>
                    <div className="text-[9px] text-white/35 uppercase tracking-widest mt-1">{s.l}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="flex-shrink-0 px-5 py-3 border border-red-400/15 rounded-2xl bg-red-400/[0.04] flex items-center gap-3">
              <span className="text-red-400 text-sm">✗</span>
              <div>
                <div className="text-[10px] font-bold text-red-400/80 uppercase tracking-widest">Verification Error</div>
                <div className="text-xs text-red-400/60 mt-0.5">{error}</div>
              </div>
            </div>
          )}

          {/* Hidden LogStream — provides WS connection + routes events to handleEvent */}
          {threadId && (
            <div className="hidden">
              <LogStream threadId={threadId} onEvent={handleEvent} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function VeriPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen p-10 flex items-center justify-center">
        <span className="text-[10px] font-bold text-white/30 uppercase tracking-[0.3em] animate-pulse">Initializing Verifier…</span>
      </div>
    }>
      <VeriContent />
    </Suspense>
  );
}
