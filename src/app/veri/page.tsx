'use client';

import React, { useState, useCallback, useEffect, useRef, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { apiUrl } from '@/lib/api';
import LogStream from '@/components/LogStream';

const VALIDATION_STEPS = [
  { step: '01', label: 'Identity Sweep',   op: 'DDG + TheOrg'         },
  { step: '02', label: 'Org Chart Logic',  op: 'TheOrg / Tavily'      },
  { step: '03', label: 'Conflict Resolv',  op: 'Perplexity AI'        },
  { step: '04', label: 'Brave Audit',      op: 'LI Profile Fetch'     },
  { step: '05', label: 'Tenure Check',     op: 'LI Employment Path'   },
  { step: '06', label: 'Email Probe',      op: 'ZeroBounce API'       },
];

function VeriContent() {
  const searchParams = useSearchParams();
  const initialThreadId = searchParams.get('threadId');

  const [running, setRunning]   = useState(false);
  const [threadId, setThreadId] = useState<string | null>(null);
  const [result, setResult]     = useState<any>(null);
  const [error, setError]       = useState<string | null>(null);
  const [rowStart, setRowStart] = useState('');
  const [rowEnd, setRowEnd]     = useState('');
  const didAutoConnect          = useRef(false);

  useEffect(() => {
    if (initialThreadId && !didAutoConnect.current) {
      didAutoConnect.current = true;
      setThreadId(initialThreadId);
      setRunning(true);
      setResult(null);
      setError(null);
    }
  }, [initialThreadId]);

  const handleEvent = useCallback((msg: any) => {
    if (msg.type === 'completed') {
      setRunning(false);
      setResult(msg.data);
    } else if (msg.type === 'error') {
      setRunning(false);
      setError(msg.data.error);
    }
  }, []);

  const handleRun = async () => {
    setRunning(true); setResult(null); setError(null);
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

  return (
    <div className="h-screen overflow-hidden flex flex-col px-6 pt-4 -mb-20 max-w-[1600px] mx-auto font-sans">

      {/* ── Header — compact single row ── */}
      <div className="flex items-center justify-between mb-3 pb-3 border-b border-white/[0.06] flex-shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-[9px] font-bold text-white/35 uppercase tracking-[0.4em]">Module 03 · Quality Control</span>
          <span className="h-3 w-[1px] bg-white/10" />
          <h1 className="text-base font-bold text-white tracking-tight">
            Pipeline<span className="text-white/50 font-light"> / </span>Veri
          </h1>
        </div>
        {running && (
          <div className="flex items-center gap-2">
            <div className="relative flex h-1.5 w-1.5">
              <span className="animate-ping absolute inset-0 rounded-full bg-emerald-400 opacity-75" />
              <span className="relative block h-1.5 w-1.5 rounded-full bg-emerald-400" />
            </div>
            <span className="text-[9px] font-bold text-emerald-400/80 uppercase tracking-[0.2em]">Verifying</span>
          </div>
        )}
      </div>

      {/* ── Main 2-col grid — fills remaining height ── */}
      <div className="flex-1 min-h-0 overflow-hidden grid grid-cols-1 xl:grid-cols-5 gap-4">

        {/* Left: Configuration */}
        <div className="xl:col-span-2 flex flex-col gap-3 min-h-0 overflow-hidden">

          {/* Config panel — compact, fixed height */}
          <div className="border border-white/[0.07] rounded-2xl bg-white/[0.02] p-4 flex flex-col gap-3 flex-shrink-0">
            <div className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em]">Configuration</div>

            {/* Batch scope */}
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
                    placeholder="e.g. 2"
                    type="number"
                    value={rowStart}
                    onChange={e => setRowStart(e.target.value)}
                    disabled={running}
                  />
                </div>
                <div>
                  <div className="text-[8px] text-white/35 uppercase tracking-widest mb-1">End Row</div>
                  <input
                    className="w-full bg-black/20 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white/90 outline-none focus:border-white/20 transition-colors placeholder-white/20 text-center tabular-nums"
                    placeholder="e.g. 50"
                    type="number"
                    value={rowEnd}
                    onChange={e => setRowEnd(e.target.value)}
                    disabled={running}
                  />
                </div>
              </div>
            </div>

            {/* Run button */}
            <button
              onClick={handleRun}
              disabled={running}
              className="w-full flex items-center justify-center gap-2 px-5 py-2 rounded-xl bg-white text-black text-[10px] font-bold uppercase tracking-[0.12em] hover:bg-white/90 disabled:bg-white/10 disabled:text-white/40 transition-all duration-200"
            >
              {running ? (
                <>
                  <div className="w-2.5 h-2.5 border-2 border-black/20 border-t-black rounded-full animate-spin" />
                  <span>Validating…</span>
                </>
              ) : (
                <span>Initialize Verification</span>
              )}
            </button>

            {/* Validation steps — compact list */}
            <div className="border border-white/[0.05] rounded-xl overflow-hidden">
              <div className="px-3 py-2 border-b border-white/[0.04]">
                <span className="text-[8px] font-bold text-white/30 uppercase tracking-[0.3em]">Validation Stack</span>
              </div>
              <div className="divide-y divide-white/[0.03]">
                {VALIDATION_STEPS.map((s, i) => (
                  <div key={i} className="flex items-center justify-between px-3 py-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-[9px] font-mono text-white/20">{s.step}</span>
                      <span className="text-[10px] text-white/60">{s.label}</span>
                    </div>
                    <span className="text-[8px] font-mono text-white/25 uppercase tracking-tight">{s.op}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Log panel — fills remaining left-column height when active */}
          {threadId && (
            <div className="flex-1 min-h-0 border border-white/[0.06] rounded-2xl bg-black/20 overflow-hidden flex flex-col">
              <LogStream threadId={threadId} onEvent={handleEvent} />
            </div>
          )}
        </div>

        {/* Right: Terminal + Results — fills full height */}
        <div className="xl:col-span-3 flex flex-col gap-3 min-h-0 overflow-hidden">

          {/* Verification terminal */}
          <div className="flex-1 min-h-0 border border-white/[0.07] rounded-2xl bg-white/[0.02] overflow-hidden flex flex-col">
            <div className="px-5 py-3 border-b border-white/[0.05] flex items-center justify-between flex-shrink-0">
              <div className="flex items-center gap-2.5">
                <div className="flex gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-white/10" />
                  <div className="w-2 h-2 rounded-full bg-white/10" />
                  <div className="w-2 h-2 rounded-full bg-white/10" />
                </div>
                <span className="h-3 w-[1px] bg-white/10" />
                <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.25em]">Verifier Terminal</span>
              </div>
              {result && (
                <span className="text-[9px] font-bold text-emerald-400/70 uppercase tracking-widest">
                  {(result.verified || 0) + (result.review || 0) + (result.rejected || 0)} processed
                </span>
              )}
            </div>

            <div className="flex-1 min-h-0 overflow-hidden">
              {threadId ? (
                <LogStream threadId={threadId} onEvent={handleEvent} />
              ) : (
                <div className="h-full flex flex-col items-center justify-center p-10 text-center">
                  <div className="w-10 h-10 rounded-full border border-white/[0.06] flex items-center justify-center mb-4">
                    <div className="w-1.5 h-1.5 rounded-full bg-white/15 animate-pulse" />
                  </div>
                  <div className="text-[10px] font-bold text-white/30 uppercase tracking-widest mb-1">Verifier Standby</div>
                  <div className="text-[11px] text-white/20 max-w-xs leading-relaxed">
                    Specify a row range or initialize with defaults to begin quality control.
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Results summary */}
          {result && (
            <div className="flex-shrink-0 border border-emerald-400/15 rounded-2xl bg-emerald-400/[0.03] overflow-hidden">
              <div className="px-5 py-3 border-b border-emerald-400/10 flex items-center justify-between">
                <div className="flex items-center gap-2.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                  <span className="text-[10px] font-bold text-emerald-400/80 uppercase tracking-[0.25em]">Batch Results</span>
                </div>
                <span className="text-[9px] font-mono text-white/30 uppercase tracking-tighter">Columns O–U Written</span>
              </div>
              <div className="p-4 grid grid-cols-3 gap-3">
                {[
                  { l: 'Validated',      v: result.verified,  color: 'text-emerald-400', border: 'border-emerald-400/15' },
                  { l: 'Review',         v: result.review,    color: 'text-amber-400',   border: 'border-amber-400/15'   },
                  { l: 'Decommissioned', v: result.rejected,  color: 'text-red-400',     border: 'border-red-400/15'     },
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
