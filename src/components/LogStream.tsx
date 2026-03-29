'use client';

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { wsUrl } from '@/lib/api';

interface Log {
  level: string;
  message: string;
  ts: string;
}

interface LogStreamProps {
  threadId: string | null;
  onEvent?: (event: any) => void;
}

const COLORS: Record<string, string> = {
  info: 'text-blue-400', 
  warning: 'text-amber-400', 
  error: 'text-red-400',
  success: 'text-emerald-400', 
  debug: 'text-gray-600',
  system: 'text-gray-600', 
  done: 'text-emerald-400', 
  pause: 'text-amber-400',
};

const PREFIXES: Record<string, string> = {
  info: 'INF', 
  warning: 'WRN', 
  error: 'ERR', 
  success: 'OK ',
  debug: 'DBG', 
  system: 'SYS', 
  done: 'DONE', 
  pause: 'WAIT',
};

export default function LogStream({ threadId, onEvent }: LogStreamProps) {
  const [logs, setLogs] = useState<Log[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const doneRef = useRef(false);
  const onEventRef = useRef(onEvent);
  
  onEventRef.current = onEvent;

  const connect = useCallback(() => {
    if (!threadId || doneRef.current) return;
    if (wsRef.current && wsRef.current.readyState < 2) return; // already open/connecting

    const ws = new WebSocket(wsUrl(threadId));
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      setLogs(p => p.length === 0 ? [{ level: 'system', message: 'pipeline connected', ts: new Date().toISOString() }] : p);
    };

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.type === 'heartbeat') return;
      if (msg.type === 'log') {
        setLogs(p => [...p, { level: msg.level, message: msg.message, ts: msg.timestamp }]);
        if (onEventRef.current) onEventRef.current(msg);
      } else if (msg.type === 'company_progress') {
        // Pass to parent silently — don't pollute the terminal log
        if (onEventRef.current) onEventRef.current(msg);
      } else {
        const label = msg.type === 'completed' ? 'done' : msg.type === 'error' ? 'error' : 'pause';
        let text: string;
        if (msg.type === 'completed') {
          text = 'pipeline step complete';
        } else if (msg.type === 'error') {
          text = msg.data?.error;
        } else if (msg.type === 'role_selection_required') {
          const d = msg.data || {};
          text = `${d.company || 'Company'} — pick role departments (${d.total_found || 0} people found across ${(d.buckets || []).length} functions)`;
        } else if (msg.type === 'contact_selection_required') {
          const d = msg.data || {};
          text = `${d.company || 'Company'} — review ${d.total || 0} contacts (${d.matched_count || 0} matched + ${d.bonus_count || 0} bonus)`;
        } else {
          text = 'awaiting confirmation';
        }
        setLogs(p => [...p, { level: label, message: text, ts: msg.timestamp }]);
        if (onEventRef.current) onEventRef.current(msg);
        if (msg.type === 'completed' || msg.type === 'error') {
          doneRef.current = true;
        }
      }
    };

    ws.onerror = () => {
      setLogs(p => [...p, { level: 'error', message: 'connection error', ts: new Date().toISOString() }]);
    };

    ws.onclose = () => {
      setConnected(false);
      if (!doneRef.current) {
        setTimeout(connect, 2000);
      }
    };
  }, [threadId]);

  useEffect(() => {
    doneRef.current = false;
    setLogs([]);
    if (threadId) connect();
    return () => {
      doneRef.current = true;
      wsRef.current?.close();
    };
  }, [threadId, connect]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs]);

  return (
    <div className="panel overflow-hidden flex flex-col h-full">
      <div className="panel-header px-5 py-4 border-b border-white/10 flex items-center justify-between bg-white/[0.02]">
        <div className="flex items-center gap-2.5">
          <div className="relative w-2 h-2">
            {connected && <span className="absolute inset-0 rounded-full bg-emerald-400 ping-slow" />}
            <span className={`relative block w-2 h-2 rounded-full ${connected ? 'bg-emerald-400' : 'bg-gray-700'}`} />
          </div>
          <span className="text-xs font-mono text-gray-500 tracking-wider uppercase">
            {connected ? 'live' : 'reconnecting...'} · pipeline log
          </span>
        </div>
        <button onClick={() => setLogs([])} className="text-[10px] font-mono text-gray-700 hover:text-gray-400 tracking-wider uppercase px-2 py-1 hover:bg-white/5 rounded">
          clear
        </button>
      </div>

      <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto p-4 space-y-0.5 font-mono text-[11px] bg-black/20 no-scrollbar">
        {logs.length === 0 && (
          <span className="text-gray-700 animate-pulse">waiting for pipeline connection...</span>
        )}
        {logs.map((log, i) => (
          <div key={i} className="flex items-baseline gap-3">
            <span className="text-gray-700 tabular-nums shrink-0">
              {new Date(log.ts).toLocaleTimeString('en-US', { hour12: false })}
            </span>
            <span className={`shrink-0 w-8 ${COLORS[log.level] || 'text-gray-500'} opacity-60 font-bold`}>
              {PREFIXES[log.level] || '---'}
            </span>
            <span className={COLORS[log.level] || 'text-gray-400'}>{log.message}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
