'use client';

import { useState, useEffect } from 'react';
import { apiUrl } from '@/lib/api';

const SERVICE_META: Record<string, { label: string; group: string; hint: string }> = {
  google_sheets:      { label: 'Google Sheets',    group: 'Data Layer',          hint: 'SPREADSHEET_ID + GOOGLE_SERVICE_ACCOUNT_JSON' },
  openai:             { label: 'OpenAI',            group: 'AI / LLM',            hint: 'OPENAI_API_KEY — used by all 3 modules' },
  tavily:             { label: 'Tavily',            group: 'AI / LLM',            hint: 'TAVILY_API_KEY — web search in Searcher & Veri' },
  perplexity:         { label: 'Perplexity',        group: 'AI / LLM',            hint: 'PERPLEXITY_API_KEY — AI Scout + Veri research' },
  unipile:            { label: 'Unipile (LinkedIn)', group: 'Data Sources',       hint: 'UNIPILE_API_KEY + UNIPILE_DSN + UNIPILE_ACCOUNT_ID' },
  zerobounce:         { label: 'ZeroBounce',        group: 'Data Sources',        hint: 'ZEROBOUNCE_API_KEY — email validation in Veri' },
  n8n:                { label: 'n8n Webhook',        group: 'Automation',         hint: 'N8N_WEBHOOK_URL — optional relay webhook' },
};

const ENV_GROUPS = [
  {
    group: 'Data Layer',
    env: '.env file on the host machine',
    fields: [
      { key: 'SPREADSHEET_ID', label: 'Google Sheet ID', desc: 'The ID from your Google Sheets URL', required: true },
      { key: 'GOOGLE_SERVICE_ACCOUNT_JSON', label: 'Service Account JSON path', desc: 'Path to credentials/google-service-account.json', required: true },
    ],
  },
  {
    group: 'AI / LLM',
    env: '.env',
    fields: [
      { key: 'OPENAI_API_KEY', label: 'OpenAI API Key', desc: 'Powers Fini, Searcher, and Veri agents', required: true },
      { key: 'TAVILY_API_KEY', label: 'Tavily API Key', desc: 'Web search for contact discovery', required: true },
      { key: 'PERPLEXITY_API_KEY', label: 'Perplexity API Key', desc: 'AI Scout real-time web research', required: true },
    ],
  },
  {
    group: 'LinkedIn (Unipile)',
    env: '.env',
    fields: [
      { key: 'UNIPILE_API_KEY', label: 'Unipile API Key', desc: 'Your Unipile platform key', required: true },
      { key: 'UNIPILE_DSN', label: 'Unipile DSN', desc: 'Data Source Node — from Unipile dashboard', required: true },
      { key: 'UNIPILE_ACCOUNT_ID', label: 'Unipile Account ID', desc: 'Your LinkedIn account instance', required: true },
    ],
  },
  {
    group: 'Email Validation',
    env: '.env',
    fields: [
      { key: 'ZEROBOUNCE_API_KEY', label: 'ZeroBounce API Key', desc: 'Email verification credits', required: true },
    ],
  },
  {
    group: 'Automation (Optional)',
    env: '.env',
    fields: [
      { key: 'N8N_WEBHOOK_URL', label: 'n8n Webhook URL', desc: 'Relay webhook — triggers after Fini enrichment', required: false },
    ],
  },
];

const PIPELINE_DEFAULTS = [
  { key: 'Target contact count', value: '15 per company', desc: 'How many contacts Searcher aims to find per company' },
  { key: 'Role selection timeout', value: '120 seconds', desc: 'Auto-selects default departments if SDR doesn\'t respond' },
  { key: 'Contact selection timeout', value: '5 minutes', desc: 'Auto-proceeds with matched contacts on timeout' },
  { key: 'Veri auto-trigger', value: 'Enabled', desc: 'Veri runs automatically after Searcher writes contacts' },
  { key: 'AI Scout model', value: 'Claude Sonnet (Bedrock)', desc: 'Synthesis + structuring for prospect research' },
  { key: 'AI Scout web model', value: 'Perplexity sonar-pro', desc: 'Real-time web research for AI Scout' },
];

export default function SettingsPage() {
  const [config, setConfig] = useState<Record<string, boolean> | null>(null);
  const [credits, setCredits] = useState<any>(null);
  const [activeTab, setActiveTab] = useState<'status' | 'config' | 'pipeline'>('status');
  const [copied, setCopied] = useState<string | null>(null);

  useEffect(() => {
    fetch(apiUrl('/api/config/check')).then(r => r.json()).then(setConfig).catch(() => {});
    fetch(apiUrl('/api/zerobounce/credits')).then(r => r.json()).then(setCredits).catch(() => {});
  }, []);

  const copyKey = (key: string) => {
    navigator.clipboard.writeText(key).then(() => {
      setCopied(key);
      setTimeout(() => setCopied(null), 1500);
    });
  };

  const missing = config ? Object.entries(config).filter(([k, v]) => !v && k !== 'chrome_cdp').map(([k]) => k) : [];
  const online = config ? Object.entries(config).filter(([k, v]) => v && k !== 'chrome_cdp').length : 0;
  const total = config ? Object.keys(config).filter(k => k !== 'chrome_cdp').length : 0;

  return (
    <div className="p-6 xl:p-8 max-w-[1200px] mx-auto font-sans">

      {/* ── Header ── */}
      <div className="flex items-start justify-between mb-6 pb-5 border-b border-white/[0.06]">
        <div>
          <div className="text-[9px] font-bold text-white/35 uppercase tracking-[0.45em] mb-2">System Configuration</div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Settings</h1>
          <p className="text-white/40 mt-1.5 text-sm max-w-md leading-relaxed">
            API keys and pipeline configuration. All keys are stored in the <code className="font-mono text-white/60 bg-white/[0.06] px-1.5 py-0.5 rounded text-xs">.env</code> file on the host machine.
          </p>
        </div>
        {config && (
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="text-xl font-bold text-white tabular-nums font-mono">{online}<span className="text-white/30 text-sm font-light">/{total}</span></div>
              <div className="text-[9px] text-white/35 uppercase tracking-widest">services online</div>
            </div>
            <div className={`w-10 h-10 rounded-xl border flex items-center justify-center ${online === total ? 'border-emerald-400/20 bg-emerald-400/[0.06]' : 'border-amber-400/20 bg-amber-400/[0.06]'}`}>
              <div className={`w-2.5 h-2.5 rounded-full ${online === total ? 'bg-emerald-400' : 'bg-amber-400 animate-pulse'}`} />
            </div>
          </div>
        )}
      </div>

      {/* ── Tabs ── */}
      <div className="flex gap-1 mb-6 p-1 bg-white/[0.03] border border-white/[0.06] rounded-xl w-fit">
        {(['status', 'config', 'pipeline'] as const).map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-[0.2em] transition-all ${
              activeTab === tab
                ? 'bg-white text-black'
                : 'text-white/40 hover:text-white/70'
            }`}
          >
            {tab === 'status' ? 'Service Status' : tab === 'config' ? 'API Keys' : 'Pipeline Defaults'}
          </button>
        ))}
      </div>

      {/* ── Service Status ── */}
      {activeTab === 'status' && (
        <div className="space-y-4">
          {/* ZeroBounce credits — top of status */}
          {credits?.credits != null && (
            <div className={`flex items-center gap-5 px-6 py-4 rounded-xl border ${
              credits.credits > 100 ? 'border-emerald-400/15 bg-emerald-400/[0.03]'
              : credits.credits > 50 ? 'border-amber-400/15 bg-amber-400/[0.03]'
              : 'border-red-400/20 bg-red-400/[0.04]'
            }`}>
              <div>
                <div className="text-[9px] font-bold text-white/35 uppercase tracking-[0.3em] mb-0.5">ZeroBounce Credits</div>
                <div className={`text-2xl font-bold font-mono tabular-nums ${
                  credits.credits > 100 ? 'text-emerald-400' : credits.credits > 50 ? 'text-amber-400' : 'text-red-400'
                }`}>
                  {credits.credits.toLocaleString()}
                </div>
              </div>
              <div className="flex-1 text-[11px] text-white/40 leading-relaxed">
                {credits.credits < 50
                  ? '⚠️ Low credits — replenish at zerobounce.net before running Veri.'
                  : credits.credits < 200
                  ? 'Sufficient for current workload. Monitor before large Veri runs.'
                  : 'Healthy credit balance. Each email verification uses 1 credit.'}
              </div>
            </div>
          )}

          {/* Service cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {config ? (
              Object.entries(config).filter(([k]) => k !== 'chrome_cdp').map(([key, ok]) => {
                const meta = SERVICE_META[key] || { label: key, group: '', hint: key.toUpperCase() };
                return (
                  <div
                    key={key}
                    className={`flex items-start gap-4 px-4 py-4 rounded-xl border transition-all ${
                      ok ? 'border-emerald-400/10 bg-emerald-400/[0.02]' : 'border-red-400/15 bg-red-400/[0.03]'
                    }`}
                  >
                    <div className={`w-8 h-8 rounded-lg border flex items-center justify-center shrink-0 mt-0.5 ${
                      ok ? 'border-emerald-400/15 bg-emerald-400/[0.06]' : 'border-red-400/20 bg-red-400/[0.06]'
                    }`}>
                      <div className={`w-2 h-2 rounded-full ${ok ? 'bg-emerald-400' : 'bg-red-400 animate-pulse'}`} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-0.5">
                        <span className="text-sm font-bold text-white/85">{meta.label}</span>
                        <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded uppercase tracking-widest ${
                          ok ? 'bg-emerald-400/10 text-emerald-400' : 'bg-red-400/10 text-red-400'
                        }`}>{ok ? 'Online' : 'Offline'}</span>
                      </div>
                      <div className="text-[9px] text-white/30 uppercase tracking-wider mb-1">{meta.group}</div>
                      <div className="text-[10px] font-mono text-white/25">{meta.hint}</div>
                    </div>
                  </div>
                );
              })
            ) : (
              <div className="col-span-2 py-12 text-center">
                <div className="text-[10px] text-white/25 uppercase tracking-widest animate-pulse">Checking services…</div>
              </div>
            )}
          </div>

          {missing.length > 0 && (
            <div className="px-5 py-4 rounded-xl border border-amber-400/15 bg-amber-400/[0.03]">
              <div className="text-[9px] font-bold text-amber-400/70 uppercase tracking-[0.3em] mb-2">Action Required</div>
              <p className="text-sm text-white/55">
                Add the missing keys to your <code className="font-mono text-white/70 bg-white/[0.06] px-1 rounded">`.env`</code> file, then restart the server with <code className="font-mono text-white/70 bg-white/[0.06] px-1 rounded">bash start.sh</code>.
              </p>
              <div className="mt-2 flex flex-wrap gap-1.5">
                {missing.map(k => (
                  <span key={k} className="text-[9px] font-mono bg-red-400/10 text-red-400/80 px-2 py-0.5 rounded border border-red-400/10">
                    {SERVICE_META[k]?.hint?.split(' ')[0] || k.toUpperCase()}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── API Keys Reference ── */}
      {activeTab === 'config' && (
        <div className="space-y-4">
          <div className="px-4 py-3 rounded-xl border border-blue-400/10 bg-blue-400/[0.03] flex items-start gap-3">
            <span className="text-blue-400 text-sm shrink-0">ℹ</span>
            <p className="text-[11px] text-white/50 leading-relaxed">
              Keys are stored in the <code className="font-mono text-white/70">.env</code> file in the project root. Edit it directly with any text editor. After saving, restart the server with <code className="font-mono text-white/70">bash start.sh</code> to apply changes.
            </p>
          </div>

          {ENV_GROUPS.map((group) => (
            <div key={group.group} className="border border-white/[0.07] rounded-xl overflow-hidden">
              <div className="px-4 py-3 border-b border-white/[0.05] bg-white/[0.01] flex items-center justify-between">
                <span className="text-[10px] font-bold text-white/60 uppercase tracking-[0.3em]">{group.group}</span>
              </div>
              <div className="divide-y divide-white/[0.04]">
                {group.fields.map((field) => (
                  <div key={field.key} className="flex items-center gap-4 px-4 py-3.5">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-0.5">
                        <span className="text-[11px] font-bold text-white/75">{field.label}</span>
                        {!field.required && (
                          <span className="text-[8px] text-white/25 uppercase tracking-wider">optional</span>
                        )}
                      </div>
                      <p className="text-[10px] text-white/35">{field.desc}</p>
                    </div>
                    <button
                      onClick={() => copyKey(field.key)}
                      className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-white/[0.04] border border-white/[0.06] hover:bg-white/[0.07] transition-colors group shrink-0"
                    >
                      <code className="text-[9px] font-mono text-blue-400/70 group-hover:text-blue-400 transition-colors">
                        {field.key}
                      </code>
                      <span className="text-[9px] text-white/20 group-hover:text-white/40 transition-colors">
                        {copied === field.key ? '✓' : '⎘'}
                      </span>
                    </button>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* ── Pipeline Defaults ── */}
      {activeTab === 'pipeline' && (
        <div className="space-y-4">
          <div className="px-4 py-3 rounded-xl border border-white/[0.06] bg-white/[0.02] flex items-start gap-3">
            <span className="text-white/30 text-sm shrink-0">⚙</span>
            <p className="text-[11px] text-white/45 leading-relaxed">
              These are the current pipeline defaults. Timeouts and limits can be adjusted in the backend source code (
              <code className="font-mono text-white/60">backend/agents/searcher.py</code>,{' '}
              <code className="font-mono text-white/60">backend/api.py</code>).
            </p>
          </div>

          <div className="border border-white/[0.07] rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.3em]">Searcher Module</span>
            </div>
            <div className="divide-y divide-white/[0.04]">
              {PIPELINE_DEFAULTS.slice(0, 4).map((d) => (
                <div key={d.key} className="flex items-center gap-4 px-4 py-3.5">
                  <div className="flex-1 min-w-0">
                    <div className="text-[11px] font-bold text-white/75 mb-0.5">{d.key}</div>
                    <div className="text-[10px] text-white/35">{d.desc}</div>
                  </div>
                  <div className="shrink-0">
                    <span className="text-[10px] font-mono font-bold text-white/60 bg-white/[0.05] border border-white/[0.07] px-2.5 py-1 rounded-lg">
                      {d.value}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="border border-white/[0.07] rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.3em]">AI Scout</span>
            </div>
            <div className="divide-y divide-white/[0.04]">
              {PIPELINE_DEFAULTS.slice(4).map((d) => (
                <div key={d.key} className="flex items-center gap-4 px-4 py-3.5">
                  <div className="flex-1 min-w-0">
                    <div className="text-[11px] font-bold text-white/75 mb-0.5">{d.key}</div>
                    <div className="text-[10px] text-white/35">{d.desc}</div>
                  </div>
                  <div className="shrink-0">
                    <span className="text-[10px] font-mono font-bold text-white/60 bg-white/[0.05] border border-white/[0.07] px-2.5 py-1 rounded-lg">
                      {d.value}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Module quick reference */}
          <div className="border border-white/[0.07] rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-white/[0.05] bg-white/[0.01]">
              <span className="text-[10px] font-bold text-white/50 uppercase tracking-[0.3em]">Google Sheets Structure</span>
            </div>
            <div className="divide-y divide-white/[0.04]">
              {[
                { sheet: 'Target Accounts', module: 'Fini output', cols: 'Company, Domain, Org ID, Email format, Status' },
                { sheet: 'First Clean List', module: 'Searcher + Scout output', cols: 'A–N: Company, Domain, First/Last name, Title, Buying role, LinkedIn, Email' },
                { sheet: 'First Clean List', module: 'Veri writes to', cols: 'O–U: Verified flag, ZeroBounce score, LinkedIn check, Tenure, Final status' },
              ].map((row, i) => (
                <div key={i} className="flex items-start gap-4 px-4 py-3.5">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="text-[11px] font-bold text-white/75">{row.sheet}</span>
                      <span className="text-[9px] text-white/30 uppercase tracking-wider">— {row.module}</span>
                    </div>
                    <div className="text-[10px] font-mono text-white/30">{row.cols}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
