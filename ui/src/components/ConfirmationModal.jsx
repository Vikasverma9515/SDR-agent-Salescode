import { useState } from 'react'
import { apiUrl } from '../lib/api'

export default function ConfirmationModal({ data, threadId, onClose }) {
  const [fields, setFields] = useState({
    normalized_name: data.normalized_name || data.raw_name || '',
    domain: data.domain || '',
    email_format: data.email_format || '',
    sdr_assigned: data.sdr_assigned || '',
    account_type: data.account_type || '',
    account_size: data.account_size || '',
    linkedin_org_id: data.linkedin_org_id || '',
    sales_nav_url: data.sales_nav_url || '',
  })
  const [submitting, setSubmitting] = useState(false)

  const post = async (confirmed) => {
    setSubmitting(true)
    try {
      await fetch(apiUrl('/api/fini/confirm'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ thread_id: threadId, confirmed, ...fields }),
      })
      onClose()
    } finally { setSubmitting(false) }
  }

  return (
    <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
      {/* Backdrop blur */}
      <div className="absolute inset-0 backdrop-blur-sm" />

      <div className="relative w-full max-w-md bg-[#080d18] border border-blue-500/20 rounded-lg shadow-2xl overflow-hidden"
           style={{ boxShadow: '0 0 60px rgba(37,99,235,0.15), 0 0 0 1px rgba(37,99,235,0.1)' }}>

        {/* Top accent line */}
        <div className="h-px bg-gradient-to-r from-transparent via-blue-500/60 to-transparent" />

        {/* Header */}
        <div className="px-6 py-4 border-b border-white/[0.05] flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="relative w-2 h-2">
              <span className="absolute inset-0 rounded-full bg-amber-400 ping-slow" />
              <span className="relative block w-2 h-2 rounded-full bg-amber-400" />
            </div>
            <div>
              <div className="text-sm font-semibold text-white">Review Required</div>
              <div className="text-[10px] font-mono text-gray-600">
                COMPANY {data.company_index + 1} OF {data.total_companies}
              </div>
            </div>
          </div>
          <div className="text-xs font-mono text-gray-700 border border-white/5 px-2 py-1 rounded">
            FINI · TARGET BUILDER
          </div>
        </div>

        {/* Raw input */}
        <div className="px-6 pt-5">
          <div className="bg-white/[0.03] border border-white/[0.06] rounded px-3 py-2 mb-5 flex items-center gap-3">
            <span className="text-[10px] font-mono text-gray-600 uppercase tracking-wider">INPUT</span>
            <span className="text-sm font-mono text-gray-300">{data.raw_name}</span>
          </div>

          {/* Fields */}
          <div className="space-y-3">
            {[
              { key: 'normalized_name', label: 'COMPANY NAME',  ph: 'Nestlé India Limited' },
              { key: 'domain',          label: 'DOMAIN',        ph: 'nestle.in' },
              { key: 'email_format',    label: 'EMAIL FORMAT',  ph: '{first}.{last}@nestle.in' },
              { key: 'sdr_assigned',    label: 'SDR',           ph: 'Gopal' },
            ].map(({ key, label, ph }) => (
              <div key={key}>
                <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">{label}</label>
                <input
                  className="input-field"
                  value={fields[key]}
                  onChange={e => setFields(p => ({ ...p, [key]: e.target.value }))}
                  placeholder={ph}
                />
              </div>
            ))}

            {/* Account type + size side by side */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
                  ACCOUNT TYPE <span className="text-gray-700 normal-case">(region)</span>
                </label>
                <input
                  className="input-field"
                  value={fields.account_type}
                  onChange={e => setFields(p => ({ ...p, account_type: e.target.value }))}
                  placeholder="India"
                />
              </div>
              <div>
                <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">
                  ACCOUNT SIZE
                </label>
                <select
                  className="input-field"
                  value={fields.account_size}
                  onChange={e => setFields(p => ({ ...p, account_size: e.target.value }))}
                >
                  <option value="">— pick —</option>
                  <option value="Large">Large</option>
                  <option value="Medium">Medium</option>
                  <option value="Small">Small</option>
                </select>
              </div>
            </div>
          </div>

          {/* LinkedIn data */}
          <div className="mt-4 bg-blue-500/5 border border-blue-500/15 rounded px-3 py-2.5 space-y-3">
            <div className="text-[10px] font-mono text-blue-500/60 uppercase tracking-wider">LinkedIn Enrichment</div>
            <div>
              <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">ORG ID</label>
              <input
                className="input-field"
                value={fields.linkedin_org_id}
                onChange={e => setFields(p => ({ ...p, linkedin_org_id: e.target.value }))}
                placeholder="16271"
              />
            </div>
            <div>
              <label className="block text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1.5">SALES NAV URL</label>
              <input
                className="input-field"
                value={fields.sales_nav_url}
                onChange={e => setFields(p => ({ ...p, sales_nav_url: e.target.value }))}
                placeholder="https://www.linkedin.com/sales/search/people/..."
              />
              {(!fields.sales_nav_url || fields.sales_nav_url.includes('keywords%3A') || !fields.linkedin_org_id) && (
                <div className="mt-1.5 flex items-center gap-1.5 text-amber-400">
                  <svg className="w-3 h-3 shrink-0" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.168 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 6a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 6zm0 9a1 1 0 100-2 1 1 0 000 2z" clipRule="evenodd" />
                  </svg>
                  <span className="text-[10px] font-mono">
                    {!fields.sales_nav_url ? 'No link found — please paste manually' :
                     fields.sales_nav_url.includes('keywords%3A') ? 'Auto-generated keyword link — paste exact link if available' :
                     !fields.linkedin_org_id ? 'No org ID — link may be imprecise' : ''}
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="px-6 py-5 flex gap-3 mt-2">
          <button onClick={() => post(false)} disabled={submitting} className="btn-secondary flex-1 justify-center">
            Skip
          </button>
          <button onClick={() => post(true)} disabled={submitting} className="btn-primary flex-1 justify-center">
            {submitting ? (
              <span className="flex items-center gap-2">
                <svg className="animate-spin w-3.5 h-3.5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
                </svg>
                Writing...
              </span>
            ) : '→ Confirm & Write'}
          </button>
        </div>

        {/* Bottom accent */}
        <div className="h-px bg-gradient-to-r from-transparent via-white/[0.04] to-transparent" />
      </div>
    </div>
  )
}
