import { useState } from 'react'

export default function ConfirmationModal({ data, threadId, onClose }) {
  const [fields, setFields] = useState({
    normalized_name: data.normalized_name || data.raw_name || '',
    domain: data.domain || '',
    email_format: data.email_format || '',
    sdr_assigned: data.sdr_assigned || '',
    account_type: data.account_type || '',
    account_size: data.account_size || '',
  })
  const [submitting, setSubmitting] = useState(false)

  const post = async (confirmed) => {
    setSubmitting(true)
    try {
      await fetch('/api/fini/confirm', {
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
          {data.linkedin_org_id && (
            <div className="mt-4 bg-blue-500/5 border border-blue-500/15 rounded px-3 py-2.5 space-y-1">
              <div className="text-[10px] font-mono text-blue-500/60 uppercase tracking-wider mb-1.5">LinkedIn Enrichment</div>
              <div className="flex items-center gap-2 text-xs font-mono">
                <span className="text-gray-600">ORG_ID</span>
                <span className="text-blue-300">{data.linkedin_org_id}</span>
              </div>
              {data.sales_nav_url && (
                <div className="flex items-center gap-2 text-xs font-mono">
                  <span className="text-gray-600">SALES_NAV</span>
                  <a href={data.sales_nav_url} target="_blank" rel="noopener noreferrer"
                     className="text-blue-400 hover:text-blue-300 underline underline-offset-2 truncate">
                    {data.sales_nav_url}
                  </a>
                </div>
              )}
            </div>
          )}
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
