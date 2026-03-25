import { useState, useEffect } from 'react'
import { apiUrl } from '../lib/api'

const SETTINGS_FIELDS = [
  {
    group: 'Google Sheets',
    fields: [
      { key: 'SPREADSHEET_ID', label: 'Spreadsheet ID', placeholder: 'From Google Sheets URL', required: true },
      { key: 'GOOGLE_SERVICE_ACCOUNT_JSON', label: 'Service Account JSON Path', placeholder: 'credentials/service-account.json', required: true },
    ]
  },
  {
    group: 'LinkedIn (Unipile)',
    fields: [
      { key: 'UNIPILE_API_KEY', label: 'Unipile API Key', placeholder: 'Your Unipile key', required: true },
      { key: 'UNIPILE_DSN', label: 'Unipile DSN', placeholder: 'api21.unipile.com:15157', required: true },
      { key: 'UNIPILE_ACCOUNT_ID', label: 'Unipile Account ID', placeholder: 'LinkedIn account ID', required: true },
    ]
  },
  {
    group: 'Search APIs',
    fields: [
      { key: 'OPENAI_API_KEY', label: 'OpenAI API Key', placeholder: 'sk-proj-...', required: true },
      { key: 'TAVILY_API_KEY', label: 'Tavily API Key', placeholder: 'tvly-...', required: true },
      { key: 'PERPLEXITY_API_KEY', label: 'Perplexity API Key', placeholder: 'pplx-...', required: true },
    ]
  },
  {
    group: 'Email Validation',
    fields: [
      { key: 'ZEROBOUNCE_API_KEY', label: 'ZeroBounce API Key', placeholder: 'Your ZeroBounce key', required: true },
    ]
  },
  {
    group: 'n8n Webhook',
    fields: [
      { key: 'N8N_WEBHOOK_URL', label: 'n8n Webhook URL', placeholder: 'https://your-n8n.com/webhook/...', required: false },
    ]
  },
]

export default function SettingsPage() {
  const [config, setConfig] = useState(null)
  const [credits, setCredits] = useState(null)

  useEffect(() => {
    fetch(apiUrl('/api/config/check')).then(r => r.json()).then(setConfig).catch(() => {})
    fetch(apiUrl('/api/zerobounce/credits')).then(r => r.json()).then(setCredits).catch(() => {})
  }, [])

  return (
    <div className="p-8 max-w-3xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-xl font-bold text-white mb-2">Settings</h1>
        <p className="text-gray-400">
          API keys and configuration. All values are stored in the <code className="text-scai-400 bg-gray-800 px-1.5 py-0.5 rounded text-sm">.env</code> file.
          Edit it directly or use the guide below.
        </p>
      </div>

      {/* Status cards */}
      {config && (
        <div className="grid grid-cols-2 gap-4 mb-8">
          {Object.entries(config).map(([key, ok]) => (
            <div
              key={key}
              className={`flex items-center gap-3 p-3 rounded-lg border ${
                ok
                  ? 'bg-emerald-900/10 border-emerald-800/40'
                  : 'bg-red-900/10 border-red-800/40'
              }`}
            >
              <span className={`text-lg ${ok ? 'text-emerald-400' : 'text-red-400'}`}>
                {ok ? '✓' : '✗'}
              </span>
              <span className={`text-sm font-medium ${ok ? 'text-emerald-300' : 'text-red-300'}`}>
                {key.replace(/_/g, ' ')}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* ZeroBounce credits */}
      {credits?.credits != null && (
        <div className={`mb-6 p-4 rounded-xl border flex items-center gap-3 ${
          credits.credits > 100
            ? 'bg-emerald-900/20 border-emerald-800/40'
            : credits.credits > 50
            ? 'bg-amber-900/20 border-amber-800/40'
            : 'bg-red-900/20 border-red-800/40'
        }`}>
          <span className="text-2xl">✉️</span>
          <div>
            <div className={`font-medium ${
              credits.credits > 100 ? 'text-emerald-300' : credits.credits > 50 ? 'text-amber-300' : 'text-red-300'
            }`}>
              ZeroBounce Credits: {credits.credits.toLocaleString()}
            </div>
            <div className="text-xs text-gray-500 mt-0.5">
              {credits.credits < 50
                ? '⚠️ Low credits — pipeline will pause when below 50'
                : 'Credits available'}
            </div>
          </div>
        </div>
      )}

      {/* Setup instructions */}
      <div className="card mb-6">
        <h3 className="font-semibold text-white mb-4">Setup Instructions</h3>

        <div className="space-y-6 text-sm">
          <div>
            <h4 className="font-medium text-gray-200 mb-2 flex items-center gap-2">
              <span className="w-5 h-5 rounded-full bg-scai-600 text-white text-xs flex items-center justify-center">1</span>
              Create your .env file
            </h4>
            <div className="bg-gray-950 rounded-lg p-3 font-mono text-xs text-gray-400 border border-gray-800">
              <div>cp .env.example .env</div>
              <div className="text-gray-600 mt-1"># Then edit .env with your API keys</div>
            </div>
          </div>

          <div>
            <h4 className="font-medium text-gray-200 mb-2 flex items-center gap-2">
              <span className="w-5 h-5 rounded-full bg-scai-600 text-white text-xs flex items-center justify-center">2</span>
              Google Sheets Setup
            </h4>
            <ol className="space-y-1.5 text-gray-400 list-decimal list-inside">
              <li>Create a Google Cloud project and enable the Sheets API</li>
              <li>Create a service account and download the JSON credentials</li>
              <li>Save the JSON to <code className="text-scai-400 bg-gray-800 px-1 rounded">credentials/google-service-account.json</code></li>
              <li>Share your spreadsheet with the service account email</li>
              <li>Copy the Spreadsheet ID from the URL and set it in <code className="text-scai-400 bg-gray-800 px-1 rounded">.env</code></li>
              <li>Sheet must have 4 tabs: Target Accounts, First Clean List, Searcher Output, Final Filtered List</li>
            </ol>
          </div>

          <div>
            <h4 className="font-medium text-gray-200 mb-2 flex items-center gap-2">
              <span className="w-5 h-5 rounded-full bg-scai-600 text-white text-xs flex items-center justify-center">3</span>
              Unipile Setup (LinkedIn API)
            </h4>
            <ol className="space-y-1.5 text-gray-400 list-decimal list-inside">
              <li>Sign up at <code className="text-scai-400 bg-gray-800 px-1 rounded">dashboard.unipile.com</code></li>
              <li>Create an Access Token and copy the API key + DSN</li>
              <li>Connect your LinkedIn Sales Navigator account under Accounts</li>
              <li>Copy the Account ID from <code className="text-scai-400 bg-gray-800 px-1 rounded">/api/v1/accounts</code></li>
            </ol>
          </div>

          <div>
            <h4 className="font-medium text-gray-200 mb-2 flex items-center gap-2">
              <span className="w-5 h-5 rounded-full bg-scai-600 text-white text-xs flex items-center justify-center">4</span>
              Install &amp; Run
            </h4>
            <div className="bg-gray-950 rounded-lg p-3 font-mono text-xs text-gray-400 border border-gray-800 space-y-1">
              <div><span className="text-gray-600"># One command does everything</span></div>
              <div>bash start.sh</div>
              <div className="mt-2"><span className="text-gray-600"># Or use CLI directly</span></div>
              <div>python -m src.main fini --companies "Company A,Company B"</div>
              <div>python -m src.main searcher --companies "Company A"</div>
            </div>
          </div>
        </div>
      </div>

      {/* .env fields reference */}
      <div className="card">
        <h3 className="font-semibold text-white mb-4">.env Reference</h3>
        <div className="space-y-6">
          {SETTINGS_FIELDS.map(({ group, fields }) => (
            <div key={group}>
              <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-3">{group}</div>
              <div className="space-y-2">
                {fields.map(({ key, label, placeholder, required }) => (
                  <div key={key} className="flex items-center gap-3 text-sm">
                    <code className="text-scai-400 font-mono text-xs bg-gray-800 px-2 py-1 rounded w-56 flex-shrink-0">{key}</code>
                    <span className="text-gray-400 flex-1">{label}</span>
                    {required && <span className="text-red-400 text-xs">required</span>}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
