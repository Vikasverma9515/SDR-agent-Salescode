# Salescode ProspectOps

Autonomous B2B prospecting pipeline — enriches company lists, discovers decision-maker contacts, and validates emails before outreach. Built for SDR teams.

---

## What it does

Three sequential modules that take you from a list of company names to verified, outreach-ready contacts:

```
Company Names
      ↓
  [01 Fini] — Enriches companies: domain, LinkedIn org ID, email format
      ↓
  Google Sheets (Target Accounts)
      ↓
  [02 Searcher] — Finds contacts: LinkedIn, TheOrg, web, AI Scout
      ↓
  [03 Veri] — Validates: ZeroBounce email check, LinkedIn tenure, 6-step QC
      ↓
  First Clean List (ready for outreach)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 16, React 19, Tailwind CSS 4 |
| Backend | Python 3.11, FastAPI, Uvicorn |
| AI Agents | LangGraph, OpenAI GPT-4o |
| AI Scout | Perplexity sonar-pro + Claude (AWS Bedrock) |
| Contact discovery | Unipile (LinkedIn API), Tavily, TheOrg |
| Email validation | ZeroBounce |
| Data storage | Google Sheets (via gspread) |
| Real-time logs | WebSocket (FastAPI + native browser WS) |

---

## Project Structure

```
├── backend/
│   ├── agents/
│   │   ├── fini.py          # Target enrichment agent (LangGraph)
│   │   ├── searcher.py      # Contact discovery agent (LangGraph)
│   │   └── veri.py          # Contact QC agent (LangGraph)
│   ├── tools/
│   │   ├── sheets.py        # Google Sheets read/write
│   │   ├── unipile.py       # LinkedIn API (Unipile)
│   │   ├── search.py        # Tavily web search
│   │   ├── zerobounce.py    # Email validation
│   │   ├── theorg.py        # TheOrg org chart lookup
│   │   └── llm.py           # OpenAI + Bedrock wrappers
│   ├── utils/
│   │   ├── role_selection.py   # SDR role bucket selection (pause/resume)
│   │   ├── dm_selection.py     # SDR contact selection (pause/resume)
│   │   └── progress.py         # Real-time WebSocket event emitter
│   ├── api.py               # FastAPI app — all REST + WebSocket endpoints
│   ├── config.py            # Pydantic settings (env vars)
│   ├── state.py             # LangGraph state definitions
│   └── main.py              # CLI + Uvicorn server entrypoint
├── src/
│   ├── app/
│   │   ├── page.tsx         # Overview + SDR guide
│   │   ├── fini/page.tsx    # Fini module UI
│   │   ├── searcher/page.tsx # Searcher module UI
│   │   ├── veri/page.tsx    # Veri module UI
│   │   └── settings/page.tsx # Config + service status
│   ├── components/
│   │   ├── LogStream.tsx    # Real-time WebSocket log panel
│   │   ├── Sidebar.tsx      # Navigation
│   │   └── ConfirmationModal.tsx
│   └── lib/api.ts           # API URL helpers
├── requirements.txt         # Python dependencies
├── pyproject.toml           # Python package config
├── package.json             # Node dependencies
├── next.config.ts           # Next.js config + API proxy
├── railway.json             # Railway deployment config
├── vercel.json              # Vercel deployment config
├── Procfile                 # Process definition for Railway
└── start.sh                 # Local development start script
```

---

## Local Development

### Prerequisites

- Python 3.11+
- Node.js 18+
- A `.env` file (copy from `.env.example`)
- Google Service Account JSON in `credentials/`

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/Vikasverma9515/v1_sales_pipeline.git
cd v1_sales_pipeline

# 2. Copy environment file and fill in your keys
cp .env.example .env
# Edit .env with your API keys

# 3. Add Google service account credentials
# Place your JSON file at: credentials/google-service-account.json

# 4. Start everything (backend + frontend)
bash start.sh
```

The app opens automatically at `http://localhost:3000`.

---

## Environment Variables

### Required

| Variable | Description | Where to get |
|----------|-------------|--------------|
| `SPREADSHEET_ID` | Google Sheet ID from the URL | Your Google Sheet URL |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to service account JSON file | Google Cloud → IAM → Service Accounts |
| `OPENAI_API_KEY` | OpenAI API key | platform.openai.com |
| `TAVILY_API_KEY` | Tavily search API key | tavily.com |
| `PERPLEXITY_API_KEY` | Perplexity API key (AI Scout) | perplexity.ai |
| `UNIPILE_API_KEY` | Unipile platform key | dashboard.unipile.com |
| `UNIPILE_DSN` | Unipile data source node | dashboard.unipile.com |
| `UNIPILE_ACCOUNT_ID` | LinkedIn account instance ID | Unipile API `/v1/accounts` |
| `ZEROBOUNCE_API_KEY` | Email validation key | zerobounce.net |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_BEARER_TOKEN_BEDROCK` | — | AWS Bedrock key (Claude for AI Scout) |
| `AWS_BEDROCK_REGION` | `us-east-1` | AWS region for Bedrock |
| `N8N_WEBHOOK_URL` | — | n8n automation webhook |
| `ALLOWED_ORIGINS` | `*` | CORS origins (set to Vercel URL in production) |
| `UI_PORT` | `8080` | Backend server port |

### Cloud only

| Variable | Description |
|----------|-------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT` | Full service account JSON as a single-line string (use instead of file on Railway/cloud) |
| `BACKEND_URL` | Set on Vercel — points to your Railway backend URL |

---

## Google Sheets Setup

The pipeline reads from and writes to a single Google Sheet with these tabs:

| Tab | Written by | Columns |
|-----|-----------|---------|
| `Target Accounts` | Fini | Company, Domain, Org ID, Email format, Status |
| `First Clean List` | Searcher + AI Scout | A–N: Name, Title, Company, LinkedIn, Buying role |
| `First Clean List` | Veri | O–U: Verified flag, ZeroBounce score, Final status |

**Setup:**
1. Create a new Google Sheet
2. Create the three tabs with exact names above
3. Share the sheet with your service account email (Editor access)
4. Copy the Sheet ID from the URL into `SPREADSHEET_ID`

---

## Deployment

### Architecture

```
User browser
     ↓
Vercel (Next.js frontend)  →  yourapp.vercel.app
     ↓ /api/* and /ws/* proxied to
Railway (FastAPI backend)  →  yourapi.railway.app
```

### Step 1 — Push to GitHub

```bash
git add .
git commit -m "deploy"
git push origin main
```

### Step 2 — Deploy backend on Railway

1. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
2. Select this repo — Railway detects the `Procfile` automatically
3. Go to **Settings → Networking → Generate Domain**
4. Add all environment variables in the **Variables** tab

For `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT`, minify your JSON file to a single line:
```bash
cat credentials/google-service-account.json | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)))"
```

Test the backend is alive: `https://your-railway-url.railway.app/api/health`

### Step 3 — Deploy frontend on Vercel

1. Go to [vercel.com](https://vercel.com) → New Project → Import from GitHub
2. Select this repo — Vercel detects Next.js automatically
3. Add one environment variable before deploying:
   ```
   BACKEND_URL = https://your-railway-url.railway.app
   ```
4. Deploy → your app is live at `yourapp.vercel.app`

### Step 4 — Update CORS

Back in Railway Variables, set:
```
ALLOWED_ORIGINS = https://yourapp.vercel.app
```

### Auto-deploy

Both Railway and Vercel watch your GitHub repo. Every `git push` to `main` automatically redeploys both services — no manual steps needed.

---

## How Each Module Works

### Fini (Target Builder)
1. Enter company names
2. Agent searches LinkedIn, web, and filings for domain + org ID + email format
3. Results shown for SDR review — approve or edit each one
4. Approved companies written to **Target Accounts** sheet
5. Optionally triggers n8n webhook to relay data

### Searcher (Contact Discovery)
1. Target companies auto-loaded from Fini output
2. Set gap-fill roles (e.g. "VP Sales, CMO, Head of Marketing")
3. Agent searches LinkedIn (via Unipile), TheOrg, and web sources in parallel
4. SDR selects relevant departments (role bucket filter)
5. SDR reviews and approves contacts from each batch
6. Approved contacts written to **First Clean List**
7. **AI Scout tab** — chat naturally to find specific people by role or name (uses Perplexity + LinkedIn + Claude)

### Veri (Contact QC)
Runs automatically after Searcher, or manually on any row range.

6-step validation stack per contact:
1. Identity sweep (DDG + TheOrg)
2. Org chart check (TheOrg / Tavily)
3. Conflict resolution (Perplexity AI)
4. Profile audit (LinkedIn via Unipile)
5. Tenure check (employment history)
6. Email probe (ZeroBounce)

Labels each contact: **VERIFIED** / **REVIEW** / **DECOMMISSIONED**

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/config/check` | Service connectivity status |
| `POST` | `/api/fini/run` | Start Fini enrichment |
| `POST` | `/api/fini/confirm` | SDR approves a company |
| `POST` | `/api/fini/commit` | Write approved company to sheet |
| `POST` | `/api/searcher/run` | Start contact discovery scan |
| `POST` | `/api/searcher/{id}/select-roles` | SDR selects department buckets |
| `POST` | `/api/searcher/{id}/select-dms` | SDR approves contacts |
| `POST` | `/api/searcher/{id}/find-more` | Find more contacts via prompt |
| `POST` | `/api/searcher/prospect-chat` | AI Scout chat (Perplexity + LinkedIn + Claude) |
| `POST` | `/api/searcher/scout-commit` | Write AI Scout contact to sheet |
| `POST` | `/api/veri/run` | Start contact verification |
| `WS` | `/ws/{thread_id}` | Real-time pipeline log stream |
