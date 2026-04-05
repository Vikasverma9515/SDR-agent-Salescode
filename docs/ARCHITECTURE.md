# Salescode ProspectOps - Complete Architecture & Documentation

> Autonomous B2B prospecting pipeline that enriches company lists, discovers decision-maker contacts, and validates them before outreach.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Tech Stack](#3-tech-stack)
4. [Project Structure](#4-project-structure)
5. [The Four Agents](#5-the-four-agents)
   - [Fini (Target Builder)](#51-fini---target-builder)
   - [Searcher (Contact Discovery)](#52-searcher---contact-discovery)
   - [Veri (Contact QC)](#53-veri---contact-qc)
   - [Scout (AI Chat Assistant)](#54-scout---ai-chat-assistant)
6. [Pipeline Flow](#6-pipeline-flow)
7. [Google Sheets Data Model](#7-google-sheets-data-model)
8. [External Integrations](#8-external-integrations)
9. [Backend API Reference](#9-backend-api-reference)
10. [Frontend Architecture](#10-frontend-architecture)
11. [n8n Integration & Auto-Pipeline](#11-n8n-integration--auto-pipeline)
12. [Command Center / Orchestrator](#12-command-center--orchestrator)
13. [Configuration & Environment](#13-configuration--environment)
14. [Deployment](#14-deployment)

---

## 1. System Overview

SCAI ProspectOps is an end-to-end B2B sales prospecting system built for SDR teams. It takes a list of target company names and produces a fully enriched, verified contact list ready for outreach.

**What it does (high level):**

```
Company Names → Enrichment → Contact Discovery → Verification → Outreach-Ready List
                  (Fini)        (Searcher)           (Veri)
```

**Key capabilities:**
- Enriches companies with domains, LinkedIn org IDs, email formats, and account size
- Discovers decision-maker contacts across LinkedIn, web, org charts, and AI research
- Verifies every contact through a 6-phase pipeline (web intel, LinkedIn audit, LLM cross-reasoning)
- Classifies contacts into role tiers: FDM, KDM, CTO/CIO, Key Influencer, Gatekeeper
- Supports 100+ countries with multilingual role matching (English, Spanish, French, German, Italian, Portuguese)
- Real-time log streaming via WebSocket
- Interactive chat-based lead finding (Scout)

---

## 2. Architecture Diagram

```
                         ┌──────────────────────────────────────────────┐
                         │              FRONTEND (Next.js)              │
                         │                                              │
                         │  /fini    /searcher    /veri    /command-ctr │
                         │    │          │          │           │       │
                         │    └──── LogStream (WebSocket) ─────┘       │
                         └────────────────────┬─────────────────────────┘
                                              │ HTTP + WS
                                              ▼
                         ┌──────────────────────────────────────────────┐
                         │           BACKEND (FastAPI + LangGraph)       │
                         │                                              │
                         │  ┌─────────┐  ┌──────────┐  ┌─────────┐    │
                         │  │  Fini   │→ │ Searcher  │→ │  Veri   │    │
                         │  │ (Agent) │  │ (Agent)   │  │ (Agent) │    │
                         │  └────┬────┘  └─────┬─────┘  └────┬────┘    │
                         │       │             │             │         │
                         │  ┌────┴─────────────┴─────────────┴────┐    │
                         │  │              TOOLS LAYER              │    │
                         │  │                                      │    │
                         │  │  sheets  unipile  search  llm  n8n  │    │
                         │  │  theorg  wikidata  zerobounce       │    │
                         │  │  domain_discovery  sales_nav_scraper│    │
                         │  └──────────────────────────────────────┘    │
                         │                                              │
                         │  ┌──────────────────────────────────────┐    │
                         │  │         ORCHESTRATOR                  │    │
                         │  │  Pipeline status + AI analysis        │    │
                         │  └──────────────────────────────────────┘    │
                         └──────────────────────────────────────────────┘
                                              │
                    ┌─────────────┬────────────┼────────────┬──────────────┐
                    ▼             ▼            ▼            ▼              ▼
              Google Sheets   Unipile     OpenAI/GPT-5  Perplexity     n8n
              (gspread)      (LinkedIn)   + Bedrock     + Tavily    (webhook)
                                          (Claude)      + DDG
```

---

## 3. Tech Stack

### Backend
| Component | Technology |
|-----------|-----------|
| Framework | FastAPI (async, Python 3.11+) |
| Agent Framework | LangGraph (StateGraph with checkpointing) |
| State Management | Pydantic v2 models |
| Database | Google Sheets (gspread) + SQLite (LangGraph checkpoints) |
| Real-time | WebSocket (native FastAPI) |
| CLI | Typer + Rich |
| Logging | structlog (structured JSON logging) |

### Frontend
| Component | Technology |
|-----------|-----------|
| Framework | Next.js 16 (App Router) |
| UI | React 19 + Tailwind CSS 4 |
| State | Zustand 5 |
| Real-time | Native WebSocket |

### External Services
| Service | Purpose |
|---------|---------|
| Unipile API | LinkedIn search, profile verification, company lookup (14 accounts, round-robin) |
| OpenAI (GPT-5) | LLM reasoning, web search verification, role classification |
| AWS Bedrock (Claude) | Fallback LLM when OpenAI is down (circuit breaker) |
| Perplexity | AI-powered web search for contact discovery & verification |
| Tavily | Web search (fallback after DDG) |
| DuckDuckGo | Free web search (primary for lightweight queries) |
| ZeroBounce | Email validation (valid/invalid/catch-all) |
| TheOrg | Org chart lookups (role/reporting structure) |
| Wikidata | SPARQL queries for company data |
| n8n | Workflow automation webhook (contact ingestion pipeline) |

---

## 4. Project Structure

```
v1_sales_pipeline/
├── backend/
│   ├── agents/
│   │   ├── fini.py              # Target Builder agent (~2,300 lines)
│   │   ├── searcher.py          # Contact Discovery agent (~3,400 lines)
│   │   ├── veri.py              # Contact QC agent (~1,450 lines)
│   │   └── scout.py             # AI Chat assistant (~975 lines)
│   ├── tools/
│   │   ├── sheets.py            # Google Sheets read/write (gspread)
│   │   ├── unipile.py           # Unipile LinkedIn API (round-robin pool)
│   │   ├── search.py            # Unified search (DDG/Tavily/Perplexity)
│   │   ├── llm.py               # LLM helper (OpenAI → Bedrock fallback)
│   │   ├── n8n.py               # n8n webhook submission
│   │   ├── zerobounce.py        # Email validation
│   │   ├── theorg.py            # TheOrg org chart scraper
│   │   ├── wikidata.py          # Wikidata SPARQL queries
│   │   ├── domain_discovery.py  # Domain + email format discovery
│   │   └── sales_nav_scraper.py # Sales Navigator scraper (Scrapling)
│   ├── prompts/
│   │   ├── fini_prompts.py      # Fini system prompts
│   │   ├── searcher_prompts.py  # Searcher system prompts
│   │   └── veri_prompts.py      # Veri system prompts
│   ├── utils/
│   │   ├── logging.py           # structlog configuration
│   │   ├── pause.py             # Pause/resume gate for pipeline runs
│   │   ├── progress.py          # WebSocket event emitter
│   │   ├── rate_limiter.py      # Rate limiters for external APIs
│   │   ├── retry.py             # Retry with exponential backoff
│   │   ├── dm_selection.py      # DM candidate selection logic
│   │   └── role_selection.py    # Role bucket selection logic
│   ├── api.py                   # FastAPI app + all REST/WS endpoints (~1,430 lines)
│   ├── config.py                # pydantic-settings configuration
│   ├── main.py                  # CLI entrypoint (Typer)
│   ├── orchestrator.py          # Command Center pipeline status + AI analysis
│   └── state.py                 # All Pydantic state models
├── src/                         # Next.js frontend
│   ├── app/
│   │   ├── page.tsx             # Home / SDR guide
│   │   ├── fini/page.tsx        # Fini agent UI
│   │   ├── searcher/page.tsx    # Searcher agent UI
│   │   ├── veri/page.tsx        # Veri agent UI
│   │   ├── command-center/page.tsx  # Pipeline dashboard
│   │   ├── settings/page.tsx    # Settings page
│   │   └── layout.tsx           # Root layout
│   ├── components/
│   │   ├── LogStream.tsx        # Real-time WebSocket log viewer
│   │   ├── Sidebar.tsx          # Navigation sidebar
│   │   └── ConfirmationModal.tsx
│   └── lib/
│       ├── api.ts               # API URL helper (proxy-aware)
│       └── stores.ts            # Zustand stores for all pages
├── credentials/                 # Google service account JSON (gitignored)
├── checkpoints/                 # SQLite checkpoint DB for LangGraph
├── logs/                        # Structured log files
├── .env                         # All secrets (gitignored)
├── requirements.txt             # Python dependencies
├── package.json                 # Node.js dependencies
├── next.config.ts               # Next.js config (API proxy rewrites)
├── Dockerfile                   # Backend container
├── Dockerfile.frontend          # Frontend container
├── render.yaml                  # Render deployment config
├── railway.json                 # Railway deployment config
└── vercel.json                  # Vercel deployment config
```

---

## 5. The Four Agents

All agents are built as **LangGraph StateGraphs** — directed acyclic graphs where each node is an async Python function that reads and updates a typed Pydantic state.

### 5.1 Fini - Target Builder

**Purpose:** Enriches raw company names with domain, LinkedIn org ID, Sales Navigator URL, email format, and account size.

**Input:** Comma-separated company names + region + SDR name  
**Output:** Enriched rows in the "Target Accounts" Google Sheet tab

#### LangGraph Node Flow

```
START
  │
  ▼
parallel_enrich_all
  │  (For EACH company concurrently):
  │    ├── normalize_company        (search-based name cleanup)
  │    ├── scrape_linkedin_org      (Unipile → org ID → Sales Nav URL)
  │    └── discover_domain_and_email (domain + email format via search + ZeroBounce probing)
  │
  ▼
END  (results returned to API for frontend review)
```

#### Key Operations
1. **Name Normalization** — Searches for the company, extracts the cleanest official name from LinkedIn pages, official websites, and page titles. Strips legal suffixes (Ltd, S.A., GmbH, etc.)
2. **LinkedIn Org Lookup** — Uses Unipile API to find the company's LinkedIn org ID, then builds a Sales Navigator people-search URL with region and seniority filters
3. **Domain Discovery** — Searches the web for the company's official domain, validates it with LLM reasoning, then probes 18 email patterns against ZeroBounce to find the correct email format
4. **Account Size Detection** — Determines Small/Medium/Large from search signals (revenue, employee count, stock exchange listing, funding stage)
5. **GPT Fallback** — If Unipile can't find the org ID, uses GPT-5 with web search to find the LinkedIn company page
6. **Region-Aware** — Builds Sales Nav URLs with geo filters for 180+ countries/regions. Auto-detects region from company name hints (e.g., "Nestlé España" → Spain)

#### Auto Mode
When `auto_mode=True`, Fini auto-commits high-confidence results (where domain, org ID, and email format all have strong signals) and only pauses for ambiguous companies that need human review.

---

### 5.2 Searcher - Contact Discovery

**Purpose:** Finds decision-maker contacts for target companies using multi-source search, then writes them to the First Clean List.

**Input:** Company names (must exist in Target Accounts) + role gaps + contact count target  
**Output:** Contact rows appended to "First Clean List" sheet tab

#### LangGraph Node Flow

```
START
  │
  ▼
load_gap_analysis ──────── (if all roles covered) ──→ advance_or_finish → END
  │ (roles missing)
  ▼
expand_search_terms         (multilingual role variants)
  │
  ▼
unipile_search              (LinkedIn people search via Unipile)
  │
  ▼
scrape_sales_nav            (Sales Navigator scraping via Scrapling)
  │
  ▼
search_company_website      (web search for contacts from filings, press, company pages)
  │
  ▼
perplexity_executive_search (AI-powered deep search for executives)
  │
  ▼
deduplicate                 (fuzzy name + company matching to remove duplicates)
  │
  ▼
group_into_role_buckets     (classify contacts into 5 role tiers)
  │
  ▼
await_role_selection        (SDR picks which role groups to keep — or auto-approve all)
  │
  ▼
score_and_rank              (AI scoring: seniority, relevance, recency)
  │
  ▼
await_full_selection        (SDR reviews final contact list — or auto-approve)
  │
  ▼
validate_linkedin           (Unipile verify_profile for each contact)
  │
  ▼
enrich_contacts             (email construction, buying role classification)
  │
  ▼
write_to_sheet              (append to First Clean List + Searcher Output)
  │
  ▼
advance_or_finish ──────── (more companies?) ──→ load_gap_analysis (loop)
  │ (all done)
  ▼
END
```

#### Key Operations
1. **Gap Analysis** — Reads Target Accounts (Fini's output) for org_id/domain/email_format. Reads First Clean List to find which contacts already exist. Uses LLM to determine which role tiers are missing.
2. **Multi-Source Search:**
   - Unipile LinkedIn people search (round-robin across 14 accounts)
   - Sales Navigator scraping (Scrapling with li_at cookie)
   - Web search (DDG/Tavily) for company filings, press releases, about pages
   - Perplexity AI executive search
   - TheOrg org chart lookups
   - Wikidata SPARQL queries
3. **Role Classification** — 5-tier system with multilingual keyword matching:
   - **FDM** (Final Decision Makers): CEO, MD, President, Founder, COO, Country Manager
   - **KDM** (Key Decision Makers): Sales Director, VP Sales, CRO, CIO, Head of Sales
   - **CTO/CIO**: CTO, CIO, Chief Digital Officer, VP Engineering
   - **Key Influencer**: Directors, VPs, Heads of departments (60+ role titles)
   - **Gatekeeper**: Managers, Coordinators, Assistants
4. **Email Construction** — Applies the company's known email format (from Fini) to construct emails: `{first}.{last}@domain.com`
5. **LinkedIn Validation** — Verifies each contact's LinkedIn profile via Unipile, confirming identity and current employment

---

### 5.3 Veri - Contact QC

**Purpose:** Multi-phase verification pipeline that validates every contact in the First Clean List before outreach.

**Input:** Row range or company filter on the First Clean List  
**Output:** Updated verification columns (Q-W) on First Clean List; rejected contacts moved to "Reject Profiles" tab

#### LangGraph Node Flow

```
START
  │
  ▼
read_contacts          (pull unverified contacts from First Clean List)
  │
  ▼
parallel_verify_all    (Semaphore(6) — 6 contacts verified concurrently)
  │
  │  For EACH contact:
  │    Phase 0: LinkedIn Discovery (if no URL, search Unipile by name)
  │    Phase 1: Web Intelligence (GPT-5 + DDG + Perplexity + TheOrg + Tavily)
  │    Phase 2: LinkedIn Audit (Unipile verify_profile)
  │    Phase 3: LLM Cross-Reasoning (gpt-4.1 synthesizes all evidence)
  │    Phase 4: Verdict + Routing (deterministic rules → VERIFIED / REVIEW / REJECT)
  │
  ▼
END
```

#### The 6-Phase Verification Pipeline (per contact)

**Phase 0 — LinkedIn Discovery**
- If no LinkedIn URL in the sheet, searches Unipile by name (fuzzy matching at 85% threshold)
- Discovers and attaches the LinkedIn profile URL

**Phase 1 — Web Intelligence (all run in parallel)**
- **GPT-5 Primary Verifier**: Searches the web like a human analyst. Checks if the person currently works at the target company, their actual employer, actual title, and domain match. Handles similar-name companies (e.g., "Marico" vs "Marico Investments Pty Ltd")
- **DuckDuckGo x3**: Three queries (name+company, name+title+company, name+LinkedIn+company) for breadth
- **Perplexity**: Structured role query for recent employment data
- **TheOrg**: Org chart lookup for title and reporting structure
- **Tavily**: Deep search fallback when DDG is inconclusive

**Phase 2 — LinkedIn Audit**
- Calls Unipile `verify_profile` on the LinkedIn URL
- Confirms: profile is valid, person is at target company, still employed, current role title

**Phase 3 — LLM Cross-Reasoning**
- GPT-4.1 reads ALL evidence from Phases 0-2
- Produces three verdicts: identity confidence, employment confidence, title_match
- Handles edge cases like similar company names, different countries, fake profiles

**Phase 4 — Verdict & Routing**
Deterministic rules on the three signals:
| Condition | Verdict |
|-----------|---------|
| LinkedIn confirmed + title match | **VERIFIED** |
| LinkedIn shows different department | **REJECT** |
| LinkedIn shows person left company | **REJECT** |
| No identity confirmation + bad email | **REJECT** |
| Strong web evidence but no LinkedIn | **REVIEW** |
| Mixed signals | **REVIEW** |

**Sheet Updates:**
- VERIFIED/REVIEW → stays in First Clean List (cols Q-W updated)
- REJECT → row copied to "Reject Profiles" tab, then deleted from First Clean List

---

### 5.4 Scout - AI Chat Assistant

**Purpose:** Interactive chat-based lead finder. SDRs can ask natural-language questions like "Find me the CEO of Nestlé India" and Scout searches, enriches, and returns candidates.

**Input:** Natural-language query + optional company context  
**Output:** Enriched candidate cards with verification status

#### LangGraph Node Flow

```
parse_intent
  │
  ├─→ [greeting / not-search / no-company] → synthesize → END  (<1s response)
  │
  └─→ prepare (parallel: dup-check + company-lookup)
         │
         ▼
       research (parallel: Perplexity + LinkedIn search)
         │
         ▼
       enrich (verify_profile on top candidates)
         │
         ▼
       synthesize → END
```

#### Key Features
- **Intent Detection**: Greetings and vague queries return instantly (<1s) with no external calls
- **Global 50s Timeout**: Hard cap on entire pipeline to prevent hangs
- **Duplicate Detection**: Checks existing contacts in First Clean List before suggesting
- **Commit to Sheet**: Each candidate can be individually committed to the First Clean List (cols A-P) for Veri verification
- **Company Context Carry-Over**: Remembers domain, email format, account type from the Fini-enriched Target Accounts

---

## 6. Pipeline Flow

### Full Manual Pipeline

```
SDR pastes company names
        │
        ▼
   ┌─────────┐     Writes to "Target Accounts" sheet
   │  FINI   │───→ (Company Name, Sales Nav URL, Domain, Email Format,
   │         │      Account Type, Account Size)
   └────┬────┘
        │
        ▼  (optional)
   ┌─────────┐     n8n scrapes LinkedIn Sales Nav URL → sends contacts
   │   n8n   │───→ via webhook to /api/n8n/contacts endpoint
   │         │     Contacts written to "First Clean List" (cols A-N)
   └────┬────┘
        │
        ▼
   ┌──────────┐    Reads Target Accounts + First Clean List
   │ SEARCHER │───→ Finds missing role gaps via multi-source search
   │          │    Writes to "First Clean List" (cols A-P) + "Searcher Output"
   └────┬─────┘
        │
        ▼
   ┌─────────┐    Reads unverified contacts from First Clean List
   │  VERI   │───→ 6-phase verification per contact
   │         │    Updates cols Q-W (VERIFIED / REVIEW / REJECT)
   └────┬────┘    Moves REJECTs to "Reject Profiles" tab
        │
        ▼
  Outreach-Ready List
  (First Clean List with all contacts verified)
```

### Automated n8n Pipeline

When n8n sends contacts via the `/api/n8n/contacts` webhook:

```
n8n drip-feeds contacts → buffered in memory (grouped by company)
        │
        ▼  (3 min silence triggers flush)
For EACH company (sequentially):
   1. Write contacts to First Clean List
   2. Veri Round 1  (verify n8n contacts)
   3. Searcher      (find gaps)
   4. Veri Round 2  (verify Searcher contacts)
        │
        ▼
  Pipeline complete for all companies
```

---

## 7. Google Sheets Data Model

All data lives in a single Google Spreadsheet with these tabs:

### Target Accounts (Fini's output)
| Column | Header | Description |
|--------|--------|-------------|
| A | Company Name | Normalized company name |
| B | Parent Company Name | Parent/group name |
| C | Sales Navigator Link | Full Sales Nav people-search URL |
| D | Company Domain | e.g., `nestle.com` |
| E | SDR Name | Assigned SDR |
| F | Email Format | e.g., `{first}.{last}` |
| G | Account type | Region: India, LATAM, Europe, etc. |
| H | Account Size | Small / Medium / Large |

### First Clean List (Main working sheet)
| Column | Header | Writer | Description |
|--------|--------|--------|-------------|
| A | Company Name | n8n/Searcher | Company name |
| B | Normalized Company Name | n8n/Searcher | Parent group name |
| C | Company Domain Name | n8n/Searcher | Domain |
| D | Account type | n8n/Searcher | Region |
| E | Account Size | n8n/Searcher | Small/Medium/Large |
| F | Country | n8n/Searcher | Contact's country |
| G | First Name | n8n/Searcher | Contact first name |
| H | Last Name | n8n/Searcher | Contact last name |
| I | Job titles (English) | n8n/Searcher | Job title |
| J | Buying Role | n8n/Searcher | FDM/KDM/Influencer/GateKeeper |
| K | Linekdin Url | n8n/Searcher | LinkedIn profile URL |
| L | Email | n8n/Searcher | Constructed or discovered email |
| M | Phone-1 | n8n | Phone number |
| N | Phone-2 | n8n | Secondary phone |
| O | Source | System | "n8n" or "searcher" or "scout" |
| P | Pipeline Status | System | Tracking field |
| Q | LinkedIn Status | **Veri** | Profile verification result |
| R | Employment Verified | **Veri** | YES/NO/UNCERTAIN |
| S | Title Match | **Veri** | MATCH/MISMATCH/UNCERTAIN |
| T | Actual Title Found | **Veri** | Title from LinkedIn/web |
| U | Overall Status | **Veri** | **VERIFIED / REVIEW / REJECT** |
| V | Verification Notes | **Veri** | Human-readable explanation |
| W | Verified On | **Veri** | ISO timestamp |

### Searcher Output (Searcher's log)
| Column | Header |
|--------|--------|
| A | Company |
| B | Full Name |
| C | Job Title |
| D | Role Bucket |
| E | LinkedIn URL |
| F | LinkedIn Status |
| G | Email Address |
| H | Email Status |

### Reject Profiles (Veri's rejected contacts)
Same columns A-W as First Clean List, with column U being "Reject Reason" instead of "Overall Status".

### N8N Webhook Log (Full visibility of all n8n webhook hits)
| Column | Header |
|--------|--------|
| A | Timestamp |
| B | Company Name |
| C-N | Contact fields + status + raw JSON |

---

## 8. External Integrations

### Unipile (LinkedIn API)
- **14 LinkedIn/Sales Navigator accounts** in a round-robin pool
- Functions: `get_company_org_id()`, `search_people()`, `verify_profile()`, `search_person_by_name()`
- Pool initialization: ENV var → API fetch → single fallback
- Each call uses next account from the cycle

### OpenAI (GPT-5 + GPT-4.1)
- **GPT-5**: Web search verification in Veri (primary verifier), gap analysis in Searcher
- **GPT-4.1**: Cross-reasoning synthesis in Veri (Phase 3)
- Uses OpenAI Responses API with `web_search` tool
- **Circuit breaker**: 2 failures in 60s → permanent switch to Bedrock (Claude) for the session

### AWS Bedrock (Claude)
- Fallback LLM when OpenAI is unavailable
- Uses Claude via AWS Bedrock (bearer token auth)
- Same interface as OpenAI calls

### Search Providers
- **DuckDuckGo**: Free, no API key, used for lightweight queries
- **Tavily**: Deep search with structured results, used as fallback
- **Perplexity**: AI-powered search, used for executive discovery and role queries
- Unified interface via `search()` and `search_with_fallback()` functions
- Rate-limited via shared semaphore

### ZeroBounce
- Email validation: valid/invalid/catch-all/unknown
- Credit check on startup (pauses if < 50 credits)
- Results cached per email (never validates same email twice)

### TheOrg
- Org chart scraping from theorg.com
- Returns: full name, role title, department, reports-to, LinkedIn URL

### Wikidata
- SPARQL queries for company info (official website, industry, country, employee count)
- Also used for person lookups (employer, position)

### n8n
- Webhook endpoint receives contacts from n8n workflow
- 60-second delay between submissions (configurable)
- Payload matches App Script's buildPayload_ format
- 3-minute buffer timeout before auto-flush

---

## 9. Backend API Reference

### Health & Config
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/config/check` | Verify all API keys are configured |

### Fini
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/fini/run` | Start Fini for a list of companies |
| POST | `/api/fini/confirm` | Confirm/edit operator review |
| POST | `/api/fini/commit` | Commit a single company to Target Accounts |
| POST | `/api/fini/reenrich` | Re-enrich a company that's already in the sheet |
| GET | `/api/fini/results/{thread_id}` | Get cached enrichment results |

### Searcher
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/searcher/run` | Start Searcher for companies |
| POST | `/api/searcher/prospect-chat` | Scout AI chat endpoint |
| POST | `/api/searcher/scout-commit` | Commit a Scout candidate to sheet |
| POST | `/api/searcher/{thread_id}/select-dms` | SDR selects DM candidates |
| POST | `/api/searcher/{thread_id}/select-roles` | SDR selects role buckets |
| POST | `/api/searcher/{thread_id}/find-more` | SDR requests more contacts |
| GET | `/api/searcher/{thread_id}/dm-pending` | Get pending DM candidates |

### Veri
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/veri/run` | Start Veri on a row range or company |

### n8n Integration
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/n8n/contacts` | Receive contacts from n8n webhook |
| POST | `/api/n8n/complete` | Legacy: trigger Veri after n8n batch |
| POST | `/api/n8n/flush` | Force flush the n8n buffer |
| POST | `/api/n8n/retry` | Retry failed pipeline steps |
| GET | `/api/n8n/buffer` | View current buffer state |
| GET | `/api/n8n/pipeline` | View pipeline progress |
| GET | `/api/n8n/debug` | Debug info (last payload, errors) |

### Orchestrator / Command Center
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/orchestrator/status` | Full pipeline status (all companies) |
| POST | `/api/orchestrator/trigger` | Trigger an agent for a specific company |
| POST | `/api/orchestrator/analyze` | AI analysis of pipeline state |
| POST | `/api/orchestrator/auto-mode` | Enable/disable auto-mode |
| GET | `/api/orchestrator/auto-mode/status` | Get auto-mode state |

### Run Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/runs` | List all active runs |
| POST | `/api/pipeline/stop-all` | Cancel all active runs |
| GET | `/api/runs/{thread_id}` | Get status of a specific run |
| POST | `/api/runs/{thread_id}/cancel` | Cancel a specific run |
| POST | `/api/runs/{thread_id}/pause` | Pause a running agent |
| POST | `/api/runs/{thread_id}/resume` | Resume a paused agent |

### WebSocket
| Endpoint | Description |
|----------|-------------|
| `/ws/{thread_id}` | Real-time log streaming + structured events |

Event types sent via WebSocket:
- `log`: Timestamped log line (info/warning/error/success)
- `progress`: Per-company stage progress (Fini/Searcher)
- `veri_contact`: Veri per-contact status updates
- `veri_step`: Veri per-phase step updates
- `fini_result`: Enrichment results ready for review
- `confirmation_needed`: Operator confirmation required
- `pause`: Agent paused, waiting for SDR input
- `done`: Agent run completed

---

## 10. Frontend Architecture

### Pages

| Route | Component | Description |
|-------|-----------|-------------|
| `/` | Home | SDR quickstart guide, module overview |
| `/fini` | Fini Page | Company input, region selector, auto-mode toggle, enrichment result cards, commit buttons |
| `/searcher` | Searcher Page | Company selector, DM role configuration, contact approval UI, Scout chat |
| `/veri` | Veri Page | Row range input, company filter, per-contact verification progress |
| `/command-center` | Command Center | Pipeline dashboard, per-company status, AI analysis, auto-trigger buttons |
| `/settings` | Settings | Configuration and status |

### Key Components

**LogStream** — WebSocket-powered real-time log viewer
- Connects to `/ws/{thread_id}`
- Color-coded log levels (info=blue, warning=amber, error=red, success=green)
- Company-level filtering
- Auto-scroll with manual override

**Sidebar** — Navigation with active agent indicators

### State Management (Zustand)
Each page has its own Zustand store:
- `useFiniStore` — companies, enrichment results, thread IDs
- `useSearcherStore` — companies, contacts, role selections
- `useVeriStore` — contacts, verification progress
- `useCommandCenterStore` — pipeline status, AI analysis, auto-mode

### API Proxy
Next.js `rewrites` in `next.config.ts` proxy all `/api/*` and `/ws/*` requests to the FastAPI backend, eliminating CORS issues during development.

---

## 11. n8n Integration & Auto-Pipeline

### How n8n Sends Contacts

1. Fini writes a company to Target Accounts with a Sales Nav URL
2. (Optional) Fini submits to n8n webhook → n8n scrapes Sales Navigator
3. n8n sends contacts one-by-one or in small batches to `POST /api/n8n/contacts`
4. Each contact payload is normalized (handles many field name variants)
5. Contacts are buffered in memory, grouped by company

### Buffer & Flush Mechanism

```
n8n POST → normalize → duplicate check → buffer (in-memory)
                                              │
                                     3 min silence timer
                                              │
                                              ▼
                                        FLUSH (per company):
                                          1. Write to First Clean List
                                          2. Veri Round 1 (verify n8n contacts)
                                          3. Searcher (find role gaps)
                                          4. Veri Round 2 (verify Searcher contacts)
```

- Contacts are deduplicated against existing First Clean List rows (fuzzy name + company match)
- Each pipeline step has retry logic (up to 2 retries per step)
- Pipeline progress is viewable via `GET /api/n8n/pipeline`
- Manual flush available via `POST /api/n8n/flush`

---

## 12. Command Center / Orchestrator

The Command Center provides a bird's-eye view of the entire pipeline.

### Pipeline Stages (per company)
| Stage | Meaning |
|-------|---------|
| `enrichment_pending` | Not in Target Accounts (needs Fini) |
| `contacts_pending` | In Target Accounts but no contacts (needs Searcher/n8n) |
| `verification_pending` | Has contacts, none verified (needs Veri) |
| `verification_partial` | Some contacts verified, others pending |
| `verification_complete` | All contacts verified |
| `ready_for_outreach` | Verified contacts with valid emails |

### AI Analysis
Uses LLM to analyze the pipeline state and suggest next actions:
- Which companies need attention
- Which role tiers are missing
- Priority recommendations

### Auto-Mode
When enabled, automatically triggers agents for companies that need processing:
- Runs Searcher for companies with no contacts
- Runs Veri for companies with unverified contacts

---

## 13. Configuration & Environment

All configuration via `.env` file (loaded by pydantic-settings):

```env
# Google Sheets
GOOGLE_SERVICE_ACCOUNT_JSON=credentials/google-service-account.json
SPREADSHEET_ID=<spreadsheet-id>

# Unipile (LinkedIn)
UNIPILE_API_KEY=<key>
UNIPILE_DSN=api21.unipile.com:15157
UNIPILE_ACCOUNT_IDS=id1,id2,id3,...  # 14 accounts for round-robin

# LLM
OPENAI_API_KEY=<key>

# AWS Bedrock (Claude fallback)
AWS_BEARER_TOKEN_BEDROCK=<token>
AWS_BEDROCK_REGION=us-east-1

# Search
TAVILY_API_KEY=<key>
PERPLEXITY_API_KEY=<key>

# Email Validation
ZEROBOUNCE_API_KEY=<key>

# n8n
N8N_WEBHOOK_URL=<url>
N8N_SUBMISSION_DELAY=60

# LinkedIn (Sales Nav scraping)
LINKEDIN_LI_AT_COOKIE=<cookie>

# Server
UI_HOST=0.0.0.0
UI_PORT=8080
ALLOWED_ORIGINS=*
```

---

## 14. Deployment

### Local Development

```bash
# Backend + Frontend together
npm run dev

# Or separately:
npm run dev:ui      # Next.js on :3000
npm run dev:api     # FastAPI on :8080
```

### CLI Usage

```bash
python -m backend.main fini --companies "Nestlé,Unilever" --region India --sdr "John"
python -m backend.main searcher --companies "Nestlé"
python -m backend.main veri
python -m backend.main ui  # Start web server
```

### Docker

```dockerfile
# Backend
docker build -t scai-backend -f Dockerfile .

# Frontend
docker build -t scai-frontend -f Dockerfile.frontend .
```

### Cloud Platforms
- **Render**: `render.yaml` (web service for backend)
- **Railway**: `railway.json` (with Procfile: `web: ...`)
- **Vercel**: `vercel.json` (for Next.js frontend only)

For cloud deployment, set `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT` to the full JSON string of the service account (instead of file path).

---

## Appendix: Data Flow Summary

```
┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐    ┌─────────────┐
│  Company     │    │  Target Accounts │    │   First Clean List   │    │  Reject     │
│  Names       │───→│  (Tab)           │───→│   (Tab)              │───→│  Profiles   │
│  (SDR input) │    │                  │    │                      │    │  (Tab)      │
│              │    │  Written by:     │    │  Written by:         │    │             │
│              │    │  FINI            │    │  n8n (A-N)           │    │  Written by:│
│              │    │                  │    │  Searcher (A-P)      │    │  VERI       │
│              │    │  Cols A-H        │    │  Scout (A-P)         │    │  (rejects)  │
│              │    │                  │    │  VERI (Q-W)          │    │             │
└──────────────┘    └──────────────────┘    └──────────────────────┘    └─────────────┘
                                                      │
                                                      ▼
                                            ┌──────────────────┐
                                            │  Searcher Output │
                                            │  (Tab)           │
                                            │                  │
                                            │  Written by:     │
                                            │  SEARCHER        │
                                            │  (log of all     │
                                            │   found contacts)│
                                            └──────────────────┘
```
