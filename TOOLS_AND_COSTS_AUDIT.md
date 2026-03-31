# SCAI ProspectOps — Tools, Credits & Cost Audit

**Date:** 2026-03-31
**Purpose:** Map every external tool/API in the pipeline, credit consumption per unit of work, and monthly cost estimates for senior review.

---

## Pipeline Flow (Quick Reference)

```
Fini (Company Enrichment)
  → n8n (Email/Phone Enrichment)
    → Final Filtered List (Google Sheet)
      → Veri (Contact QC — LinkedIn + Web signals)
        → Verified / Review → stay in FFL
        → Rejected → moved to Rejected Profiles tab
      → Searcher (Gap-fill — find missing CEO, CTO, CSO)
        → Veri again (verify new contacts)
```

---

## 1. Tool-by-Tool Breakdown

### A. ZeroBounce (Email Validation)

| | Details |
|---|---|
| **What it does** | Validates if an email address is deliverable (valid/invalid/catch-all/unknown) |
| **Pricing** | ~$0.007/credit (1 credit = 1 email validated) |
| **API** | `https://api.zerobounce.net/v2/validate` |

**Where it's used:**

| Agent | When | Credits per unit | Notes |
|-------|------|-----------------|-------|
| **Fini** (domain_discovery) | Finding email format for a new company | 1–19 per company | 1 catch-all probe + up to 18 pattern probes. Stops on first hit. |
| **Searcher** | ~~Validating constructed emails~~ | ~~1–19 per contact~~ | **REMOVED (March 2026)** — now constructs emails from known format, zero ZB calls |
| **Veri** | ~~Re-validating emails during verification~~ | ~~1 per contact~~ | **REMOVED (March 2026)** — emails already validated upstream |

**March 2026 usage:** 10,101 credits (~$70)
**Expected after cleanup:** ~500–1,000 credits/month (Fini only, one-time per company)
**Estimated savings:** ~90% reduction

---

### B. OpenAI / LLM (GPT-4.1 family)

| | Details |
|---|---|
| **What it does** | Company analysis, role classification, title comparison, cross-reasoning verdicts |
| **Pricing** | gpt-4.1-mini: $0.40/1M input, $1.60/1M output · gpt-4.1: $2/1M input, $8/1M output · gpt-4o-search-preview: ~$2.50/1M input, $10/1M output |
| **Fallback** | AWS Bedrock Claude Sonnet 4 (auto-switches after 2 OpenAI failures in 60s) |
| **Concurrency** | Global Semaphore(8) across all agents |

**Where it's used:**

| Agent | Operation | Model | Calls per unit | Tokens per call (approx) |
|-------|-----------|-------|---------------|------------------------|
| **Fini** | Company validation, LinkedIn slug guess, domain discovery | gpt-4o-search-preview | 3–5 per company | ~500–1,000 tokens |
| **Fini** | Auto-mode reasoning (choose best LinkedIn candidate) | gpt-4.1 | 1 per company | ~800 tokens |
| **Searcher** | Role classification, title analysis | gpt-4.1-mini | 2–3 per company | ~100–200 tokens |
| **Veri** | Title comparison (semantic match) | gpt-4.1-mini | 1 per contact | ~60 tokens |
| **Veri** | Cross-reasoning (uncertain cases only) | gpt-4.1-mini | 0–1 per contact | ~120 tokens |
| **Veri** | Reject/Review reason generation | gpt-4.1-mini | 0–1 per contact | ~100 tokens |
| **Orchestrator** | AI pipeline analysis & suggestions | gpt-4.1 | 1 per analysis | ~1,500 tokens |
| **Scout** | Contact ranking & importance scoring | gpt-4.1-mini | 1 per query | ~200 tokens |

**Monthly estimate (100 companies, 500 contacts):**
- gpt-4.1-mini: ~200K tokens → ~$0.40
- gpt-4.1: ~150K tokens → ~$1.50
- gpt-4o-search-preview: ~400K tokens → ~$5.00
- **Total LLM: ~$7–10/month**

---

### C. Unipile (LinkedIn API)

| | Details |
|---|---|
| **What it does** | LinkedIn company lookup, people search, profile verification |
| **Pricing** | Subscription-based (15 LinkedIn Sales Nav seats in pool) |
| **API** | `https://{dsn}/api/v1` (default: api21.unipile.com:15157) |
| **Rate Limiter** | 1 request per 5 seconds (linkedin_limiter) |
| **Auth** | X-API-KEY header, round-robins across account pool |

**Where it's used:**

| Agent | Operation | Calls per unit | Notes |
|-------|-----------|---------------|-------|
| **Fini** | Company org lookup (slug variants + keyword search) | 1–20+ per company | Tries multiple slug variants, then keyword search |
| **Fini** | Find real employee name (for ZB probe) | 1 per company | Searches people at company |
| **Searcher** | People search (parallel by role title) | 6 per company | Semaphore(6), one search per target role |
| **Veri** | LinkedIn profile verification | 1 per contact | Confirms identity, employment, current title |
| **Veri** | LinkedIn URL discovery (if missing) | 0–1 per contact | Only when no LinkedIn URL in sheet |
| **Scout** | People search by title/name | 1–3 per query | Interactive, up to 3 title variants |

**Monthly estimate (100 companies, 500 contacts):**
- Fini: ~1,500 calls
- Searcher: ~600 calls
- Veri: ~600 calls
- **Total: ~2,700 Unipile calls/month**
- Cost is subscription-based, not per-call

---

### D. Search APIs (DuckDuckGo, Tavily, Perplexity)

| Provider | Pricing | Auth | Notes |
|----------|---------|------|-------|
| **DuckDuckGo** | Free | None | Via `ddgs` Python library, no API key needed |
| **Tavily** | $5/1,000 searches (Starter) | API key | Used as fallback when DDG is inconclusive |
| **Perplexity** | ~$5/1,000 queries (API) | Bearer token | Used for structured role/employment queries |

**Shared rate limiter:** 5 burst, 1/sec across all providers

**Where it's used:**

| Agent | Operation | Provider | Calls per unit |
|-------|-----------|----------|---------------|
| **Fini** | Domain discovery fallback | Perplexity → Tavily → DDG | 1–3 per company |
| **Veri** | Web intelligence (identity/employment) | DDG ×3 + Perplexity | 4 per contact |
| **Veri** | Tavily fallback (when DDG inconclusive) | Tavily | 0–1 per contact |
| **Searcher** | Website/people discovery | DDG/Perplexity | 2–4 per company |
| **Scout** | Role context lookup | Perplexity | 1 per query |

**Monthly estimate (100 companies, 500 contacts):**
- DDG: ~1,800 calls → **Free**
- Perplexity: ~700 calls → ~$3.50
- Tavily: ~200 calls → ~$1.00
- **Total search: ~$4.50/month**

---

### E. Google Sheets API (gspread)

| | Details |
|---|---|
| **What it does** | Read/write all pipeline data (Target Accounts, FFL, Rejected Profiles, etc.) |
| **Pricing** | Free (Google Cloud project quota: 60 read/write requests per minute) |
| **Rate Limiter** | 50 burst, 0.9/sec (~54 req/min, stays under 60 limit) |
| **Auth** | Service account JSON |

**Tabs used:**

| Tab | Written by | Read by |
|-----|-----------|---------|
| Target Accounts | Fini | Searcher, Orchestrator |
| First Clean List | n8n | Searcher (dedup check) |
| Final Filtered List | Searcher (new contacts), Veri (verification cols O–U) | Veri, Searcher, Orchestrator |
| Searcher Output | Searcher | — |
| Rejected Profiles | Veri (moved rejects) | Orchestrator |

**Monthly estimate:** ~3,000–5,000 API calls → **Free** (within quota)

---

### F. n8n Webhook (Data Enrichment)

| | Details |
|---|---|
| **What it does** | External workflow that enriches company data (emails, phones, org info) — writes results back to First Clean List |
| **Pricing** | Self-hosted (free) or n8n Cloud ($20+/month) |
| **Trigger** | Fini submits company after operator confirmation |
| **Submission delay** | 60 seconds between companies (hardcoded) |
| **Retry** | 3 attempts, 30s backoff |

**Calls:** 1 per company → ~100 calls/month for 100 companies

---

### G. TheOrg (Org Chart Lookup)

| | Details |
|---|---|
| **What it does** | Scrapes TheOrg.com org charts to verify person's role at a company |
| **Pricing** | Free (HTML scraping, no API key) |
| **Rate** | No rate limiter (20s timeout per request) |

**Where it's used:**

| Agent | Calls per unit |
|-------|---------------|
| **Veri** | 1 per contact (web intelligence phase) |

**Monthly estimate:** ~500 calls → **Free**

---

### H. Sales Navigator Scraper (Scrapling/Playwright)

| | Details |
|---|---|
| **What it does** | Browser-based scraping of LinkedIn Sales Navigator company people pages |
| **Pricing** | Free (uses existing LinkedIn session cookie) |
| **Auth** | `LINKEDIN_LI_AT_COOKIE` (browser session) |
| **Risk** | LinkedIn rate limiting / account restrictions if overused |

**Where it's used:**

| Agent | Calls per unit | Notes |
|-------|---------------|-------|
| **Searcher** | 0–1 per company | Optional fallback for people discovery |

**Monthly estimate:** ~10–30 calls → **Free** (but use sparingly to avoid LinkedIn restrictions)

---

### I. AWS Bedrock (Fallback LLM)

| | Details |
|---|---|
| **What it does** | Claude Sonnet 4 as fallback when OpenAI is down |
| **Pricing** | ~$3/1M input, $15/1M output tokens |
| **Trigger** | Auto-switches after 2 OpenAI failures within 60 seconds |
| **Auth** | AWS Bearer token |

**Monthly estimate:** Only used during OpenAI outages → typically **$0/month**

---

## 2. Per-Pipeline-Run Cost (Processing 1 Company End-to-End)

| Step | Tool | Calls | Cost |
|------|------|-------|------|
| **Fini** — company enrichment | Unipile (org lookup) | 5–20 | subscription |
| | LLM (analysis + slug guess) | 3–5 | ~$0.01 |
| | Search (domain discovery) | 2–5 | ~$0.01 |
| | ZeroBounce (email format probe) | 1–19 | ~$0.01–0.13 |
| | Sheets (write) | 1 | free |
| | n8n (submit) | 1 | free/subscription |
| **n8n** — enrichment | External | 1 | depends on n8n setup |
| **Veri** — verify 10 contacts | Unipile (profile verify) | 10 | subscription |
| | Search (DDG + Perplexity) | 40 | ~$0.02 |
| | TheOrg (org chart) | 10 | free |
| | LLM (title + reasoning) | 15–20 | ~$0.01 |
| | Sheets (read + write) | 12 | free |
| **Searcher** — fill 3 role gaps | Unipile (people search) | 6 | subscription |
| | LLM (role analysis) | 3 | ~$0.005 |
| | Sheets (write) | 4 | free |
| | Search (discovery) | 3 | ~$0.01 |
| **Veri** (round 2) — verify 3 new contacts | Same as above ×3 | ~15 | ~$0.01 |

### Total per company (end-to-end): ~$0.07–0.20

### Estimated monthly (100 companies): ~$15–25/month

---

## 3. Monthly Cost Summary

| Service | Before cleanup (March 2026) | After cleanup (April 2026+) | Notes |
|---------|---------------------------|----------------------------|-------|
| **ZeroBounce** | ~$70 (10,101 credits) | ~$5–7 (700–1,000 credits) | Removed from Searcher + Veri |
| **OpenAI LLM** | ~$7–10 | ~$7–10 | No change |
| **Perplexity** | ~$3–4 | ~$3–4 | No change |
| **Tavily** | ~$1–2 | ~$1–2 | No change |
| **Unipile** | Subscription | Subscription | No change |
| **Google Sheets** | Free | Free | Within quota |
| **TheOrg** | Free | Free | HTML scraping |
| **DuckDuckGo** | Free | Free | No API key |
| **AWS Bedrock** | ~$0 (fallback only) | ~$0 | Only during outages |
| | | | |
| **TOTAL** | **~$85–90/month** | **~$15–25/month** | **~70–75% reduction** |

---

## 4. Rate Limiters in Place

| Limiter | Config | Protects |
|---------|--------|----------|
| `sheets_limiter` | 50 burst, 0.9/sec (~54 req/min) | Google Sheets 60 req/min quota |
| `linkedin_limiter` | 1 burst, 0.2/sec (1 req/5 sec) | Unipile / LinkedIn rate limits |
| `search_limiter` | 5 burst, 1.0/sec | DDG, Tavily, Perplexity |
| `zerobounce_limiter` | 10 burst, 0.5/sec | ZeroBounce API |
| LLM Semaphore(8) | 8 concurrent | OpenAI / Bedrock |
| Veri Semaphore(6) | 6 concurrent contacts | Overall Veri throughput |
| Searcher enrich_sem | 8 concurrent | Contact enrichment |

---

## 5. Safety Mechanisms

| Mechanism | What it does |
|-----------|-------------|
| ZeroBounce credit check | Checks balance on first call; disables all validation if 0 credits |
| ZeroBounce credit warning | Logs warning when credits below threshold (default: 50) |
| LLM circuit breaker | Switches to Bedrock after 2 OpenAI failures in 60 seconds |
| ZeroBounce in-memory cache | Never validates the same email twice per server run |
| n8n submission delay | 60-second gap between company submissions |
| Search fallback chain | Perplexity → Tavily → DDG (degrades gracefully) |

---

## 6. Changes Made (March 2026)

### Removed ZeroBounce from Veri
- **Before:** Veri called ZeroBounce for every contact (1 credit each), re-validating emails that n8n already provided
- **After:** Veri uses LinkedIn + web signals only for verification. Email already validated upstream.
- **Savings:** ~500 credits/month

### Removed ZeroBounce from Searcher
- **Before:** Searcher tried up to 18 email patterns via ZeroBounce per contact (18 credits worst case)
- **After:** Searcher constructs emails from known format:
  1. Fini's email format (from Target Accounts)
  2. Learned from existing n8n emails in FFL (reverse-engineered)
  3. Fallback: `{first}.{last}@domain` (most common B2B pattern)
- **Savings:** ~9,000 credits/month

---

## 7. Remaining Risks / Recommendations

| Risk | Impact | Recommendation |
|------|--------|---------------|
| ZeroBounce cache is in-memory only | Resets on server restart → re-probes same domains | Add persistent cache (Redis/file) for domain formats |
| Command Center auto-mode can loop | Triggers Veri/Searcher on a timer (default 60s) | Ensure dry_run=true until pipeline is stable |
| Unipile account pool | 15 seats; heavy Searcher use can exhaust | Monitor seat utilization |
| LinkedIn cookie expiry | Sales Nav scraper silently fails | Add cookie health check on startup |
| Perplexity API costs scale with contacts | 1 call per contact in Veri | Consider caching Perplexity results per company (not per contact) |
