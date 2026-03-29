# Veri Agent — Design & Verification Logic

## What Veri Does

Veri reads contacts from **First Clean List**, runs a 4-phase verification pipeline on each one, and routes them to:

| Verdict | Sheet Tab | Meaning |
|---------|-----------|---------|
| **VERIFIED** | Final Filtered List | High confidence: LinkedIn confirmed + email deliverable + title matches |
| **REVIEW** | Final Filtered List | Moderate confidence: some signals missing but contact plausible |
| **REJECT** | Rejected Profiles | Clear disqualifier found (see routing rules below) |

---

## 4-Phase Pipeline (per contact, concurrent via Semaphore(6))

```
Phase 0  LinkedIn Discovery
Phase 1  Web Intelligence          ← parallel: DDG ×3 + Perplexity + TheOrg (+ Tavily fallback)
Phase 2  LinkedIn Audit + Email    ← parallel: Unipile verify_profile + ZeroBounce
Phase 3  LLM Cross-Reasoning       ← gpt-4.1-mini synthesises all evidence
Phase 4  Verdict + Sheet Routing
```

---

### Phase 0 — LinkedIn URL Discovery

If the sheet has no LinkedIn URL for a contact, Veri calls **Unipile `search_person_by_name`** to find it automatically. Uses fuzzy name matching (≥ 85% token similarity) to avoid false matches.

---

### Phase 1 — Web Intelligence

All searches run **in parallel** for speed:

| Tool | Query | Purpose |
|------|-------|---------|
| **DuckDuckGo ×3** | `"Name" Company`, `"Name" Role Company`, `"Name" LinkedIn Company` | General web presence |
| **Perplexity** | *"What is [Name]'s current job title and employer as of 2025? Are they still at [Company]?"* | Structured role extraction |
| **TheOrg** | Name + Company lookup | Org chart confirmation |
| **Tavily** | `"Name" Company Role` | Fallback when DDG is inconclusive |

Each source is scored for:
- **Positive signal**: name + company both mentioned (person is there)
- **Stale signal**: former / ex- / left / now at / departed / etc. (person has left)

---

### Phase 2 — LinkedIn Audit + ZeroBounce

Run **in parallel**:

**Unipile `verify_profile`** loads the LinkedIn profile and returns:
- `valid`: profile accessible
- `at_target_company`: currently listed at our target company
- `still_employed`: marked as current employee (not end-dated)
- `current_company`: what company they actually show
- `current_role`: their actual current title

**ZeroBounce** validates the email and returns:
- `status`: valid / catch-all / unknown / invalid
- `score`: 0–10 deliverability score

---

### Phase 3 — LLM Cross-Reasoning

#### Title Comparison (`_llm_compare_titles`)

When both a sheet title and a LinkedIn/TheOrg title exist, **GPT-4.1-mini** compares them semantically:

> "VP Marketing" ≈ "VP of Marketing" → **MATCH**
> "Head of Digital" ≈ "Chief Digital Officer" → **MATCH**
> "Head of Consumer and Shopper Planning SE" vs "CMO" → **MISMATCH**
> "Regional Key Account Manager" vs "Digital Media Manager" → **MISMATCH**

Rules given to the LLM:
- MATCH = same function/seniority regardless of exact wording
- MISMATCH = clearly different departments or functions
- UNKNOWN = insufficient information to judge

Fast word-overlap (50% threshold + `_is_different_function`) runs first. LLM only called when needed.

#### Cross-Reasoning (`_llm_cross_reason`)

Called when `employment == UNCERTAIN` or `identity == UNCONFIRMED`. Gives GPT-4.1-mini:
- Full LinkedIn audit result
- All web signal summaries
- Perplexity snippet
- Email status

Returns structured `IDENTITY`, `EMPLOYMENT`, `ROLE_MATCH`, and a one-sentence explanation.

---

### Phase 4 — Verdict Decision Tree

```
identity == UNCONFIRMED?
  ├─ email bad      → REJECT  "Identity unconfirmed and email invalid"
  └─ email ok       → REVIEW

employment == REJECTED?
  ├─ LinkedIn shows different company  → REJECT  "Now at [Company X]"
  └─ 2+ stale web signals             → REJECT  "Stale role"

employment == CONFIRMED?
  ├─ role_match == MISMATCH  → REJECT  "Title mismatch: sheet vs LinkedIn"
  ├─ email ok + role MATCH/UNKNOWN  → VERIFIED
  ├─ email bad               → REVIEW
  └─ fallback                → VERIFIED

employment == UNCERTAIN?
  ├─ identity confirmed + email ok   → REVIEW
  └─ fallback                        → REVIEW
```

**Key rule**: LinkedIn confirmed at company **but** title from a different department → **REJECT**. We don't want to send outreach to the wrong function just because the person is reachable.

---

## Sheet Column Layout

### Final Filtered List (cols A–U)

| Col | Field | Written by |
|-----|-------|-----------|
| A–N | Raw contact data (company, name, email, LinkedIn URL, etc.) | Fini / Searcher / Scout |
| O | LinkedIn Status | Veri |
| P | Employment Verified | Veri |
| Q | Title Match | Veri |
| R | Actual Title Found | Veri |
| S | Overall Status (VERIFIED / REVIEW) | Veri |
| T | Verification Notes | Veri |
| U | Verified On (date) | Veri |

### Rejected Profiles (cols A–U)

Same A–N raw data, then:

| Col | Field |
|-----|-------|
| O | LinkedIn Status |
| P | Employment Verified |
| Q | Title Match |
| R | Actual Title Found |
| S | **Reject Reason** (concise, one sentence) |
| T | Verification Notes (full evidence trail) |
| U | Verified On |

---

## Evidence Sources & Trust Hierarchy

```
1. Unipile LinkedIn (highest)  — direct profile data, real-time
2. ZeroBounce                  — email deliverability, real-time
3. TheOrg                      — org chart, near real-time
4. Perplexity                  — AI-synthesised web (2025-aware)
5. DuckDuckGo                  — raw web search snippets
6. Tavily                      — deep web search (fallback only)
7. LLM reasoning               — cross-signal synthesis (uncertain cases only)
```

---

## Concurrency Architecture

```
parallel_verify_all
  ├─ asyncio.Semaphore(6) — max 6 contacts verified simultaneously
  └─ asyncio.Lock (sheet_lock) — serialises all Google Sheets writes
       ├─ append_row(FINAL_FILTERED_LIST, ...)  ← VERIFIED / REVIEW
       └─ append_row(REJECTED_PROFILES, ...)    ← REJECT
```

Within each contact verification, Phase 1 searches run fully in parallel (DDG ×3 + Perplexity + TheOrg = 5 concurrent requests), and Phase 2 LinkedIn + ZeroBounce run in parallel.
