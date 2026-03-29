# AI Scout — Smart Lead Enrichment Agent: Upgrade Plan

## Current State

The Scout can find people but only writes `company_name + full_name + role_title` to First Clean List.
No email, no verified LinkedIn URL, no duplicate detection, no LangGraph reasoning.

---

## Target State

Scout becomes a **full LangGraph agent** that:
1. Understands the SDR's intent (find person, check person, add to sheet)
2. Checks both sheets for duplicates **before doing any work**
3. Finds LinkedIn URL and verifies it with Unipile
4. Builds + validates the email using company email format + ZeroBounce
5. Writes a **complete row (cols A–U)** to `Final Filtered List` — same as Veri output
6. Tells the SDR exactly what exists and what was added

---

## Architecture: LangGraph Agent Graph

```
query
  │
  ▼
[parse_intent]
  ├─ extracts: company_name, target_roles, intent (find/check/add)
  │
  ▼
[check_duplicates]  ◄── reads First Clean List + Final Filtered List
  ├─ if found: returns message "X already exists at row Y"
  │
  ▼
[research_parallel]  ◄── asyncio.gather 3 agents
  ├─ Perplexity:  web search for person + role at company
  ├─ TheOrg:      org chart lookup
  └─ Unipile:     LinkedIn people search by org_id + title variants
  │
  ▼
[verify_linkedin]
  ├─ for each candidate: verify_profile(linkedin_url)
  ├─ confirm employment at target company
  └─ extract actual title, employment status
  │
  ▼
[generate_email]
  ├─ look up company email format from Target Accounts sheet
  ├─ construct email: {first}.{last}@domain.com
  └─ ZeroBounce validate → include only valid/catch-all
  │
  ▼
[synthesize]
  ├─ Claude merges all data, deduplicates by name
  ├─ assigns confidence (high/medium/low)
  ├─ assigns buying_role (Decision Maker / Influencer)
  └─ produces final candidate list + SDR-friendly message
  │
  ▼
response → frontend (same shape as today + email + linkedin_verified + exists_in_sheet)
```

The **write to sheet** happens separately when the SDR clicks "+ Add to Sheet" — it calls a commit endpoint.

---

## Data Models

### ScoutState (LangGraph state TypedDict)

```python
class ScoutState(TypedDict):
    # Input
    query: str
    history: list[dict]
    # Parsed
    company_name: str
    target_roles: list[str]
    intent: str                       # "find" | "check" | "add"
    # Company context (from Target Accounts)
    company_domain: str | None
    email_format: str | None
    company_org_id: str | None
    # Research raw results
    perplexity_text: str
    theorg_results: list[dict]
    linkedin_raw: list[dict]
    # Per-candidate enriched
    candidates: list[ScoutCandidateFull]
    # Duplicate check
    existing_contacts: list[ExistingContact]
    # Output
    message: str
    error: str | None
```

### ScoutCandidateFull

```python
@dataclass
class ScoutCandidateFull:
    full_name: str
    first_name: str
    last_name: str
    role_title: str
    company: str
    linkedin_url: str
    linkedin_verified: bool
    linkedin_status: str              # CONFIRMED / UNCONFIRMED
    employment_verified: str          # CONFIRMED / UNCERTAIN / REJECTED
    title_match: str                  # MATCH / MISMATCH / UNKNOWN
    actual_title: str                 # from LinkedIn profile
    email: str
    email_status: str                 # valid / catch-all / unknown / invalid
    confidence: str                   # high / medium / low
    source: str                       # linkedin | web | theorg
    buying_role: str                  # Decision Maker | Influencer
    exists_in_sheet: bool
    sheet_name: str | None            # "First Clean List" | "Final Filtered List"
    sheet_row: int | None
    is_new: bool
```

### ExistingContact (duplicate match)

```python
@dataclass
class ExistingContact:
    full_name: str
    role_title: str
    company: str
    email: str
    sheet_name: str        # which sheet it was found in
    row_number: int
    overall_status: str    # if in Final Filtered List
```

---

## Node Specifications

### Node 1: `parse_intent`

**Input:** `query`, `history`
**Output:** `company_name`, `target_roles`, `intent`

Uses Claude (fast, small) to extract:
- Company name (handles "Meta", "Meta Platforms", "meta.com")
- Role(s) the SDR is asking about ("CMO", "VP Marketing", "Head of Digital")
- Intent:
  - `find` = "find me the CMO at Meta"
  - `check` = "does John Smith work at Meta?"
  - `add` = "add Jane Doe, VP Sales at Diageo to the sheet"

```python
# Prompt skeleton
PARSE_INTENT_PROMPT = """
Extract from the SDR's query:
1. company_name: the target company
2. target_roles: list of role titles / personas being requested
3. intent: "find" | "check" | "add"

Query: {query}
History context: {history_summary}

Return JSON: {"company_name": str, "target_roles": [...], "intent": str}
"""
```

---

### Node 2: `check_duplicates`

**Input:** `company_name`, `target_roles`
**Output:** `existing_contacts`

Reads both sheets in parallel:
```python
fcl, ffl = await asyncio.gather(
    sheets.read_all_records(sheets.FIRST_CLEAN_LIST),
    sheets.read_all_records(sheets.FINAL_FILTERED_LIST),
)
```

Match logic:
- Normalize names: lowercase, strip punctuation
- Company match: fuzzy (same as unipile.py's fuzzy logic)
- Name match: full_name OR (first_name + last_name)
- If match found → populate `ExistingContact` with row info

If duplicates found → Scout responds:
> "⚠ John Smith (VP Marketing at Meta) already exists in Final Filtered List (row 34, status: CONFIRMED). Want me to find someone else or update?"

This runs even if the user just asks to `find` — so the SDR always knows before adding.

---

### Node 3: `research_parallel`

**Input:** `company_name`, `target_roles`, `company_org_id`
**Output:** `perplexity_text`, `theorg_results`, `linkedin_raw`

Three async agents running in parallel (`asyncio.gather`):

**Agent A — Perplexity web search:**
```
"Current [target_roles] at [company_name] LinkedIn profile 2024 2025"
```
- Uses `sonar-pro` model (real-time web)
- Extracts: names, titles, LinkedIn URLs from citations

**Agent B — TheOrg lookup:**
```python
await theorg.search_company(company_name)
```
- Returns org chart entries with LinkedIn URLs
- Filter by role matching target_roles

**Agent C — Unipile LinkedIn search:**
```python
org_id = await unipile.get_company_org_id(company_name)
# Generate 6+ title variants with Claude
title_variants = await _gen_title_variants(target_roles)
people = await unipile.search_people(org_id, title_variants, limit=30)
```

---

### Node 4: `verify_linkedin`

**Input:** `linkedin_raw` (from Unipile), `perplexity_text`
**Output:** Each candidate updated with `linkedin_verified`, `employment_verified`, `actual_title`

For each unique candidate found across all research sources:
```python
if candidate.linkedin_url:
    profile = await unipile.verify_profile(candidate.linkedin_url)
    candidate.linkedin_verified = profile.is_valid
    candidate.employment_verified = "CONFIRMED" if profile.current_company_match else "UNCERTAIN"
    candidate.actual_title = profile.current_title
    candidate.linkedin_status = "CONFIRMED" if profile.is_valid else "UNCONFIRMED"
```

Run up to 5 verifications concurrently (Unipile rate limit aware).

---

### Node 5: `generate_email`

**Input:** `candidates`, `company_name`, `email_format`, `company_domain`
**Output:** Each candidate updated with `email`, `email_status`

Step 1: Look up email format from Target Accounts sheet
```python
accounts = await sheets.read_all_records(sheets.TARGET_ACCOUNTS)
row = next((r for r in accounts if fuzzy_match(r["Company Name"], company_name)), None)
email_format = row["Email Format"] if row else None
company_domain = row["Company Domain"] if row else None
```

Step 2: Construct email from format template
```python
# Format examples: "{first}.{last}@domain.com", "{f}{last}@domain.com"
email = _apply_email_format(email_format, first_name, last_name, company_domain)
```

Format parser handles:
- `{first}` → full first name
- `{last}` → full last name
- `{f}` → first initial
- `{l}` → last initial
- `{first}.{last}`, `{f}{last}`, etc.

Step 3: ZeroBounce validate
```python
result = await zerobounce.validate_email(email)
candidate.email = email if result["status"] in ("valid", "catch-all") else ""
candidate.email_status = result["status"]
```

If no email format in Target Accounts: try common patterns ({first}.{last}, {first}{last}) and pick the one ZeroBounce validates as `valid`.

---

### Node 6: `synthesize`

**Input:** All research + enrichment results
**Output:** Final `candidates` list, `message`

Claude synthesis:
- Deduplicates candidates by normalized name
- Preferring LinkedIn-verified over web-only
- Assigns confidence:
  - `high` = LinkedIn verified + email valid + employment confirmed
  - `medium` = LinkedIn verified OR email valid (not both)
  - `low` = web-only, no verification
- Assigns `buying_role`:
  - `Decision Maker` = C-suite, VP, SVP, EVP, President, Director, Head of
  - `Influencer` = Manager, Specialist, Analyst, Associate
- Writes SDR-friendly message including:
  - Duplicate warnings (if any)
  - What was found and confidence
  - Next steps suggestion

---

## Sheet Write: Where and What

### Target: `Final Filtered List` (A–U)

Scout writes **directly to Final Filtered List** — same columns as Veri output.
No need to go through Fini → n8n → First Clean List → Veri pipeline.

```python
async def scout_commit(candidate: ScoutCandidateFull) -> int:
    row = [
        candidate.company,                    # A  Company Name
        candidate.company,                    # B  Normalized Company Name
        company_domain,                       # C  Company Domain
        account_type,                         # D  Account Type (from Target Accounts)
        account_size,                         # E  Account Size
        country,                              # F  Country
        candidate.first_name,                 # G  First Name
        candidate.last_name,                  # H  Last Name
        candidate.role_title,                 # I  Job Title
        candidate.buying_role,                # J  Buying Role
        candidate.linkedin_url,               # K  LinkedIn URL
        candidate.email,                      # L  Email
        "",                                   # M  Phone-1 (Scout doesn't get phone)
        "",                                   # N  Phone-2
        candidate.linkedin_status,            # O  LinkedIn Status
        candidate.employment_verified,        # P  Employment Verified
        candidate.title_match,                # Q  Title Match
        candidate.actual_title,               # R  Actual Title Found
        "VERIFIED" if candidate.confidence == "high" else "REVIEW",  # S  Overall Status
        f"Scout AI · {candidate.source} · confidence={candidate.confidence}",  # T  Notes
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),  # U  Verified On
    ]
    row_num = await sheets.append_row(sheets.FINAL_FILTERED_LIST, row)
    return row_num
```

### Duplicate Check Before Write

Before writing, re-check sheet in real-time (not cached):
```python
existing = await check_person_exists(candidate.full_name, candidate.company)
if existing:
    return {"status": "duplicate", "sheet": existing.sheet_name, "row": existing.row_number}
```

---

## Frontend Changes

### Scout Candidate Card — New Fields

```typescript
interface ScoutCandidate {
  full_name: string;
  first_name: string;       // NEW
  last_name: string;        // NEW
  role_title: string;
  company: string;
  linkedin_url: string;
  linkedin_verified: boolean;
  linkedin_status: string;  // NEW  CONFIRMED / UNCONFIRMED
  email: string;            // NEW
  email_status: string;     // NEW  valid / catch-all / unknown / invalid
  buying_role: string;      // NEW  Decision Maker / Influencer
  confidence: string;
  source: string;
  exists_in_sheet: boolean; // NEW
  sheet_name?: string;      // NEW  if exists
  sheet_row?: number;       // NEW  if exists
  added?: boolean;
  sendStatus?: 'idle' | 'sending' | 'sent' | 'error' | 'duplicate';
}
```

### Card UI Updates

Each Scout candidate card should show:
1. Avatar + name + title + company (same as now)
2. **LinkedIn badge**: `✓ Verified` (emerald) or `Unverified` (white/dim)
3. **Email row**: show constructed email + status dot (green/amber/red)
4. **Buying Role badge**: `DM` (decision maker, violet) or `Inf` (influencer, slate)
5. **Exists in sheet warning**: amber banner "⚠ Already in [sheet name], row [N]"
6. **+ Add button**: disabled if `exists_in_sheet`, shows "Exists" instead

### Scout Response Bubble

SDR message and AI response include:
- Duplicate warnings at the top (before candidates)
- Progress hints: "Checking sheets... Searching LinkedIn... Validating emails..."

---

## API Changes

### `/api/searcher/prospect-chat` (upgrade existing)

Replace current 3-agent parallel with LangGraph graph execution.

**Request** (unchanged):
```python
class ProspectChatRequest(BaseModel):
    query: str
    company: str = ""
    history: list[dict] = []
```

**Response** (expanded):
```python
{
  "message": "Found 2 contacts at Meta...",
  "candidates": [ScoutCandidateFull, ...],  # full enriched
  "duplicates": [ExistingContact, ...],      # NEW — already in sheets
}
```

### `/api/searcher/scout-commit` (upgrade existing)

**Request** (expanded):
```python
class ScoutCommitRequest(BaseModel):
    full_name: str
    first_name: str = ""          # NEW
    last_name: str = ""           # NEW
    role_title: str
    company: str
    linkedin_url: str = ""
    linkedin_verified: bool = False
    linkedin_status: str = ""     # NEW
    employment_verified: str = "" # NEW
    title_match: str = ""         # NEW
    actual_title: str = ""        # NEW
    email: str = ""               # NEW
    email_status: str = ""        # NEW
    buying_role: str = ""         # NEW
    confidence: str = "low"
    source: str = "scout"
```

**Response**:
```python
{
  "status": "ok" | "duplicate",
  "row": int,
  "sheet": "Final Filtered List",
  "duplicate_info": ExistingContact | None  # if duplicate found at commit time
}
```

---

## Implementation Steps (Ordered)

### Phase 1 — Duplicate Detection (1-2 hours)
1. Create `backend/agents/scout.py`
2. Implement `check_person_exists(name, company)` — reads both sheets
3. Add duplicate check to `/api/searcher/prospect-chat` response
4. Frontend: show "⚠ Already in sheet" banner on candidate cards

### Phase 2 — Full LangGraph Agent (2-3 hours)
5. Define `ScoutState` TypedDict and `ScoutCandidateFull` dataclass
6. Implement all 6 nodes as async functions
7. Wire graph with `StateGraph` from LangGraph
8. Replace the current 3-agent parallel block in `api.py` with graph invocation
9. Add email format + domain lookup from Target Accounts

### Phase 3 — Sheet Write Upgrade (1 hour)
10. Upgrade `scout_commit` to write all A–U columns to Final Filtered List
11. Add pre-write duplicate check
12. Return `{status: "duplicate", ...}` if person already exists

### Phase 4 — Frontend Updates (1-2 hours)
13. Update `ScoutCandidate` interface in `stores.ts`
14. Update candidate card to show email, LinkedIn status, buying role
15. Handle `exists_in_sheet` → show warning, disable Add button
16. Handle `sendStatus: 'duplicate'` in commit response

---

## Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Where to write | Final Filtered List (A–U) | Scout already verifies, no need for Fini → n8n → Veri pipeline |
| Duplicate check timing | At research time + at commit time | SDR sees warning during conversation, not blocked at write |
| Email validation | ZeroBounce for valid/catch-all, skip invalid | Don't write bad emails to CRM |
| LinkedIn verification | Unipile verify_profile | Same tool Veri uses, consistent confidence |
| LangGraph vs sequential | LangGraph StateGraph | Easier to add/modify nodes, better error handling per node |
| Email format source | Target Accounts sheet | Fini already collected this, reuse it |
| Confidence scoring | high = LinkedIn + email both verified | Clear definition, not subjective |

---

## Files to Create/Modify

| File | Action | What |
|------|--------|-------|
| `backend/agents/scout.py` | **CREATE** | LangGraph agent with all 6 nodes |
| `backend/api.py` | **MODIFY** | Replace prospect-chat logic, upgrade scout-commit |
| `src/lib/stores.ts` | **MODIFY** | Expand ScoutCandidate interface |
| `src/app/searcher/page.tsx` | **MODIFY** | Update card UI, handle new fields |

---

## Example SDR Interaction (Target)

**SDR:** "find the CMO at Diageo UK"

**Scout:**
> Searching LinkedIn and web... Checking if already in sheets...
>
> ⚠ Found 1 existing: **Patricia Corsi** (Chief Marketing Officer, Diageo) is already in Final Filtered List (row 89, status: CONFIRMED).
>
> Also found a new contact:
> **[James Thompson]** · VP Brand Marketing · Diageo UK
> ✓ LinkedIn Verified · Email: j.thompson@diageo.com (valid) · DM
> Confidence: HIGH

**SDR:** "+ Add" → writes full row to Final Filtered List instantly.

---

## What This Unlocks for SDRs

1. **No duplicate CRM entries** — Scout warns before adding
2. **Email ready to send** — validated, not guessed
3. **LinkedIn confirmed** — not a stale web result
4. **Buying role pre-classified** — SDR knows DM vs influencer
5. **Full audit trail** — sheet row O-U shows how Scout found and verified

---

*Plan written: 2026-03-29 · Ready for implementation*
