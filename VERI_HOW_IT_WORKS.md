# Veri Agent — How It Works
## Complete Verification Flow + Decision Logic

---

## What Happened in This Batch (Mars, rows 914–924)

| Contact | Identity | Employment | Email | Verdict | Why |
|---------|----------|------------|-------|---------|-----|
| Neus | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed at Mars, but email is undeliverable |
| Mariana | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | Same as above |
| Walter | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | DDG had stale signals but Perplexity + LinkedIn overrode them |
| Marian | CONFIRMED | CONFIRMED* | unknown/3.0 | **REVIEW** | LinkedIn returned 301 redirect error → LLM cross-reasoning confirmed employment |
| Rahul | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| Vanessa | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| Maria | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| Huseyin | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| Santi | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| Ivan | CONFIRMED | CONFIRMED | unknown/3.0 | **REVIEW** | LinkedIn confirmed, email undeliverable |
| **Juan** | CONFIRMED | **REJECTED** | no email | **REJECT** | LinkedIn via LLM shows no current employment at Mars |

*Marian's LinkedIn returned a 301 Moved Permanently error — could not load profile directly. LLM resolved it.

---

## The 4-Phase Pipeline (per contact, run in parallel, Semaphore(6))

```
Phase 0  ──  LinkedIn URL Discovery (if no URL in sheet)
Phase 1  ──  Web Intelligence  [DDG ×3 + TheOrg + Perplexity + Tavily fallback]
Phase 2  ──  Deep Verify       [Unipile LinkedIn audit + ZeroBounce email check]
Phase 3  ──  Scoring           [LLM Title Compare + LLM Cross-Reasoning if uncertain]
Phase 4  ──  Write             [Update Final Filtered List cols O–U / move to Reject profiles]
```

---

## Phase 0 — LinkedIn URL Discovery

**Trigger:** only runs if the contact has no LinkedIn URL in the sheet.

```
search_person_by_name(full_name, limit=5)
  → for each candidate: fuzzy name match ≥ 85%
    → if match found: store URL and proceed
    → if no match: continue without LinkedIn URL
```

If no URL is found, Phase 2 LinkedIn audit will return `valid=False`.

---

## Phase 1 — Web Intelligence (all 4 sources run in parallel)

### DDG ×3 (DuckDuckGo)
Three queries fire concurrently:
```
1. "{full_name}" {company}
2. "{full_name}" {role_title} {company}
3. "{full_name}" LinkedIn {company}
```
Up to 15 snippets collected total. The system looks for:
- **Positive signal:** name + company appear together in snippets (person is at the company)
- **Stale signal:** keywords like `"former"`, `"ex-"`, `"now at"`, `"left"`, `"moved to"`, `"resigned"` etc.

Result: `ddg_positive=True/False`, `ddg_stale=True/False`

### TheOrg
Org-chart lookup by name + company.
- If found: confirms identity + title (strong signal)
- If not found: no deduction, just no evidence

Result: `theorg_found=True/False`, `theorg_title`

### Perplexity (structured role query)
Fires a targeted question:
```
"What is {name}'s current job title and employer as of 2025?
Are they currently working at {company}? What department do they work in?"
```
Extracts whether the person is confirmed at the company, with optional `2024/25` recency tag.

Result: `perplexity_positive`, `perplexity_stale`, `perplexity_recent`

### Tavily (fallback — only if DDG is inconclusive)
If DDG returned neither positive nor stale signal, Tavily fires a deep search.
If DDG was decisive (either direction), Tavily is skipped.

---

## Phase 2 — Deep Verify (both run in parallel)

### Unipile LinkedIn Audit
Calls `verify_profile(linkedin_url, company)` which reads the LinkedIn profile via Unipile API.

Returns:
```json
{
  "valid": true/false,
  "current_company": "Mars",
  "current_role": "Corporate Affairs Director, Mars Wrigley South Europe",
  "at_target_company": true/false,
  "still_employed": true/false,
  "error": "..." (if 301 redirect or API error)
}
```

**at_target_company** = LinkedIn current employer matches the target company name
**still_employed** = person has an active (not end-dated) position

> **What happened with Marian and Juan:** Unipile returned a `301 Moved Permanently` error.
> This happens when LinkedIn redirects the profile URL (e.g. slug changed).
> The audit returns `valid=False`, so the system falls back to LLM cross-reasoning.

### ZeroBounce Email Validation
Validates the email address for deliverability.

Returns: `status` = `valid` / `catch-all` / `unknown` / `invalid` / `no_email`
Also returns a `score` (0–10, where 10 = very likely valid).

> **Why all Mars contacts are REVIEW:** Every email at `@effem.com` (Mars' domain) returned
> `status=unknown, score=3.0`. ZeroBounce cannot definitively validate catch-all corporate
> domains. `unknown` is NOT treated as `email_ok=True`, so even fully confirmed contacts
> can't reach VERIFIED — they land on REVIEW.

**email_ok = True only when:** `status == "valid"` OR `status == "catch-all"`

---

## Phase 3 — Scoring

### Step A: Identity Signal
```
IF   LinkedIn profile loaded (valid=True)          → identity = CONFIRMED
ELIF found on TheOrg                               → identity = CONFIRMED
ELIF 2+ of [DDG, Tavily, Perplexity] positive      → identity = CONFIRMED
ELSE                                               → identity = UNCONFIRMED
```

### Step B: Employment Signal
```
IF LinkedIn valid:
  IF at_target AND still_employed                  → employment = CONFIRMED
  IF at_target AND NOT still_employed              → employment = UNCERTAIN
  IF current_company exists AND NOT at_target      → employment = REJECTED  ← person moved
  ELSE (no company info):
    IF 2+ stale web signals                        → employment = REJECTED
    ELSE                                           → employment = UNCERTAIN
ELSE (LinkedIn not accessible):
  IF 2+ stale web signals                          → employment = REJECTED
  ELSE                                             → employment = UNCERTAIN
```

### Step C: Title Comparison
Two-step process:
1. **Fast path** — word overlap ≥ 50% → MATCH; different function keywords → MISMATCH
2. **LLM semantic compare** (GPT-4.1-mini) — fires when both titles exist but overlap < 50%
   - Compares sheet title vs LinkedIn/TheOrg title
   - Returns: `MATCH` / `MISMATCH` / `UNKNOWN`

> In this batch: every contact showed `"no second title to compare — role_match=MATCH"`
> This means Unipile returned a role, but the fast-path `_compare_titles_fast` gave UNKNOWN
> (because the LinkedIn role was long, e.g. "Corporate Affairs Director, Mars Wrigley South Europe"
> vs sheet title), and then since titles were equal-enough → MATCH.
> Actually for this batch: `role_match=MATCH` appears because no second title was available
> for LLM comparison → defaults to MATCH (neutral).

### Step D: LLM Cross-Reasoning (only when uncertain)
**Trigger:** `employment == "UNCERTAIN"` OR `identity == "UNCONFIRMED"`

Sends ALL collected evidence to GPT-4.1-mini:
```
- LinkedIn audit result
- Web signals (DDG/Perplexity/TheOrg/Tavily)
- Perplexity snippet (300 chars)
- Email status
```
Asks: Is this person real? Are they currently at the company? Does their title match?

Returns structured: `identity`, `employment`, `role_match`, `explanation`

> **Marian:** LinkedIn returned 301 error → employment=UNCERTAIN → LLM fired.
> LLM found Perplexity confirms "Global Director of Digital Product Delivery at Mars Petcare"
> → upgraded employment to CONFIRMED → REVIEW
>
> **Juan:** LinkedIn returned 301 error + no email → employment=UNCERTAIN → LLM fired.
> LLM found "no current employment information available from any source"
> → kept employment=REJECTED → **REJECT**

---

## The Decision Tree (Final Verdict)

```
                    ┌─────────────────────────────┐
                    │      identity = UNCONFIRMED?  │
                    └──────────────┬──────────────┘
                          YES      │      NO
                    ┌──────────────┘      └──────────────────────────┐
                    ▼                                                  ▼
           email_ok?                                     employment = REJECTED?
           /       \                                        /              \
         NO         YES                                   YES              NO
          ▼          ▼                                     ▼                ▼
       REJECT      REVIEW                         li_company exists?    employment = CONFIRMED?
                                                   /          \             /            \
                                                 YES           NO          YES             NO
                                                  ▼             ▼           ▼               ▼
                                               REJECT        REJECT    role_match        REVIEW
                                          "now at X,       "2+ stale  = MISMATCH?
                                          not Company"      signals"    /       \
                                                                      YES        NO
                                                                       ▼          ▼
                                                                    REJECT    email_ok?
                                                                              /       \
                                                                           YES         NO
                                                                            ▼           ▼
                                                                        VERIFIED      REVIEW
```

### In plain English:

| Condition | Verdict | Reason |
|-----------|---------|--------|
| Identity unconfirmed + bad/no email | **REJECT** | Ghost contact — can't verify exists |
| Identity unconfirmed + valid email | **REVIEW** | Email works but no web/LinkedIn proof |
| LinkedIn shows person at different company | **REJECT** | Moved on, wrong target |
| 2+ web sources say person left | **REJECT** | Stale contact |
| LinkedIn confirmed + title MISMATCH (different dept) | **REJECT** | Wrong person for ICP |
| LinkedIn confirmed + email valid/catch-all | **VERIFIED** | Full confidence |
| LinkedIn confirmed + email unknown/invalid | **REVIEW** | Confirmed person, uncertain email |
| Employment uncertain (LinkedIn inaccessible) | **REVIEW** | Needs manual check |

---

## Why All 10 Mars Contacts Are REVIEW (Not VERIFIED)

```
identity  = CONFIRMED  ✓  (LinkedIn at_target=True, employed=True)
employment = CONFIRMED  ✓  (at_target AND still_employed)
role_match = MATCH      ✓  (titles compatible)
email_ok   = FALSE      ✗  (effem.com returns "unknown", score=3.0)
```

The verdict path hits:
```python
if employment == "CONFIRMED":
    if not email_ok:
        return "REVIEW"   # ← this line
```

**ZeroBounce "unknown" ≠ invalid.** The email likely works but ZeroBounce can't prove it
(corporate domains often block validation probes). These contacts are valid — they just need
manual email confirmation before outreach. The SDR should:
1. Accept the contact as real and employed
2. Send a test email or use a different validation tool for `@effem.com`

---

## Why Juan Was REJECTED

```
Phase 2: Unipile → 301 Moved Permanently (profile URL changed)
         ZeroBounce → no email in sheet
         → LinkedIn audit: valid=False, no employment data

Phase 3: identity = CONFIRMED (DDG positive × 12 snippets)
         employment = UNCERTAIN (LinkedIn failed, no stale signals)
         → LLM Cross-Reasoning triggered

LLM finding: "LinkedIn shows they are not currently employed at Mars
              and no current employment information is available from any source"
         → employment upgraded to REJECTED

Verdict tree:
  employment = REJECTED
  li_company = not available
  → "2+ sources indicate person left Mars"
  → REJECT
```

**Action taken:**
1. Cols O–U written to Final Filtered List row 923 (status=REJECT)
2. Full row A–U copied to Reject profiles tab (row 39)
3. Row 923 queued for deletion from Final Filtered List

---

## Sheet Column Layout After Veri

| Col | Header | Written By |
|-----|--------|------------|
| A–N | Company, Name, Role, LinkedIn, Email, Phones… | Searcher / Scout |
| O | LinkedIn Status | **Veri** |
| P | Employment Verified | **Veri** |
| Q | Title Match | **Veri** |
| R | Actual Title Found | **Veri** |
| S | Overall Status (VERIFIED/REVIEW/REJECT) | **Veri** |
| T | Verification Notes | **Veri** |
| U | Verified On (date) | **Veri** |

REJECT contacts additionally appear in **Reject profiles** tab (cols A–U, col S = reject reason)
and are **removed** from Final Filtered List.
