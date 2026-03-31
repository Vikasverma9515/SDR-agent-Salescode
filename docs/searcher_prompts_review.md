# Searcher Agent — LLM Prompts Review

**File:** `backend/agents/searcher.py`
**Agent:** Searcher (Agent 3) — Contact Gap-Fill
**Purpose:** Runs after Veri. Identifies companies missing Decision Maker contacts, then discovers and appends new DM contacts.
**Reviewer:** _______________________
**Review Date:** _______________________
**Status:** [ ] Approved &nbsp; [ ] Changes Requested &nbsp; [ ] Needs Discussion

---

## Overview

The Searcher agent uses **7 LLM prompts** across its pipeline. Two call `llm_web_search` (live web grounding), five call `llm_complete` (pure LLM inference). All prompts are designed to return structured output (JSON arrays or plain strings) — no free-form prose.

| # | Prompt Name | Function | LLM Call Type | Returns |
|---|---|---|---|---|
| 1 | Role Expansion | `_expand_dm_roles_for_region()` | `llm_complete` | JSON array of strings |
| 2 | Importance Notes & Priority Score | `_add_importance_notes()` | `llm_complete` | JSON array of objects |
| 3 | Find More — Role Title Generation | `_find_more_agents()` → Agent 1 | `llm_complete` | JSON array of strings |
| 4 | Must-Have Tier Web Search | `_search_must_have_tiers_web()` | `llm_web_search` | Plain string `Name — Title` |
| 5 | Company Website Fallback | `_fetch_leadership_via_llm()` | `llm_web_search` | Plain list, one per line |
| 6 | Department Priority Ranking | `_rank_buckets_by_llm()` | `llm_complete` | JSON array of objects |
| 7 | Email Format Discovery | `_discover_email_format_llm()` | `llm_web_search` | Single pattern string |

---

## Prompt 1 — Role Expansion

**Function:** `_expand_dm_roles_for_region()`
**Location:** [searcher.py:424](../backend/agents/searcher.py#L424)
**Triggered:** At the start of `unipile_search` — before LinkedIn queries are built
**Purpose:** Converts English role targets (e.g. "Head of Sales") into multilingual LinkedIn title variants for the company's region, so searches don't miss local-language profiles.

**Model:** `llm_complete` — `temperature=0`, `max_tokens=800`

### Full Prompt (as sent to LLM)

```
You are helping find decision-makers at companies in {region} (language: {language}).

Target roles: {roles}

Generate a comprehensive list of equivalent LinkedIn job title strings to search for.
Include:
- English titles as-is
- {language} equivalents (exact local-language titles used on LinkedIn profiles in {region})
- Seniority synonyms (VP ↔ Director ↔ Head of ↔ Managing Director)
- C-suite equivalents (CMO, CDO, CTO, CRO, CCO)

Rules:
- SENIOR roles only: C-suite, VP, Director, Head of, Country/Regional Manager, General Manager
- NO junior titles: coordinator, analyst, specialist, associate, junior manager
- Max 35 titles total
- Return ONLY a JSON array: ["title1", "title2", ...]
- No explanation, no markdown fences.
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{region}` | `state.target_region` | `"Spain"` |
| `{language}` | Derived from region via `language_map` dict | `"Spanish"` |
| `{roles}` | `state.dm_roles` (from Target Accounts sheet) | `["Head of Ecommerce", "Marketing Director"]` |

### Expected Output

```json
["Head of Ecommerce", "Director de Ecommerce", "Responsable de Ecommerce",
 "VP Marketing", "Director de Marketing", "CMO", "Directeur Marketing", ...]
```

### Notes / Review Points

- [ ] Max 35 titles is hardcoded — is this enough for large multinationals with many sub-regions?
- [ ] Falls back silently to the original English roles if LLM fails or returns no valid array
- [ ] No validation that the returned titles are actually senior — the rules are instructions only

---

## Prompt 2 — Importance Notes & Priority Score

**Function:** `_add_importance_notes()`
**Location:** [searcher.py:575](../backend/agents/searcher.py#L575)
**Triggered:** After contacts are discovered, before they are shown to the SDR for selection
**Purpose:** Assigns each contact a one-sentence sales rationale and a 1–100 priority score based on seniority and buying authority.

**Model:** `llm_complete` — `temperature=0`, `max_tokens=1500`

### Full Prompt (as sent to LLM)

```
Company: {company}
We sell digital commerce / ecommerce technology. Looking for: {roles_str}

People found at {company}:
1. {full_name} — {role_title or "Unknown Role"}
2. {full_name} — {role_title or "Unknown Role"}
...

For each person, write ONE sentence (max 15 words) explaining why they matter
for our sales — focus on budget authority or decision-making power.
ALSO assign a priority_score (1-100) based on their seniority and decision-making power
(e.g. C-level=90-100, VP=70-90, Manager=40-70).

Return ONLY a JSON array:
[{"index": 0, "note": "...", "priority_score": 85}, ...]
No markdown, no explanation.
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{company}` | `state.target_normalized_name or state.target_company` | `"Inditex"` |
| `{roles_str}` | First 6 of `state.dm_roles` joined by `, ` | `"CMO, Head of Ecommerce, VP Digital"` |
| Contact list | All `contacts` passed into function | `"1. Ana García — Chief Marketing Officer"` |

### Expected Output

```json
[
  {"index": 0, "note": "Controls all digital marketing budget and agency relationships.", "priority_score": 95},
  {"index": 1, "note": "Owns ecommerce P&L and platform vendor decisions.", "priority_score": 88}
]
```

### Notes / Review Points

- [ ] Role description is hardcoded to "digital commerce / ecommerce technology" — should this be configurable per campaign?
- [ ] Priority score thresholds (C-level=90–100, VP=70–90, Manager=40–70) are given as examples in-prompt, not enforced post-response. LLM could return arbitrary values.
- [ ] Falls back silently — if LLM fails, contacts are returned without notes or scores (score defaults to 0)
- [ ] Max 15 words per note is instructional only — not validated post-parse

---

## Prompt 3 — Find More: Role Title Generation

**Function:** `_find_more_agents()` → internal `_unipile_agent()`
**Location:** [searcher.py:789](../backend/agents/searcher.py#L789)
**Triggered:** When SDR clicks "Find More" and types a freeform prompt (e.g. "find me the head of logistics")
**Purpose:** Converts the SDR's freeform request into structured LinkedIn job title search strings, including adjacent/adjacent-value roles.

**Model:** `llm_complete` — `temperature=0`, `max_tokens=300`

### Full Prompt (as sent to LLM)

```
Company: {company_name}
SDR request: "{prompt}"

Act as an expert SDR Brain. We need to map all prominent decision-makers related to this request.
1. Generate 4-6 specific LinkedIn job title search strings based strictly on the request.
2. Suggest 4-6 ADJACENT, highly valuable roles (e.g., if they asked for 'Sales', include 'Marketing' or 'Ops' or 'Growth').
Target ONLY non-entry roles (exclude interns/students).
Include local-language variants if the company is in a non-English country.
Return ONLY a JSON array of 8-12 combined string titles: ["title1", "title2", ...]
No explanation.
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{company_name}` | `state.target_normalized_name or state.target_company` | `"Zalando"` |
| `{prompt}` | SDR's live input text from the UI | `"find the head of logistics and supply chain"` |

### Expected Output

```json
["Head of Logistics", "Supply Chain Director", "VP Operations", "COO",
 "Chief Operating Officer", "Head of Fulfillment", "Operations Director",
 "Director de Logística", "Head of Supply Chain", "VP Logistics"]
```

### Notes / Review Points

- [ ] The `{prompt}` is raw SDR freetext — no sanitisation before it enters the LLM call. Consider trimming and capping length.
- [ ] "ADJACENT roles" instruction is subjective. LLM may hallucinate roles with no relationship to the request.
- [ ] "non-English country" detection relies on the LLM guessing from the company name alone — no region context injected here
- [ ] Max 300 tokens might be tight for 8–12 titles with local-language variants
- [ ] Results are fed directly into Unipile search without any post-filter for seniority

---

## Prompt 4 — Must-Have Tier Web Search

**Function:** `_search_must_have_tiers_web()`
**Location:** [searcher.py:1149](../backend/agents/searcher.py#L1149)
**Triggered:** During gap analysis, when a "must-have" tier (CEO/CTO/CSO) is missing and LinkedIn returned nothing
**Purpose:** Uses live web search to find the named individual holding a specific C-suite title at the target company.

**Model:** `llm_web_search` — live search grounding

### Full Prompt (as sent to LLM)

```
Who is the {search_title} of {company_name}?
If the exact title doesn't exist, find the closest equivalent
(e.g. Managing Director instead of CEO, VP Engineering instead of CTO).
Return ONLY: Full Name — Exact Title. Nothing else.
If unknown, return: unknown
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{search_title}` | First `search_queries` entry for the missing tier (e.g. `"CEO"`, `"CTO"`, `"Head of Sales"`) | `"CEO"` |
| `{company_name}` | `state.target_normalized_name or state.target_company` | `"L'Oréal"` |

### Expected Output

```
Sophie Bernard — Chief Executive Officer
```
or
```
unknown
```

### Notes / Review Points

- [ ] The response parser splits on `—`, `–`, or ` - ` with `maxsplit=1`. If the LLM returns `"Name - Title - Extra Info"`, the title absorbs the trailing text.
- [ ] Basic name validation: length 3–60 chars, excludes words like "the", "is", "was", "company", "unknown". This could still pass corporate phrases as names.
- [ ] No LinkedIn URL returned from this path — contact is created without `linkedin_url`, so it will need enrichment later
- [ ] Called once per missing tier per company — could be 3 web search calls per company if all tiers are missing

---

## Prompt 5 — Company Website Fallback

**Function:** `_fetch_leadership_via_llm()`
**Location:** [searcher.py:1733](../backend/agents/searcher.py#L1733)
**Triggered:** Inside `search_company_website()` when `httpx` scraping returns 403 or a JS-rendered shell
**Purpose:** Uses live web search grounded on the company's official domain to retrieve the current leadership/board list.

**Model:** `llm_web_search` — live search grounding, `timeout=20s`

### Full Prompt (as sent to LLM)

```
Find the CURRENT board of directors and leadership team for {company_name}
from their official website {domain}.
List only current members — ignore anyone who has resigned or retired.
Return ONLY a plain list, one per line: Full Name — Job Title.
No explanation, no markdown, no numbering, no citation brackets.
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{company_name}` | `state.target_normalized_name or state.target_company` | `"Mango"` |
| `{domain}` | `state.target_domain` | `"mango.com"` |

### Expected Output

```
Isak Andic — Founder & President
Daniel López — CEO
Mariona Baliu — CFO
```

### Notes / Review Points

- [ ] "official website {domain}" does not guarantee the LLM searches that domain specifically — it can pull from LinkedIn, Wikipedia, etc.
- [ ] Citation bracket stripping (`[1]`, `[2]`) is handled, but numbered list prefix stripping (`1.`, `2.`) relies on `lstrip('-•*')` which won't catch `"1. Name"` — these would fail the `—`/`-` split and be silently dropped
- [ ] The `_looks_like_name()` helper provides additional safety, but its logic is not visible in this review
- [ ] 20-second hard timeout — if web search is slow, all contacts from this source are lost silently
- [ ] Provenance is set to `"company_website_llm"` — useful for filtering and auditing

---

## Prompt 6 — Department Priority Ranking

**Function:** `_rank_buckets_by_llm()`
**Location:** [searcher.py:2183](../backend/agents/searcher.py#L2183)
**Triggered:** Inside `group_into_role_buckets()` after contacts are grouped by functional department
**Purpose:** Asks the LLM to rank the discovered functional departments (e.g. Marketing, Digital, Sales, Operations) by relevance as SDR outreach targets, using knowledge of the specific company.

**Model:** `llm_complete` — `temperature=0.0`

### Full Prompt (as sent to LLM)

```
You are a B2B sales expert. For the company "{company_name}", rank these departments
by how important they are as SDR outreach targets (budget control, buying authority, deal relevance).

Departments:
- id="marketing" label="Marketing" (3 people) e.g. CMO, VP Marketing
- id="digital" label="Digital" (2 people) e.g. Chief Digital Officer, Head of Digital
- id="sales" label="Sales" (1 people) e.g. VP Sales
...

Return ONLY a JSON array sorted by priority (most important first), no markdown:
[{"id": "bucket_id", "priority_rank": 1, "priority_reason": "6-8 word reason"}, ...]
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{company_name}` | `state.target_normalized_name or state.target_company` | `"Carrefour"` |
| Bucket lines | Built from `role_buckets` list in state — each bucket has `id`, `label`, `count`, `sample_roles[:2]` | See prompt example above |

### Expected Output

```json
[
  {"id": "digital", "priority_rank": 1, "priority_reason": "Controls digital platform and vendor budget"},
  {"id": "marketing", "priority_rank": 2, "priority_reason": "Drives ecommerce growth and spend"},
  {"id": "sales", "priority_rank": 3, "priority_reason": "Key decision-maker for commerce tools"}
]
```

### Notes / Review Points

- [ ] Markdown fence stripping is handled (```` ``` ```` and ` ```json `). Good defensive parsing.
- [ ] If the LLM returns a `priority_rank` not matching any bucket `id`, that bucket silently falls to rank 99
- [ ] "6-8 word reason" is instructional only. LLM may return longer or shorter reasons with no enforcement.
- [ ] Falls back gracefully — sequential ranks (0, 1, 2, ...) assigned if LLM call fails
- [ ] Ranking is company-aware (uses `company_name`) but has no industry or product context injected — the LLM relies on world knowledge of the company

---

## Prompt 7 — Email Format Discovery

**Function:** `_discover_email_format_llm()`
**Location:** [searcher.py:2589](../backend/agents/searcher.py#L2589)
**Triggered:** Inside `enrich_contacts()` when no email format is found in the Target Accounts sheet and no existing emails can be reverse-engineered
**Purpose:** Uses live web search to determine the standard employee email format for a company domain (e.g. `{first}.{last}`, `{first_initial}{last}`).

**Model:** `llm_web_search` — live search grounding

### Full Prompt (as sent to LLM)

```
Find the email format used by employees at {company} (domain: {domain}).
Search for real employee emails — LinkedIn, email finders, press releases, PDFs.
Pick EXACTLY ONE pattern from this list:
- {first}.{last}
- {first_initial}{last}
- {first}
- {last}
- {first_initial}.{last}
- {first}{last}
- {first_name}.{last_name}
- {first_name}{last_name}
Or return 'unknown' if you cannot determine it with confidence.
Reply with ONLY the pattern, nothing else.
```

### Variable Substitutions

| Variable | Source | Example |
|---|---|---|
| `{company}` | Company name from enrichment context | `"LVMH"` |
| `{domain}` | `state.target_domain` | `"lvmh.com"` |
| Pattern list | `_EMAIL_PATTERNS` constant from `backend/tools/domain_discovery.py` | See above |

### Expected Output

```
{first}.{last}
```

### Notes / Review Points

- [ ] The `_EMAIL_PATTERNS` list is imported from another module — if it changes there, the prompt automatically reflects the updated list. Good coupling.
- [ ] Response parsing iterates `_EMAIL_PATTERNS` and checks if the pattern appears as a substring of the answer — this could match partially (e.g. `"{first}"` inside `"{first}.{last}"`). A stricter exact-match would be safer.
- [ ] If the LLM returns `"unknown"`, no format is set and email construction is skipped — this is correct behaviour
- [ ] The result is appended as `"{pattern}@{domain}"` — this is the full format string used by `construct_email()` downstream

---

## Cross-Cutting Review Points

These apply across all 7 prompts.

### Output Parsing

| Risk | Where | Current Handling |
|---|---|---|
| LLM wraps JSON in markdown fences | Prompts 1, 2, 3, 6 | `re.search(r'\[.*?\]', response, re.DOTALL)` — strips fences implicitly |
| LLM returns extra explanation text | All prompts | Regex extraction picks out the structured part and ignores surrounding text |
| LLM returns empty string | All prompts | Each function has an explicit `if not content` / `if not match` guard |
| LLM returns malformed JSON | Prompts 1, 2, 3, 6 | `json.loads()` raises — caught by outer `except Exception` → silent fallback |

### Error Handling

All 7 prompts are wrapped in `try/except Exception` blocks with `logger.warning(...)`. Failures are silent to the SDR — the pipeline continues with partial or empty data. This is intentional for resilience but means bad prompts may be hard to detect without log monitoring.

### Temperature

All prompts use `temperature=0` or `temperature=0.0`. Correct for deterministic structured output tasks.

### Token Limits

| Prompt | `max_tokens` | Assessment |
|---|---|---|
| Role Expansion | 800 | Adequate for 35 titles |
| Importance Notes | 1500 | Adequate for large contact batches |
| Find More Role Titles | 300 | Tight — could truncate for multilingual lists |
| Must-Have Tier Web Search | Default | Fine — single name/title response |
| Company Website Fallback | Default | Fine — plain text list |
| Department Priority Ranking | Default | Fine — small JSON array |
| Email Format | Default | Fine — single pattern word |

---

## Questions for Senior Review

1. **Product description is hardcoded** — Prompt 2 says _"We sell digital commerce / ecommerce technology"_. Should this be a config variable so the agent can be reused for different sales products?

2. **SDR freetext injection** — Prompt 3 takes raw SDR input as `{prompt}` with no sanitisation. Should we cap length or strip special characters before injecting into the LLM call?

3. **Adjacent role suggestion** — Prompt 3 instructs the LLM to suggest "ADJACENT" roles. Is this behaviour desired, or should Find More be strictly limited to what the SDR asked for?

4. **Name validation in Prompt 4** — The `_search_must_have_tiers_web()` name validator only excludes a short word list (`the`, `is`, `was`, `company`, `unknown`). Is this sufficient, or do we need a more robust name classifier?

5. **Prompt 5 domain grounding** — The website fallback prompt passes the company's domain but `llm_web_search` may not restrict search to that domain. Should we add explicit instruction like _"Search only {domain}"_?

6. **Email pattern substring matching** — Prompt 7 parses the response by checking if a pattern appears as a substring. Could return the wrong pattern if patterns are subsets of each other. Should this be an exact match?

7. **Silent fallbacks** — All prompts fail silently. Is there a threshold (e.g. 3 consecutive failures per company) at which we should surface an error to the SDR rather than proceeding with incomplete data?

---

_Document generated from code at commit `a07be53`. All line numbers reference `backend/agents/searcher.py`._
