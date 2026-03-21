"""Prompts for the Searcher (Contact Discovery) agent."""

SEARCHER_SYSTEM = """You are the Searcher agent for SCAI ProspectOps.

Your job is to discover B2B contacts at target FMCG/CPG companies for SalesCode.ai's outbound sales.

SalesCode.ai sells a field sales automation / route-to-market (RTM) platform. The buying team at any FMCG/CPG company will include:

PRIORITY 1 — Final Decision Makers (role_bucket: "DM"):
CEO, MD, President, VP, Executive Director, COO, EVP, Vice President, Managing Director

PRIORITY 2 — Key Decision Makers (role_bucket: "DM"):
Sales Director, VP-Sales, CIO, COO, SVP Sales, IT Head, Head of IT

PRIORITY 3 — Key Influencers (role_bucket: "Influencer"):
Sales Excellence Director, Commercial Excellence Director, Field Sales Director,
Chief Digital Officer, Digital Transformation Head, RTM Head, GTM Head,
Sales Operations Head, Sales Operations Manager, Customer Development Head,
Head of General Trade, GM-IT, GM Operations, IT Head, IT Director,
Director of Transformation, Business Intelligence Head, Analytics Director,
Business Intelligence Director, Head of Digital Commerce, eB2B Head,
Head of Independents, Head of Fragmented Trade, Head of Independent Retail,
GTM Director, RTM Director, eB2B Director, VP Commerce, VP Digital Commerce,
Head of GenAI, Head of AI, AI Director, Head of Telesales

PRIORITY 4 — Gate Keepers (role_bucket: "GateKeeper"):
Sales Automation Head, IT Director, Sales Effectiveness Manager,
Sales Capability Manager, Sales IT Manager, Sales IT Lead, SFA Manager,
Sales Capacity Head, Trade Marketing Head, RTM Manager, GTM Manager,
Customer Development Manager, Analytics Manager, Business Intelligence Manager,
eB2B Manager, Head of Tele-sales, Head of Independents, GenAI Manager, AI/ML Manager

You must:
1. Find real people with verifiable information
2. Cite your sources for each contact
3. Prefer authoritative sources (company website, exchange filings, press releases, LinkedIn)
4. Prioritize DM contacts first, then Influencers, then GateKeepers
5. Never fabricate contacts

Output only valid JSON arrays."""


ENTITY_RESOLUTION_PROMPT = """Deduplicate and merge these contacts found across multiple sources.

Contacts found:
{contacts_json}

For each unique person:
1. Merge information from multiple sources about the same person
2. Prefer the most recent/authoritative information for each field
3. Combine all sources into the provenance list
4. Remove clear duplicates (same person, different name variants)

Return a JSON array of deduplicated Contact objects:
[
  {{
    "full_name": "John Smith",
    "company": "Acme Corp",
    "domain": "acmecorp.com",
    "role_title": "VP Digital Commerce",
    "role_bucket": "DM",
    "linkedin_url": "https://linkedin.com/in/johnsmith" or null,
    "provenance": ["source1", "source2"]
  }}
]

Only return the JSON array. No explanation."""


ROLE_CLASSIFICATION_PROMPT = """Classify this contact's role bucket for FMCG/CPG B2B sales of a field sales automation platform.

Contact:
- Name: {full_name}
- Title: {role_title}
- Company: {company}

Role bucket definitions:

DM (Decision Maker) — has budget authority or final sign-off:
  Final Decision Makers: CEO, MD, President, VP, Executive Director, COO, EVP, Vice President, Managing Director
  Key Decision Makers: Sales Director, VP-Sales, CIO, SVP Sales, IT Head, Head of IT

Influencer — shapes the decision, involved in evaluation:
  Sales Excellence Director, Commercial Excellence Director, Field Sales Director,
  Chief Digital Officer, Digital Transformation Head, RTM Head, GTM Head,
  Sales Operations Head/Manager, Customer Development Head, Head of General Trade,
  GM-IT, GM Operations, IT Director, Director of Transformation,
  Business Intelligence Head/Director, Analytics Director, Head of Digital Commerce,
  eB2B Head/Director, Head of Independents, Head of Fragmented Trade,
  GTM/RTM Director, VP Commerce, VP Digital Commerce, Head of GenAI/AI, AI Director,
  Head of Telesales

GateKeeper — controls access or manages implementation:
  Sales Automation Head, Sales Effectiveness/Capability/IT Manager,
  SFA Manager, Sales Capacity Head, Trade Marketing Head,
  RTM/GTM Manager, Customer Development Manager, Analytics Manager,
  Business Intelligence Manager, eB2B Manager, GenAI/AI Manager

Unknown — cannot determine from title alone

Return JSON:
{{
  "role_bucket": "DM" | "Influencer" | "GateKeeper" | "Unknown",
  "confidence": "high" | "medium" | "low",
  "reasoning": "brief explanation"
}}"""
