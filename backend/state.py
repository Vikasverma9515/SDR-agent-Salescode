"""
All Pydantic state models for the SCAI ProspectOps pipeline.

Design principle: Every piece of data that moves between nodes or agents is
typed, validated, and serializable. No dicts-as-state. No optional fields
that are actually required. Use Annotated reducer patterns for list fields
that accumulate across nodes.
"""
from __future__ import annotations

import operator
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class TargetCompany(BaseModel):
    """A single target company flowing through Fini."""

    raw_name: str
    normalized_name: str | None = None
    linkedin_org_id: str | None = None
    sales_nav_url: str | None = None
    domain: str | None = None
    email_format: str | None = None  # e.g. "{first}.{last}@domain.com"
    sdr_assigned: str | None = None
    account_type: str | None = None   # Region: India, Global, LATAM, etc.
    account_size: str | None = None   # Small / Medium / Large
    operator_confirmed: bool = False
    sheet_row_written: bool = False
    n8n_submitted: bool = False

    # Confidence scores: "high" | "medium" | "low" for each enriched field
    linkedin_confidence: str | None = None  # how certain is the org ID / Sales Nav URL
    domain_confidence: str | None = None    # how certain is the domain
    email_confidence: str | None = None     # how certain is the email format
    size_confidence: str | None = None      # how certain is the account size

    # Human-readable notes from the agent explaining what was found / any ambiguity
    agent_notes: str | None = None

    # Top LinkedIn candidate matches — each candidate is independently enriched
    # [{org_id, name, slug, how, domain, email_format, account_size, account_type,
    #   sales_nav_url, domain_confidence, email_confidence, size_confidence}]
    linkedin_candidates: list[dict] = Field(default_factory=list)

    # Auto-mode: was this company auto-committed without human review?
    auto_committed: bool = False
    # LLM reasoning explaining why a particular candidate was chosen (shown in frontend)
    selection_reasoning: str | None = None


class Contact(BaseModel):
    """A single contact flowing through Searcher and Veri."""

    full_name: str
    company: str
    domain: str
    role_title: str | None = None
    role_bucket: Literal[
        "DM", "Champion", "Influencer", "GateKeeper", "Unknown",
        "CEO/MD", "CTO/CIO", "CSO/Head of Sales", "P1 Influencer", "Gatekeeper",
        "FDM", "KDM", "Irrelevant",
    ] = "Unknown"
    linkedin_url: str | None = None
    linkedin_verified: bool = False
    email: str | None = None
    email_status: Literal["valid", "invalid", "catch-all", "unknown", "pending", "constructed"] = "pending"
    phones: list[str] = Field(default_factory=list)
    provenance: list[str] = Field(default_factory=list)  # which sources found this person
    verification_status: Literal["VERIFIED", "REVIEW", "REJECT", "PENDING"] = "PENDING"
    verification_notes: str | None = None
    verification_timestamp: str | None = None
    importance_note: str | None = None  # AI-generated "why this person matters" for SDR
    priority_score: int | None = None   # AI-assigned value score (0-100)


class FiniState(BaseModel):
    """State for the Fini agent graph."""

    companies: list[TargetCompany]
    current_index: int = 0
    submit_to_n8n: bool = False
    region: str = ""  # operator-provided region, applied to all companies in this batch
    enrichment_done: bool = False  # True after parallel_enrich_all completes
    auto_mode: bool = False         # When True: auto-commit high-confidence, pause on ambiguous
    sdr_name: str = ""              # SDR name for auto-commit sheet writes
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)
    status: Literal["running", "awaiting_confirmation", "completed", "failed"] = "running"
    thread_id: str | None = None  # used to emit per-company progress events


class SearcherState(BaseModel):
    """State for the Searcher agent graph."""

    # Current company being processed
    target_company: str
    target_domain: str = ""       # populated from Target Accounts in load_gap_analysis
    target_org_id: str = ""             # LinkedIn org ID from Target Accounts (Fini's work)
    target_email_format: str = ""       # email format from Target Accounts
    target_region: str = ""             # Account type / region from Target Accounts (e.g. "India")
    target_account_size: str = ""       # Account Size from Target Accounts (Small/Medium/Large)
    target_normalized_name: str = ""    # Normalized company name from Target Accounts (col A)
    target_sales_nav_url: str = ""      # Sales Navigator company people URL from Target Accounts
    # All companies to process (list of {"name": str})
    target_companies: list[dict] = Field(default_factory=list)
    current_index: int = 0
    # DM roles Amy wants filled (e.g. ["VP Ecommerce", "CDO", "Head of Digital"])
    dm_roles: list[str] = Field(default_factory=list)
    # Which DM roles are missing for the current company (populated by load_gap_analysis)
    missing_dm_roles: list[str] = Field(default_factory=list)
    # Multilingual/expanded role variants (populated by expand_search_terms)
    expanded_dm_roles: list[str] = Field(default_factory=list)
    # Structured gap info: which tiers are missing and what roles to search for
    # [{"tier": "FDM", "search_queries": ["CEO", "MD", ...], "priority": 1}, ...]
    missing_tiers: list[dict] = Field(default_factory=list)
    # People discovered via web search (name + title + tier), before LinkedIn lookup
    # [{"name": "...", "title": "...", "tier": "...", "sources": [...], "confidence": "..."}, ...]
    web_discovered_people: list[dict] = Field(default_factory=list)
    # How many contacts to find per company (0 = unlimited)
    target_contact_count: int = 10
    discovered_contacts: list[Contact] = Field(default_factory=list)
    # Senior contacts found with roles NOT in target list — held for SDR selection
    pending_dm_candidates: list[Contact] = Field(default_factory=list)
    phase: Literal[
        "discover_role_holders", "linkedin_lookup", "verify",
        "enrichment", "write_output", "done",
        # Legacy phases kept for backwards compatibility
        "unipile_search", "filing_search", "web_search", "linkedin_validation",
    ] = "discover_role_holders"
    role_buckets: list[dict] = Field(default_factory=list)
    # Names already existing in Google Sheets (populated by gap analysis)
    existing_names: list[str] = Field(default_factory=list)
    # Track rows written to FIRST_CLEAN_LIST for Veri auto-trigger
    total_contacts_written: int = 0       # accumulates across all companies
    fcl_row_start: int | None = None      # first row written to FIRST_CLEAN_LIST (1-based data row)
    fcl_row_end: int | None = None        # last row written
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)
    thread_id: str | None = None  # used to emit per-company progress events
    # If True: skip SDR pause steps (role selection + contact approval) and auto-write all matched contacts
    auto_approve: bool = False


class VeriState(BaseModel):
    """State for the Veri agent graph."""

    contacts: list[Contact]
    current_index: int = 0
    verified_count: int = 0
    review_count: int = 0
    rejected_count: int = 0
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)
    status: Literal["running", "completed", "failed"] = "running"
    row_start: int | None = None  # 1-based data row (not header), inclusive — optional override
    row_end: int | None = None    # 1-based data row, inclusive — optional override
    company_filter: str | None = None  # only verify contacts for this company (auto-detect mode)
    thread_id: str | None = None   # injected by API so emit calls work
    # Raw A-P values keyed by contact index (copied to Rejected Profiles for REJECT contacts)
    raw_rows: dict[int, list] = Field(default_factory=dict)
    # Source column value per contact index ("n8n" or "searcher")
    source_values: dict[int, str] = Field(default_factory=dict)
    # Actual 1-based sheet row number keyed by contact index (for in-place update of cols Q-W)
    sheet_row_nums: dict[int, int] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Shared search result model
# ---------------------------------------------------------------------------


class SearchResult(BaseModel):
    """Normalized search result from any provider."""

    title: str
    url: str
    snippet: str
    source_provider: Literal["ddg", "tavily", "perplexity"]

    def __getitem__(self, key: str):
        return getattr(self, key)
