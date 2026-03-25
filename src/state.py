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


class Contact(BaseModel):
    """A single contact flowing through Searcher and Veri."""

    full_name: str
    company: str
    domain: str
    role_title: str | None = None
    role_bucket: Literal["DM", "Champion", "Influencer", "GateKeeper", "Unknown"] = "Unknown"
    linkedin_url: str | None = None
    linkedin_verified: bool = False
    email: str | None = None
    email_status: Literal["valid", "invalid", "catch-all", "unknown", "pending"] = "pending"
    zerobounce_score: float | None = None
    phones: list[str] = Field(default_factory=list)
    provenance: list[str] = Field(default_factory=list)  # which sources found this person
    verification_status: Literal["VERIFIED", "REVIEW", "REJECT", "PENDING"] = "PENDING"
    verification_notes: str | None = None
    verification_timestamp: str | None = None


class FiniState(BaseModel):
    """State for the Fini agent graph."""

    companies: list[TargetCompany]
    current_index: int = 0
    submit_to_n8n: bool = False
    region: str = ""  # operator-provided region, applied to all companies in this batch
    enrichment_done: bool = False  # True after parallel_enrich_all completes
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)
    status: Literal["running", "awaiting_confirmation", "completed", "failed"] = "running"


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
    # All companies to process (list of {"name": str})
    target_companies: list[dict] = Field(default_factory=list)
    current_index: int = 0
    # DM roles Amy wants filled (e.g. ["VP Ecommerce", "CDO", "Head of Digital"])
    dm_roles: list[str] = Field(default_factory=list)
    # Which DM roles are missing for the current company (populated by load_gap_analysis)
    missing_dm_roles: list[str] = Field(default_factory=list)
    discovered_contacts: list[Contact] = Field(default_factory=list)
    phase: Literal[
        "unipile_search", "filing_search", "web_search", "linkedin_validation", "enrichment", "write_output", "done"
    ] = "unipile_search"
    # Track rows written to FIRST_CLEAN_LIST for Veri auto-trigger
    total_contacts_written: int = 0       # accumulates across all companies
    fcl_row_start: int | None = None      # first row written to FIRST_CLEAN_LIST (1-based data row)
    fcl_row_end: int | None = None        # last row written
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)


class VeriState(BaseModel):
    """State for the Veri agent graph."""

    contacts: list[Contact]
    current_index: int = 0
    verified_count: int = 0
    review_count: int = 0
    rejected_count: int = 0
    errors: Annotated[list[str], operator.add] = Field(default_factory=list)
    status: Literal["running", "completed", "failed"] = "running"
    row_start: int | None = None  # 1-based data row (not header), inclusive
    row_end: int | None = None    # 1-based data row, inclusive
    # Raw row values from First Clean List keyed by contact index (for copying to Final Filtered List)
    raw_rows: dict[int, list] = Field(default_factory=dict)


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
