"""
Wikidata SPARQL queries for company and person data.
Used to find org IDs, official domains, and leadership.
"""
from __future__ import annotations

import asyncio
from typing import TypedDict

import httpx

from backend.utils.logging import get_logger

logger = get_logger("wikidata")

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"


class WikiCompanyInfo(TypedDict):
    name: str
    wikidata_id: str | None
    official_website: str | None
    industry: str | None
    country: str | None
    employees: str | None


class WikiPersonInfo(TypedDict):
    name: str
    wikidata_id: str | None
    employer: str | None
    position: str | None
    linkedin_url: str | None


async def lookup_company(company_name: str) -> WikiCompanyInfo:
    """Look up a company in Wikidata."""
    query = f"""
    SELECT ?company ?companyLabel ?website ?industryLabel ?countryLabel ?employees WHERE {{
      ?company wdt:P31 wd:Q4830453 .
      ?company rdfs:label "{company_name}"@en .
      OPTIONAL {{ ?company wdt:P856 ?website }}
      OPTIONAL {{ ?company wdt:P452 ?industry }}
      OPTIONAL {{ ?company wdt:P17 ?country }}
      OPTIONAL {{ ?company wdt:P1082 ?employees }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }}
    LIMIT 1
    """

    try:
        result = await _sparql_query(query)
        if result:
            row = result[0]
            wikidata_id = row.get("company", {}).get("value", "").split("/")[-1]
            return WikiCompanyInfo(
                name=company_name,
                wikidata_id=wikidata_id or None,
                official_website=row.get("website", {}).get("value"),
                industry=row.get("industryLabel", {}).get("value"),
                country=row.get("countryLabel", {}).get("value"),
                employees=row.get("employees", {}).get("value"),
            )
    except Exception as e:
        logger.warning("wikidata_company_error", company=company_name, error=str(e))

    return WikiCompanyInfo(
        name=company_name,
        wikidata_id=None,
        official_website=None,
        industry=None,
        country=None,
        employees=None,
    )


async def lookup_person(person_name: str, company_name: str | None = None) -> WikiPersonInfo:
    """Look up a person in Wikidata, optionally filtered by employer."""
    filter_clause = ""
    if company_name:
        filter_clause = f'?employer rdfs:label "{company_name}"@en .'

    query = f"""
    SELECT ?person ?personLabel ?employerLabel ?positionLabel WHERE {{
      ?person wdt:P31 wd:Q5 .
      ?person rdfs:label "{person_name}"@en .
      OPTIONAL {{
        ?person wdt:P108 ?employer .
        {filter_clause}
      }}
      OPTIONAL {{ ?person wdt:P39 ?position }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }}
    LIMIT 1
    """

    try:
        result = await _sparql_query(query)
        if result:
            row = result[0]
            wikidata_id = row.get("person", {}).get("value", "").split("/")[-1]
            return WikiPersonInfo(
                name=person_name,
                wikidata_id=wikidata_id or None,
                employer=row.get("employerLabel", {}).get("value"),
                position=row.get("positionLabel", {}).get("value"),
                linkedin_url=None,
            )
    except Exception as e:
        logger.warning("wikidata_person_error", person=person_name, error=str(e))

    return WikiPersonInfo(
        name=person_name,
        wikidata_id=None,
        employer=None,
        position=None,
        linkedin_url=None,
    )


async def _sparql_query(query: str) -> list[dict]:
    """Execute a SPARQL query against Wikidata."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers={"User-Agent": "SCAI-ProspectOps/1.0 (research pipeline)"},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", {}).get("bindings", [])
