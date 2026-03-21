"""
Unified search interface across DDG, Tavily, and Perplexity.

All results normalized to SearchResult model.
DDG: no API key needed.
Tavily: tavily-python SDK.
Perplexity: REST API (OpenAI-compatible).
"""
from __future__ import annotations

import asyncio
from typing import Literal

import httpx
try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS

from src.config import get_settings
from src.state import SearchResult
from src.utils.logging import get_logger
from src.utils.rate_limiter import search_limiter

logger = get_logger("search")


async def search(
    query: str,
    provider: Literal["ddg", "tavily", "perplexity"] = "ddg",
    max_results: int = 10,
) -> list[SearchResult]:
    """
    Unified search interface. Returns normalized SearchResult list.
    Falls back gracefully if a provider fails.
    """
    await search_limiter.acquire()

    try:
        if provider == "ddg":
            return await _ddg_search(query, max_results)
        elif provider == "tavily":
            return await _tavily_search(query, max_results)
        elif provider == "perplexity":
            return await _perplexity_search(query, max_results)
        else:
            raise ValueError(f"Unknown search provider: {provider}")
    except Exception as e:
        logger.warning("search_failed", provider=provider, query=query[:100], error=str(e))
        return []


async def search_with_fallback(
    query: str,
    max_results: int = 10,
    providers: list[Literal["ddg", "tavily", "perplexity"]] | None = None,
) -> list[SearchResult]:
    """
    Try providers in order until one succeeds.
    Default: Perplexity -> Tavily -> DDG
    """
    if providers is None:
        providers = ["perplexity", "tavily", "ddg"]

    for provider in providers:
        results = await search(query, provider=provider, max_results=max_results)
        if results:
            logger.info("search_success", provider=provider, query=query[:80], n_results=len(results))
            return results

    logger.warning("all_search_providers_failed", query=query[:100])
    return []


# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------


async def _ddg_search(query: str, max_results: int) -> list[SearchResult]:
    loop = asyncio.get_event_loop()

    def _run():
        ddgs = DDGS()
        return list(ddgs.text(query, max_results=max_results))

    raw = await asyncio.wait_for(loop.run_in_executor(None, _run), timeout=20)
    return [
        SearchResult(
            title=r.get("title", ""),
            url=r.get("href", ""),
            snippet=r.get("body", ""),
            source_provider="ddg",
        )
        for r in raw
    ]


async def _tavily_search(query: str, max_results: int) -> list[SearchResult]:
    from tavily import TavilyClient

    settings = get_settings()
    client = TavilyClient(api_key=settings.tavily_api_key)

    loop = asyncio.get_event_loop()
    response = await asyncio.wait_for(
        loop.run_in_executor(None, lambda: client.search(query, max_results=max_results, search_depth="basic")),
        timeout=20,
    )

    return [
        SearchResult(
            title=r.get("title", ""),
            url=r.get("url", ""),
            snippet=r.get("content", ""),
            source_provider="tavily",
        )
        for r in response.get("results", [])
    ]


async def _perplexity_search(query: str, max_results: int) -> list[SearchResult]:
    settings = get_settings()

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.perplexity_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "sonar",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a research assistant. Return relevant factual information with sources.",
                    },
                    {"role": "user", "content": query},
                ],
                "max_tokens": 1024,
            },
        )
        response.raise_for_status()
        data = response.json()

    content = data["choices"][0]["message"]["content"]
    citations = data.get("citations", [])

    # Convert Perplexity's response to SearchResult format
    results = []
    if citations:
        for i, url in enumerate(citations[:max_results]):
            results.append(
                SearchResult(
                    title=f"Perplexity Source {i+1}",
                    url=url,
                    snippet=content[:500] if i == 0 else "",
                    source_provider="perplexity",
                )
            )
    else:
        # No citations but we have content
        results.append(
            SearchResult(
                title="Perplexity Research",
                url="",
                snippet=content[:1000],
                source_provider="perplexity",
            )
        )

    return results
