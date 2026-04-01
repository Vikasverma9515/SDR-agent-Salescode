"""
Shared LLM helper — tries OpenAI first, falls back to Claude via AWS Bedrock.

Usage:
    from backend.tools.llm import llm_complete, llm_web_search

    # Simple text completion (no web search)
    text = await llm_complete("What is the capital of France?")

    # Web search + answer (OpenAI Responses API → Bedrock Claude fallback)
    text = await llm_web_search("Find the LinkedIn page for Sirca Paints")
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Literal

import httpx

from backend.config import get_settings
from backend.utils.logging import get_logger

logger = get_logger("llm")

# Global semaphore — limits concurrent OpenAI/Bedrock calls across all companies
_LLM_CONCURRENCY = 8
_llm_semaphore = asyncio.Semaphore(_LLM_CONCURRENCY)

# Circuit breaker — if OpenAI fails 2 times within 60s, switch to Bedrock permanently for this process
_openai_failures: list[float] = []  # timestamps of recent failures
_openai_dead = False  # once True, stays True for the entire process lifetime


def _openai_is_broken() -> bool:
    """Check if OpenAI has been permanently disabled."""
    return _openai_dead


def _record_openai_failure():
    """Record an OpenAI failure. 2 failures within 60s = permanent switch to Bedrock."""
    global _openai_dead
    import time
    now = time.time()
    _openai_failures.append(now)
    # Count failures in the last 60 seconds
    recent = [t for t in _openai_failures if now - t <= 60]
    if len(recent) >= 2:
        _openai_dead = True
        logger.warning("openai_circuit_breaker_permanent",
                       msg="OpenAI failed 2x in 60s — switching to Bedrock for this session")


# ---------------------------------------------------------------------------
# OpenAI helpers
# ---------------------------------------------------------------------------

async def _openai_responses(prompt: str, model: str = "gpt-5",
                            use_web_search: bool = False,
                            max_retries: int = 1, wait_secs: int = 10) -> str:
    """Call OpenAI Responses API. Retries on 429."""
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    tools = [{"type": "web_search"}] if use_web_search else []

    async with httpx.AsyncClient(timeout=60) as client:
        for attempt in range(max_retries + 1):
            resp = await client.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={"model": model, "tools": tools, "input": prompt},
            )
            if resp.status_code == 429 and attempt < max_retries:
                logger.warning("openai_429_retry", attempt=attempt + 1, wait=wait_secs)
                await asyncio.sleep(wait_secs)
                continue
            resp.raise_for_status()
            break

        data = resp.json()
        content = ""
        for item in data.get("output", []):
            if item.get("type") == "message":
                for part in item.get("content", []):
                    if part.get("type") == "output_text":
                        content += part.get("text", "")
        return content


async def _openai_chat(prompt: str, model: str = "gpt-4.1-mini",
                       max_retries: int = 1, wait_secs: int = 10,
                       max_tokens: int = 200, temperature: float = 0) -> str:
    """Call OpenAI Chat Completions API. Retries on 429."""
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    from openai import AsyncOpenAI
    oai = AsyncOpenAI(api_key=settings.openai_api_key)

    for attempt in range(max_retries + 1):
        try:
            resp = await oai.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            if "429" in str(e) and attempt < max_retries:
                logger.warning("openai_chat_429_retry", attempt=attempt + 1, wait=wait_secs)
                await asyncio.sleep(wait_secs)
                continue
            raise


# ---------------------------------------------------------------------------
# Claude Bedrock helpers
# ---------------------------------------------------------------------------

async def _bedrock_claude(prompt: str, max_tokens: int = 1024,
                          temperature: float = 0) -> str:
    """Call Claude via AWS Bedrock using bearer token auth (REST API)."""
    settings = get_settings()
    if not settings.aws_bearer_token_bedrock:
        raise RuntimeError("AWS_BEARER_TOKEN_BEDROCK not set")

    region = settings.aws_bedrock_region
    model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
    url = f"https://bedrock-runtime.{region}.amazonaws.com/model/{model_id}/invoke"

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {settings.aws_bearer_token_bedrock}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()

    text = ""
    for block in result.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")
    return text


async def _bedrock_claude_with_search(prompt: str, max_tokens: int = 1024,
                                       temperature: float = 0) -> str:
    """
    Claude Bedrock with web search context.
    Runs Perplexity/Tavily/DDG search first, feeds results into Claude's prompt.
    """
    from backend.tools.search import search_with_fallback
    from backend.state import SearchResult

    # Extract a good search query from the prompt (first 150 chars, clean up)
    search_query = prompt[:150].replace('"', '').replace('\n', ' ').strip()

    # Run search
    search_results = []
    try:
        search_results = await search_with_fallback(search_query, max_results=5)
    except Exception as e:
        logger.warning("bedrock_search_prefetch_error", error=str(e))

    # Build context from search results
    if search_results:
        context_parts = []
        for i, r in enumerate(search_results, 1):
            context_parts.append(f"[{i}] {r.title}\nURL: {r.url}\n{r.snippet}")
        search_context = "\n\n".join(context_parts)

        augmented_prompt = (
            f"I searched the web for relevant information. Here are the results:\n\n"
            f"{search_context}\n\n"
            f"---\n\n"
            f"Based on the search results above and your knowledge, please answer:\n\n"
            f"{prompt}"
        )
    else:
        augmented_prompt = prompt

    return await _bedrock_claude(augmented_prompt, max_tokens=max_tokens,
                                 temperature=temperature)


# ---------------------------------------------------------------------------
# Public API — try OpenAI, fallback to Claude Bedrock
# ---------------------------------------------------------------------------

async def llm_web_search(prompt: str, model: str = "gpt-5") -> str:
    """
    LLM call with web search capability.
    Tries OpenAI Responses API (has native web_search tool).
    Falls back to Claude Bedrock (no web search, but still answers from training data).
    Max 2 concurrent LLM calls across all companies (semaphore).
    """
    async with _llm_semaphore:
        # Try OpenAI first (unless circuit breaker tripped)
        if not _openai_is_broken():
            try:
                result = await _openai_responses(prompt, model=model, use_web_search=True)
                if result and result.strip():
                    return result
            except Exception as e:
                _record_openai_failure()
                logger.warning("llm_web_search_openai_failed", error=str(e),
                              failures=len(_openai_failures))
        else:
            logger.info("llm_web_search_openai_skipped", reason="circuit breaker open")

        # Fallback to Claude Bedrock with search context
        try:
            logger.info("llm_web_search_bedrock_fallback")
            result = await _bedrock_claude_with_search(prompt)
            if result and result.strip():
                return result
        except Exception as e:
            logger.warning("llm_web_search_bedrock_failed", error=str(e))

    return ""


async def llm_complete(prompt: str, model: str = "gpt-4.1-mini",
                       max_tokens: int = 200, temperature: float = 0) -> str:
    """
    Simple LLM text completion (no web search).
    Tries OpenAI Chat Completions, falls back to Claude Bedrock.
    Max 2 concurrent LLM calls across all companies (semaphore).
    """
    async with _llm_semaphore:
        # Try OpenAI first (unless circuit breaker tripped)
        if not _openai_is_broken():
            try:
                result = await _openai_chat(prompt, model=model,
                                            max_tokens=max_tokens, temperature=temperature)
                if result and result.strip():
                    return result
            except Exception as e:
                _record_openai_failure()
                logger.warning("llm_complete_openai_failed", error=str(e),
                              failures=len(_openai_failures))
        else:
            logger.info("llm_complete_openai_skipped", reason="circuit breaker open")

        # Fallback to Claude Bedrock
        try:
            logger.info("llm_complete_bedrock_fallback")
            result = await _bedrock_claude(prompt, max_tokens=max_tokens,
                                           temperature=temperature)
            if result and result.strip():
                return result
        except Exception as e:
            logger.warning("llm_complete_bedrock_failed", error=str(e))

    return ""
