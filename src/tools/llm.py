"""
Shared LLM helper — tries OpenAI first, falls back to Claude via AWS Bedrock.

Usage:
    from src.tools.llm import llm_complete, llm_web_search

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

from src.config import get_settings
from src.utils.logging import get_logger

logger = get_logger("llm")


# ---------------------------------------------------------------------------
# OpenAI helpers
# ---------------------------------------------------------------------------

async def _openai_responses(prompt: str, model: str = "gpt-5",
                            use_web_search: bool = False,
                            max_retries: int = 2, wait_secs: int = 30) -> str:
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
                       max_retries: int = 2, wait_secs: int = 30,
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


# ---------------------------------------------------------------------------
# Public API — try OpenAI, fallback to Claude Bedrock
# ---------------------------------------------------------------------------

async def llm_web_search(prompt: str, model: str = "gpt-5") -> str:
    """
    LLM call with web search capability.
    Tries OpenAI Responses API (has native web_search tool).
    Falls back to Claude Bedrock (no web search, but still answers from training data).
    """
    # Try OpenAI first
    try:
        result = await _openai_responses(prompt, model=model, use_web_search=True)
        if result and result.strip():
            return result
    except Exception as e:
        logger.warning("llm_web_search_openai_failed", error=str(e))

    # Fallback to Claude Bedrock
    try:
        logger.info("llm_web_search_bedrock_fallback")
        result = await _bedrock_claude(prompt)
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
    """
    # Try OpenAI first
    try:
        result = await _openai_chat(prompt, model=model,
                                    max_tokens=max_tokens, temperature=temperature)
        if result and result.strip():
            return result
    except Exception as e:
        logger.warning("llm_complete_openai_failed", error=str(e))

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
