"""LLM-based extraction of structured coffee data from Royal Coffee tab text.

Uses Gemini 3 Flash via the google-genai SDK (Vertex AI backend with global
endpoint). Caches results keyed by sha256(text)[:16].

Parallelism: asyncio.Semaphore + asyncio.create_task + asyncio.as_completed.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import threading
from pathlib import Path

from google import genai
from pydantic import BaseModel, Field, ValidationError

# Allow importing match_watchlist from same directory
_SCRAPER_DIR = Path(__file__).resolve().parent
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

from match_watchlist import load_watchlist, build_watchlist_ref

log = logging.getLogger(__name__)

CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "royal_llm_cache.json"

MODEL = "gemini-3-flash-preview"
CONCURRENCY = 30
CACHE_SAVE_INTERVAL = 20


class RoyalCoffeeExtracted(BaseModel):
    """LLM-extracted structured data from Royal Coffee tab text."""
    region: str | None = None
    altitude: str | None = None
    variety: list[str] = Field(default_factory=list)
    process: str | None = None
    grower: str | None = None
    harvest: str | None = None
    is_coffee_product: bool = True
    watchlist_match: str | None = None


EXTRACTION_PROMPT = """\
You are a specialty green coffee data extractor. Given text from a Royal Coffee \
product page (Overview and Source tabs), extract structured information.

Product name: {name}

Page text:
{text}

Extract the following fields. If a field is not mentioned, return null for strings \
or an empty list for arrays. Be precise — only extract what is explicitly stated.

Return a JSON object with these fields:
- region (string | null): Specific region, department, or area within the country \
  (e.g. "Huila", "Yirgacheffe", "Antigua")
- altitude (string | null): Growing altitude/elevation (e.g. "1800 masl", "1600-1900m")
- variety (list[string]): Coffee variety/cultivar names (e.g. "Geisha", "Bourbon", "SL-28"). \
  Normalize: "geisha" → "Gesha", "sl28"/"sl-28" → "SL-28", "borbon" → "Bourbon"
- process (string | null): Processing method (e.g. "Washed", "Natural", "Honey", \
  "Carbonic Maceration")
- grower (string | null): Producer, farmer, farm name, or cooperative \
  (e.g. "Edwin Noreña", "Finca La Terraza")
- harvest (string | null): Harvest period or year (e.g. "October-December 2024", "2024/25")
- is_coffee_product (bool): true if this is a green/unroasted coffee product. \
  Return false for chicory, tea, cascara, or other non-coffee products.
- watchlist_match (string | null): If the text mentions a producer or farm from the \
  elite producer watchlist below, return the producer's name exactly as listed. \
  Return null if no match.

Elite producer watchlist:
{watchlist_ref}

Return ONLY the JSON object, no markdown fences or explanation."""


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Failed to load Royal LLM cache (%s), starting fresh", e)
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8",
    )


def _init_client() -> genai.Client:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("VERTEX_AI_LOCATION", "global")

    if project:
        log.info("Using Vertex AI: project=%s, location=%s", project, location)
        return genai.Client(vertexai=True, project=project, location=location)

    api_key = os.environ.get("GEMINI_API_KEY")
    if api_key:
        log.info("Using Gemini API key (direct)")
        return genai.Client(api_key=api_key)

    raise RuntimeError(
        "Set GOOGLE_CLOUD_PROJECT + VERTEX_AI_LOCATION=global for Vertex AI, "
        "or GEMINI_API_KEY for direct API access."
    )


def _parse_llm_response(text: str) -> RoyalCoffeeExtracted:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```\w*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        data = json.loads(cleaned)
        return RoyalCoffeeExtracted(**data)
    except (json.JSONDecodeError, ValidationError) as e:
        log.warning("Failed to parse LLM response: %s\nResponse: %s", e, text[:200])
        return RoyalCoffeeExtracted(is_coffee_product=True)


async def extract_royal_products(
    items: list[dict],
) -> dict[str, RoyalCoffeeExtracted]:
    """Extract structured data from Royal Coffee product texts using Gemini Flash.

    Args:
        items: list of {"url": str, "name": str, "text": str}

    Returns:
        dict mapping product URL -> RoyalCoffeeExtracted
    """
    cache = _load_cache()
    client = _init_client()
    results: dict[str, RoyalCoffeeExtracted] = {}
    cache_hits = 0
    llm_calls = 0
    failed_jobs: dict[str, str] = {}
    cache_lock = threading.Lock()

    # Build watchlist reference once for all prompts
    watchlist = load_watchlist()
    wl_ref = build_watchlist_ref(watchlist) if watchlist else "(no watchlist available)"

    work_items: list[tuple[dict, str, str]] = []

    for item in items:
        text = item["text"]
        cache_key = _content_hash(f"{item['name']}|{text}")

        if cache_key in cache:
            try:
                results[item["url"]] = RoyalCoffeeExtracted(**cache[cache_key])
                cache_hits += 1
                continue
            except ValidationError:
                pass

        if not text or len(text) < 50:
            results[item["url"]] = RoyalCoffeeExtracted(is_coffee_product=True)
            continue

        if len(text) > 3000:
            text = text[:3000] + "..."

        prompt = EXTRACTION_PROMPT.format(
            name=item["name"], text=text, watchlist_ref=wl_ref,
        )
        work_items.append((item, cache_key, prompt))

    log.info(
        "[royal-llm] %d cache hits, %d need LLM extraction (concurrency=%d)",
        cache_hits, len(work_items), CONCURRENCY,
    )

    if not work_items:
        _save_cache(cache)
        return results

    sem = asyncio.Semaphore(CONCURRENCY)
    completed = 0

    async def extract_one(item: dict, cache_key: str, prompt: str):
        nonlocal completed
        async with sem:
            try:
                response = await client.aio.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                )
                extracted = _parse_llm_response(response.text)

                with cache_lock:
                    cache[cache_key] = extracted.model_dump()

                completed += 1
                if completed % CACHE_SAVE_INTERVAL == 0:
                    with cache_lock:
                        _save_cache(cache)
                    log.info(
                        "[royal-llm] Progress: %d/%d LLM calls complete",
                        completed, len(work_items),
                    )

                return item["url"], extracted, None
            except Exception as e:
                completed += 1
                return item["url"], None, str(e)

    tasks = [
        asyncio.create_task(extract_one(item, cache_key, prompt))
        for item, cache_key, prompt in work_items
    ]

    for fut in asyncio.as_completed(tasks):
        url, extracted, err = await fut
        if err is None and extracted is not None:
            results[url] = extracted
            llm_calls += 1
        else:
            if err:
                failed_jobs[url] = err
            results[url] = RoyalCoffeeExtracted(is_coffee_product=True)
            llm_calls += 1

    _save_cache(cache)

    if failed_jobs:
        log.warning(
            "[royal-llm] %d extraction failures: %s",
            len(failed_jobs),
            list(failed_jobs.values())[:3],
        )

    log.info(
        "[royal-llm] Extraction complete: %d items, %d LLM calls, %d cache hits, %d failures",
        len(items), llm_calls, cache_hits, len(failed_jobs),
    )
    return results
