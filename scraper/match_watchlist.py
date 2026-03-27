"""Producer watchlist matching — LLM-based matching for green coffee scrapers.

Loads producer_watchlist.csv from the coffee-explorer repo and matches product
records against it using Gemini Flash. Results are cached by content hash.
"""

import csv
import hashlib
import json
import logging
import os
import re
from pathlib import Path

from google import genai

log = logging.getLogger(__name__)

WATCHLIST_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "coffee-explorer"
    / "data"
    / "producer_watchlist.csv"
)

MATCH_CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "match_cache.json"

MODEL = "gemini-3-flash-preview"

WATCHLIST_FIELDS = [
    "watchlist_match",
    "watchlist_farm",
    "watchlist_tier",
    "watchlist_credential_type",
    "watchlist_credential_detail",
    "watchlist_notes",
    "watchlist_url",
]


def load_watchlist() -> list[dict]:
    """Load producer watchlist CSV. Returns [] if file missing (graceful)."""
    if not WATCHLIST_FILE.exists():
        log.warning("Watchlist not found at %s — skipping producer matching", WATCHLIST_FILE)
        return []
    with open(WATCHLIST_FILE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info("Loaded %d producers from watchlist", len(rows))
    return rows


def build_watchlist_ref(watchlist: list[dict]) -> str:
    """Build a token-efficient watchlist reference string for LLM prompts."""
    lines = []
    for p in watchlist:
        name = p.get("producer_name", "").strip()
        farm = p.get("farm_or_station", "").strip()
        if farm:
            lines.append(f"- {name} ({farm})")
        else:
            lines.append(f"- {name}")
    return "\n".join(lines)


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _load_cache() -> dict:
    if MATCH_CACHE_FILE.exists():
        try:
            return json.loads(MATCH_CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cache(cache: dict) -> None:
    MATCH_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    MATCH_CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8",
    )


def _parse_json_response(text: str) -> list | dict:
    """Parse JSON from LLM response, stripping markdown fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```\w*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)
    return json.loads(cleaned.strip())


def _find_watchlist_row(name: str, watchlist: list[dict]) -> dict | None:
    """Find a watchlist row by producer name or farm name (case-insensitive)."""
    norm = name.strip().lower()
    for wp in watchlist:
        if (wp.get("producer_name", "").strip().lower() == norm
                or wp.get("farm_or_station", "").strip().lower() == norm):
            return wp
    # Partial match — check if name appears in farm field (handles "Finca X" vs "X")
    for wp in watchlist:
        farm = wp.get("farm_or_station", "").lower()
        if norm and norm in farm:
            return wp
    return None


def _init_client() -> genai.Client:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("VERTEX_AI_LOCATION", "global")
    if project:
        return genai.Client(vertexai=True, project=project, location=location)
    api_key = os.environ.get("GEMINI_API_KEY")
    if api_key:
        return genai.Client(api_key=api_key)
    raise RuntimeError(
        "Set GOOGLE_CLOUD_PROJECT + VERTEX_AI_LOCATION=global for Vertex AI, "
        "or GEMINI_API_KEY for direct API access."
    )


def match_products(records: list[dict], corpus_fields: list[str]) -> None:
    """Run LLM-based watchlist matching on records, mutating in-place.

    For each record, sends the product text to Gemini Flash along with the
    watchlist to determine if the product is from a tracked producer.
    Results are cached by content hash to avoid repeat API calls.

    Args:
        records: list of dicts (product records) — mutated in-place.
        corpus_fields: keys to use as product context for matching.
    """
    watchlist = load_watchlist()
    if not watchlist:
        for rec in records:
            for field in WATCHLIST_FIELDS:
                rec[field] = ""
        return

    try:
        client = _init_client()
    except RuntimeError as e:
        log.warning("No LLM client available — skipping matching: %s", e)
        for rec in records:
            for field in WATCHLIST_FIELDS:
                rec[field] = ""
        return

    cache = _load_cache()

    watchlist_ref = "\n".join(
        f"- {p.get('producer_name', '?')} | {p.get('farm_or_station', '?')} | "
        f"{p.get('country', '?')}"
        for p in watchlist
    )

    # Separate cached from uncached
    uncached_indices: list[int] = []
    for i, rec in enumerate(records):
        corpus_parts = [str(rec.get(f, "") or "") for f in corpus_fields]
        cache_key = _content_hash(" ".join(corpus_parts))
        rec["_cache_key"] = cache_key

        if cache_key in cache:
            cached_val = cache[cache_key]
            if cached_val is None:
                for field in WATCHLIST_FIELDS:
                    rec[field] = ""
            else:
                wp = _find_watchlist_row(cached_val, watchlist)
                if wp:
                    _apply_match(rec, wp)
                else:
                    for field in WATCHLIST_FIELDS:
                        rec[field] = ""
        else:
            uncached_indices.append(i)

    cached_count = len(records) - len(uncached_indices)
    log.info("Watchlist matching: %d cached, %d to process", cached_count, len(uncached_indices))

    if not uncached_indices:
        _cleanup_keys(records)
        _save_cache(cache)
        return

    # Batch LLM matching
    batch_size = 10
    batches = [
        uncached_indices[i:i + batch_size]
        for i in range(0, len(uncached_indices), batch_size)
    ]

    matched_count = 0
    for batch_num, batch_indices in enumerate(batches, 1):
        products_text = "\n".join(
            f"{j+1}. {_product_summary(records[idx], corpus_fields)}"
            for j, idx in enumerate(batch_indices)
        )

        prompt = f"""\
You are matching green coffee product listings to a watchlist of elite producers.

IMPORTANT: Most products will NOT match any watchlist producer. "No match" is the \
expected answer for the majority of products. Only return a match if the product \
text explicitly names the producer, farm, or estate on the watchlist.

DO NOT match based on:
- Shared country or region alone (e.g. a coffee from "El Paraiso, Honduras" is NOT \
  from Diego Bermudez's "Finca El Paraiso" in Colombia)
- Shared processing method or variety
- Common place names that happen to match a farm name
- Vague associations or educated guesses

WATCHLIST (producer | farm | country):
{watchlist_ref}

PRODUCTS:
{products_text}

Return a JSON array with one entry per product:
[{{"product_number": 1, "matched_producer": null}}, ...]

Set matched_producer to the exact watchlist producer name ONLY if you are certain \
the product is from that specific producer/farm. Otherwise null.
Return ONLY the JSON array."""

        try:
            response = client.models.generate_content(model=MODEL, contents=prompt)
            matches = _parse_json_response(response.text)

            for match_result in matches:
                j = match_result.get("product_number", 0) - 1
                if 0 <= j < len(batch_indices):
                    idx = batch_indices[j]
                    rec = records[idx]
                    matched_name = match_result.get("matched_producer")

                    if matched_name:
                        wp = _find_watchlist_row(matched_name, watchlist)
                        if wp:
                            _apply_match(rec, wp)
                            cache[rec["_cache_key"]] = matched_name
                            matched_count += 1
                        else:
                            for field in WATCHLIST_FIELDS:
                                rec[field] = ""
                            cache[rec["_cache_key"]] = None
                    else:
                        for field in WATCHLIST_FIELDS:
                            rec[field] = ""
                        cache[rec["_cache_key"]] = None

        except Exception as e:
            log.error("LLM matching batch %d failed: %s", batch_num, e)
            for idx in batch_indices:
                rec = records[idx]
                for field in WATCHLIST_FIELDS:
                    rec[field] = ""

        if batch_num % 5 == 0 or batch_num == len(batches):
            log.info("Matching progress: %d/%d batches", batch_num, len(batches))

    _cleanup_keys(records)
    _save_cache(cache)

    total_matched = sum(1 for r in records if r.get("watchlist_match"))
    log.info(
        "Watchlist matching: %d/%d products matched (%d producers in watchlist)",
        total_matched, len(records), len(watchlist),
    )


def _product_summary(rec: dict, corpus_fields: list[str]) -> str:
    """Build a concise product summary for the LLM prompt."""
    parts = []
    for f in corpus_fields:
        val = str(rec.get(f, "") or "").strip()
        if val:
            parts.append(f"{f}: {val[:300]}")
    return " | ".join(parts) if parts else "(no data)"


def _apply_match(rec: dict, wp: dict) -> None:
    rec["watchlist_match"] = wp.get("producer_name", "")
    rec["watchlist_farm"] = wp.get("farm_or_station", "")
    rec["watchlist_tier"] = wp.get("tier", "")
    rec["watchlist_credential_type"] = wp.get("credential_type", "")
    rec["watchlist_credential_detail"] = wp.get("credential_detail", "")
    rec["watchlist_notes"] = wp.get("notes", "")
    rec["watchlist_url"] = wp.get("direct_sales_url", "")


def _cleanup_keys(records: list[dict]) -> None:
    """Remove internal keys from records."""
    for rec in records:
        rec.pop("_cache_key", None)
