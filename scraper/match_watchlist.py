"""Producer watchlist matching — deterministic substring matching for green coffee scrapers.

Loads producer_watchlist.csv from the coffee-explorer repo and matches product
records against it. Ported from coffee-explorer/scraper/match.py (Tier 1 only).
"""

import csv
import logging
import re
import unicodedata
from pathlib import Path

log = logging.getLogger(__name__)

WATCHLIST_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "coffee-explorer"
    / "data"
    / "producer_watchlist.csv"
)

WATCHLIST_FIELDS = [
    "watchlist_match",
    "watchlist_farm",
    "watchlist_tier",
    "watchlist_credential_type",
    "watchlist_credential_detail",
    "watchlist_notes",
    "watchlist_url",
]


def _normalize(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", without_accents.lower().strip())


# Words too common in coffee product text to use as standalone match terms
_STOPWORDS = frozenset({
    "coffee", "coffees", "cafe", "farm", "farms", "family", "estate", "estates",
    "cooperative", "coop", "union", "project", "special", "reserve", "natural",
    "washed", "honey", "roast", "roasted", "blend", "single", "origin",
    "bourbon", "geisha", "gesha", "java", "nova", "halo", "goro",
})


def _build_match_terms(producer: dict) -> list[str]:
    """Build normalized search terms from a watchlist producer.

    Uses full names only — no single last-name extraction (too many false positives).
    """
    terms = []

    name = producer.get("producer_name", "").strip()
    if name:
        terms.append(_normalize(name))

    farm = producer.get("farm_or_station", "").strip()
    if farm:
        for part in farm.split("/"):
            part = part.strip()
            if part:
                normalized = _normalize(part)
                terms.append(normalized)
                if normalized.startswith("finca "):
                    terms.append(normalized[6:])
                elif normalized.startswith("hacienda "):
                    terms.append(normalized[9:])

    # Filter: must be 5+ chars, not a stopword
    return [t for t in terms if len(t) >= 5 and t not in _STOPWORDS]


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
    """Build a token-efficient watchlist reference string for LLM prompts.

    Returns ~54 lines of "- Name (Farm)" suitable for appending to extraction prompts.
    """
    lines = []
    for p in watchlist:
        name = p.get("producer_name", "").strip()
        farm = p.get("farm_or_station", "").strip()
        if farm:
            lines.append(f"- {name} ({farm})")
        else:
            lines.append(f"- {name}")
    return "\n".join(lines)


def match_products(records: list[dict], corpus_fields: list[str]) -> None:
    """Run deterministic watchlist matching on records, mutating in-place.

    For each record, concatenates the values of corpus_fields into a search
    corpus and checks for substring matches against watchlist producer terms.
    Adds 7 watchlist_* fields to every record (empty string if no match).

    Args:
        records: list of dicts (product records) — mutated in-place.
        corpus_fields: keys to concatenate for substring search.
    """
    watchlist = load_watchlist()
    if not watchlist:
        # No watchlist — add empty fields and return
        for rec in records:
            for field in WATCHLIST_FIELDS:
                rec[field] = ""
        return

    # Pre-compute match terms per producer
    all_terms: dict[int, list[str]] = {}
    for i, producer in enumerate(watchlist):
        all_terms[i] = _build_match_terms(producer)

    matched_count = 0
    for rec in records:
        # Build search corpus from specified fields
        corpus_parts = [str(rec.get(f, "") or "") for f in corpus_fields]
        search_text = _normalize(" ".join(corpus_parts))

        hit = None
        for i, producer in enumerate(watchlist):
            for term in all_terms[i]:
                if term in search_text:
                    hit = producer
                    break
            if hit:
                break

        if hit:
            rec["watchlist_match"] = hit.get("producer_name", "")
            rec["watchlist_farm"] = hit.get("farm_or_station", "")
            rec["watchlist_tier"] = hit.get("tier", "")
            rec["watchlist_credential_type"] = hit.get("credential_type", "")
            rec["watchlist_credential_detail"] = hit.get("credential_detail", "")
            rec["watchlist_notes"] = hit.get("notes", "")
            rec["watchlist_url"] = hit.get("direct_sales_url", "")
            matched_count += 1
        else:
            for field in WATCHLIST_FIELDS:
                rec[field] = ""

    log.info(
        "Watchlist matching: %d/%d products matched (%d producers in watchlist)",
        matched_count, len(records), len(watchlist),
    )
