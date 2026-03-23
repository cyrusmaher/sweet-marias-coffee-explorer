"""
Scraper for Sweet Maria's green coffee listings.
Uses Playwright with incremental caching: the listing page is always scraped
(fast, one request), but detail pages are only fetched for new/changed URLs.

Outputs:
  - data/coffees.csv          Full dataset
  - data/detail_cache.json    Cached detail-page extractions keyed by URL
  - docs/data.json            Filtered to priced coffees only (for GitHub Pages)
"""

import csv
import json
import logging
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Allow importing match_watchlist from same directory
_SCRAPER_DIR = Path(__file__).resolve().parent
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

from match_watchlist import WATCHLIST_FIELDS, match_products

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.sweetmarias.com"
LISTING_URL = f"{BASE_URL}/green-coffee.html?product_list_limit=all&sm_status=1"

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DOCS_DIR = REPO_ROOT / "docs"
CACHE_FILE = DATA_DIR / "detail_cache.json"
CSV_FILE = DATA_DIR / "coffees.csv"
JSON_FILE = DOCS_DIR / "data.json"

# --- Country parsing from coffee name ---
COUNTRY_PREFIXES = [
    ("Papua New Guinea", "Papua New Guinea"),
    ("Costa Rica", "Costa Rica"),
    ("Brazil", "Brazil"),
    ("Colombia", "Colombia"),
    ("Ethiopia", "Ethiopia"),
    ("Flores", "Indonesia (Flores)"),
    ("Guatemala", "Guatemala"),
    ("Hawaii", "USA (Hawaii)"),
    ("Honduras", "Honduras"),
    ("Java", "Indonesia (Java)"),
    ("Kenya", "Kenya"),
    ("Nicaragua", "Nicaragua"),
    ("Peru", "Peru"),
    ("Rwanda", "Rwanda"),
    ("Sulawesi", "Indonesia (Sulawesi)"),
    ("Sumatra", "Indonesia (Sumatra)"),
    ("Zambia", "Zambia"),
]

COUNTRY_TO_ORIGIN_CATEGORY = {
    "Brazil": "South America",
    "Colombia": "South America",
    "Peru": "South America",
    "Nicaragua": "Central America",
    "Costa Rica": "Central America",
    "Guatemala": "Central America",
    "Honduras": "Central America",
    "Ethiopia": "Africa",
    "Kenya": "Africa",
    "Rwanda": "Africa",
    "Zambia": "Africa",
    "Indonesia (Flores)": "Indonesia & SE Asia",
    "Indonesia (Java)": "Indonesia & SE Asia",
    "Indonesia (Sulawesi)": "Indonesia & SE Asia",
    "Indonesia (Sumatra)": "Indonesia & SE Asia",
    "USA (Hawaii)": "North America",
    "Papua New Guinea": "Oceania",
}

NON_GEO_CATEGORIES = {"Decaf", "Sample Sets", "Sweet Maria's Blends", "Roasted Coffee"}

# --- Score categories ---
CUPPING_CATEGORIES = [
    "Dry Fragrance", "Wet Aroma", "Brightness", "Flavor", "Body",
    "Finish", "Sweetness", "Clean Cup", "Complexity", "Uniformity",
]

FLAVOR_CATEGORIES = [
    "Floral", "Honey", "Sugars", "Caramel", "Fruits", "Citrus",
    "Berry", "Cocoa", "Nuts", "Rustic", "Spice", "Flavor Body",
]

CSV_COLUMNS = [
    "name", "url", "price", "country", "region", "origin_category",
    "total_score", "cupper_correction",
    *[f"cupping_{c.lower().replace(' ', '_')}" for c in CUPPING_CATEGORIES],
    *[f"flavor_{c.lower().replace(' ', '_')}" for c in FLAVOR_CATEGORIES],
    "process_method", "cultivar", "cultivar_detail", "farm_gate",
    "processing", "drying_method", "arrival_date", "lot_size", "bag_size",
    "packaging", "grade", "appearance", "roast_recommendations", "type",
    "recommended_for_espresso",
    "short_description", "full_cupping_notes", "farm_notes",
    # Watchlist matching
    *WATCHLIST_FIELDS,
]


def parse_country(name: str) -> str:
    for prefix, country in COUNTRY_PREFIXES:
        if name.startswith(prefix):
            return country
    return ""


def fix_origin_category(origin_category: str, country: str) -> str:
    if origin_category in NON_GEO_CATEGORIES and country:
        return COUNTRY_TO_ORIGIN_CATEGORY.get(country, origin_category)
    return origin_category


def parse_chart_value(data_str: str) -> dict[str, str]:
    result = {}
    if not data_str:
        return result
    for pair in data_str.split(","):
        parts = pair.split(":", 1)
        if len(parts) == 2:
            result[parts[0].strip()] = parts[1].strip()
    return result


# ---------- Cache I/O ----------

def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Failed to load cache (%s), starting fresh", e)
    return {}


def save_cache(cache: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------- Listing page ----------

def extract_listing_data(page) -> list[dict]:
    """Scrape the 'View All' listing page — one fast request."""
    log.info("Loading listing page (View All)...")
    page.goto(LISTING_URL, wait_until="networkidle", timeout=60_000)
    page.wait_for_selector("table tbody tr", timeout=30_000)

    rows = page.evaluate(
        """() => {
        const rows = document.querySelectorAll('.products-grid tbody tr, .products.list tbody tr, table.products tbody tr, #amasty-shopby-product-list table tbody tr');
        const allRows = rows.length > 0 ? rows : document.querySelectorAll('main table tbody tr');
        return Array.from(allRows).map(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length < 3) return null;
            const originCategory = cells[0]?.textContent?.trim() || '';
            const nameEl = cells[1]?.querySelector('a');
            const name = nameEl?.textContent?.trim() || '';
            const url = nameEl?.href || '';
            const priceText = cells[2]?.textContent?.trim() || '';
            const priceMatch = priceText.match(/\\$[\\d.]+/);
            const price = priceMatch ? priceMatch[0] : priceText;
            return { name, url, price, origin_category: originCategory };
        }).filter(r => r && r.name && r.url);
    }"""
    )
    log.info("Found %d coffees on listing page.", len(rows))
    return rows


# ---------- Detail page ----------

DETAIL_JS = """() => {
    const result = {};

    // Short description
    const descEl = document.querySelector('.product-info-main .product.attribute.overview .value, .product.attribute.description p, .product-info-main > p');
    result.short_description = descEl ? descEl.textContent.trim() : '';

    // Total score
    const scoreEl = document.querySelector('.score-value');
    result.total_score = scoreEl ? scoreEl.textContent.trim() : '';

    // Cupping chart data
    const cuppingChart = document.querySelector('.forix-chartjs[data-chart-id="cupping-chart"]');
    result.cupping_values = cuppingChart ? cuppingChart.getAttribute('data-chart-value') || '' : '';
    result.cupping_score = cuppingChart ? cuppingChart.getAttribute('data-chart-score') || '' : '';
    result.cupper_correction = cuppingChart ? cuppingChart.getAttribute('data-cupper-correction') || '' : '';

    // Flavor chart data
    const flavorChart = document.querySelector('.forix-chartjs[data-chart-id="flavor-chart"]');
    result.flavor_values = flavorChart ? flavorChart.getAttribute('data-chart-value') || '' : '';

    // Full cupping notes
    const cuppingPanel = document.querySelector('[id="product.info.description"]');
    if (cuppingPanel) {
        const p = cuppingPanel.querySelector('p');
        result.full_cupping_notes = p ? p.textContent.trim() : '';
    } else {
        result.full_cupping_notes = '';
    }

    // Overview list attrs (Process Method, Cultivar, Farm Gate)
    const overviewItems = document.querySelectorAll('[id="product.info.description"] li');
    result.overview_attrs = {};
    for (const item of overviewItems) {
        const strong = item.querySelector('strong');
        const value = item.querySelector('div, span:not(strong)');
        if (strong && value) {
            result.overview_attrs[strong.textContent.trim().replace(/:$/, '')] = value.textContent.trim();
        }
    }

    // Farm notes
    const farmPanel = document.querySelector('#product-info-origin-notes');
    if (farmPanel) {
        const p = farmPanel.querySelector('p');
        result.farm_notes = p ? p.textContent.trim() : farmPanel.textContent.trim();
    } else {
        result.farm_notes = '';
    }

    // Specs table
    const specsPanel = document.querySelector('[id="product.info.specs"]');
    result.specs = {};
    if (specsPanel) {
        const rows = specsPanel.querySelectorAll('tr');
        for (const row of rows) {
            const th = row.querySelector('th');
            const td = row.querySelector('td');
            if (th && td) {
                result.specs[th.textContent.trim()] = td.textContent.trim();
            }
        }
    }

    return result;
}"""


def extract_detail_data(page, url: str) -> dict:
    """Visit a product detail page, return raw extracted data (cacheable)."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout loading %s — returning empty detail", url)
        return {}

    try:
        return page.evaluate(DETAIL_JS)
    except Exception as e:
        log.warning("Error extracting detail from %s: %s", url, e)
        return {}


# ---------- Record building ----------

def build_record(listing: dict, detail: dict) -> dict:
    """Merge listing-page data with (possibly cached) detail-page data."""
    record = {col: "" for col in CSV_COLUMNS}
    record["name"] = listing["name"]
    record["url"] = listing["url"]
    record["price"] = listing["price"]
    record["country"] = parse_country(listing["name"])
    record["origin_category"] = fix_origin_category(
        listing["origin_category"], record["country"]
    )

    if not detail:
        return record

    record["short_description"] = detail.get("short_description", "")
    record["total_score"] = detail.get("total_score", "") or detail.get("cupping_score", "")
    record["cupper_correction"] = detail.get("cupper_correction", "")
    record["full_cupping_notes"] = detail.get("full_cupping_notes", "")
    record["farm_notes"] = detail.get("farm_notes", "")

    # Cupping scores
    cupping = parse_chart_value(detail.get("cupping_values", ""))
    for cat in CUPPING_CATEGORIES:
        col = f"cupping_{cat.lower().replace(' ', '_')}"
        record[col] = cupping.get(cat, "")

    # Flavor scores
    flavor = parse_chart_value(detail.get("flavor_values", ""))
    for cat in FLAVOR_CATEGORIES:
        source_key = cat if cat != "Flavor Body" else "Body"
        col = f"flavor_{cat.lower().replace(' ', '_')}"
        record[col] = flavor.get(source_key, "")

    # Overview attrs
    ov = detail.get("overview_attrs", {})
    record["process_method"] = ov.get("Process Method", "")
    record["cultivar"] = ov.get("Cultivar", "")
    record["farm_gate"] = ov.get("Farm Gate", "")

    # Specs
    specs = detail.get("specs", {})
    record["region"] = specs.get("Region", "")
    record["processing"] = specs.get("Processing", "")
    record["drying_method"] = specs.get("Drying Method", "")
    record["arrival_date"] = specs.get("Arrival date", "")
    record["lot_size"] = specs.get("Lot size", "")
    record["bag_size"] = specs.get("Bag size", "")
    record["packaging"] = specs.get("Packaging", "")
    record["cultivar_detail"] = specs.get("Cultivar Detail", "")
    record["grade"] = specs.get("Grade", "")
    record["appearance"] = specs.get("Appearance", "")
    record["roast_recommendations"] = specs.get("Roast Recommendations", "")
    record["type"] = specs.get("Type", "")
    record["recommended_for_espresso"] = specs.get("Recommended for Espresso", "")

    return record


# ---------- Output writers ----------

def write_csv(records: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(records)
    log.info("Wrote %d coffees to %s", len(records), CSV_FILE)


def write_json(records: list[dict]) -> None:
    """Write docs/data.json filtered to coffees that have a price."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    priced = [r for r in records if r.get("price") and r["price"].startswith("$")]
    JSON_FILE.write_text(
        json.dumps(priced, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Wrote %d priced coffees to %s", len(priced), JSON_FILE)


# ---------- Main ----------

def main():
    cache = load_cache()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        # Step 1: Always scrape listing (fast, one page)
        listings = extract_listing_data(page)
        if not listings:
            log.error("No coffees found on listing page. Exiting.")
            browser.close()
            sys.exit(1)

        # Step 2: For each coffee, use cache or scrape detail
        records = []
        cache_hits = 0
        cache_misses = 0
        total = len(listings)

        for i, listing in enumerate(listings, 1):
            url = listing["url"]
            if url in cache:
                log.info("[%d/%d] CACHED: %s", i, total, listing["name"])
                detail = cache[url]
                cache_hits += 1
            else:
                log.info("[%d/%d] SCRAPING: %s", i, total, listing["name"])
                detail = extract_detail_data(page, url)
                cache[url] = detail
                cache_misses += 1
                time.sleep(1)  # polite delay only for actual requests

            record = build_record(listing, detail)
            records.append(record)

        browser.close()

    log.info("Cache: %d hits, %d misses", cache_hits, cache_misses)

    # Step 3: Watchlist producer matching
    match_products(records, corpus_fields=[
        "name", "country", "region", "farm_notes",
        "cultivar_detail", "short_description",
    ])

    # Step 4: Write all outputs
    save_cache(cache)
    write_csv(records)
    write_json(records)

    log.info("Done. %d total coffees.", len(records))


if __name__ == "__main__":
    main()
