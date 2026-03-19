"""
Scraper for Hacea Coffee (haceacoffee.com) green coffee listings.
Hybrid approach: Shopify JSON API for listing, Playwright for detail pages.

Outputs:
  - data/hacea_coffees.csv          Full dataset
  - data/hacea_detail_cache.json    Cached detail-page extractions keyed by URL
  - docs/hacea-data.json            All coffees with detail data (for GitHub Pages)
"""

import csv
import json
import logging
import sys
import time
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BASE_URL = "https://haceacoffee.com"
PRODUCTS_JSON_URL = f"{BASE_URL}/collections/green-coffee/products.json?limit=250"

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DOCS_DIR = REPO_ROOT / "docs"
CACHE_FILE = DATA_DIR / "hacea_detail_cache.json"
CSV_FILE = DATA_DIR / "hacea_coffees.csv"
JSON_FILE = DOCS_DIR / "hacea-data.json"

CSV_COLUMNS = [
    "name", "url", "price", "country", "region", "tags",
    "washing_station", "cooperative", "coordinates", "farmer_grower",
    "varieties", "processing", "altitude", "humidity", "density",
    "harvest_date", "arrival_date", "grade", "marks", "reference_id",
    "sca_cupping_score", "tariff_cost",
    "tasting_notes", "roasting_notes", "story",
    "image_url",
]


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
    CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ---------- Listing via Shopify JSON API ----------

def fetch_product_listing() -> list[dict]:
    """Fetch all green coffee products from Shopify JSON API."""
    log.info("Fetching product listing from Shopify JSON API...")
    resp = requests.get(PRODUCTS_JSON_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    products = data.get("products", [])
    assert len(products) > 0, "No products returned from Shopify API"
    log.info("Fetched %d products from Shopify API.", len(products))

    listings = []
    for p in products:
        # Skip non-coffee products (e.g., "Roaster Seasoning Green Coffee")
        tags = [t.strip() for t in p.get("tags", [])]

        # Get the smallest variant price (500g)
        variants = p.get("variants", [])
        price = ""
        if variants:
            prices = [float(v["price"]) for v in variants if v.get("price")]
            if prices:
                price = f"${min(prices):.2f}"

        # Extract country from tags
        country = ""
        known_countries = [
            "Brazil", "Colombia", "Ethiopia", "Guatemala", "Costa Rica",
            "El Salvador", "India", "Honduras", "Kenya", "Rwanda",
            "Nicaragua", "Peru", "Mexico", "Panama", "Indonesia",
            "Burundi", "Tanzania", "Uganda", "Yemen", "Bolivia",
        ]
        for tag in tags:
            if tag in known_countries:
                country = tag
                break

        # Get primary image
        images = p.get("images", [])
        image_url = images[0]["src"] if images else ""

        url = f"{BASE_URL}/products/{p['handle']}"
        listings.append({
            "name": p["title"],
            "handle": p["handle"],
            "url": url,
            "price": price,
            "country": country,
            "tags": ", ".join(tags),
            "image_url": image_url,
        })

    return listings


# ---------- Detail page extraction ----------

DETAIL_JS = """() => {
    const result = {};

    // Tab 1 - Description specs
    const tab1 = document.getElementById('tab1');
    result.specs = {};
    if (tab1) {
        const paragraphs = tab1.querySelectorAll('p');
        for (const p of paragraphs) {
            const strong = p.querySelector('strong');
            if (strong) {
                const key = strong.textContent.trim().replace(/:$/, '');
                // Get text after the strong tag
                const fullText = p.textContent.trim();
                const strongText = strong.textContent.trim();
                const value = fullText.substring(strongText.length).trim();
                result.specs[key] = value;
            }
        }
    }

    // Tab 2 - Tasting Notes
    const tab2 = document.getElementById('tab2');
    result.tasting_notes = tab2 ? tab2.textContent.trim() : '';

    // Tab 3 - Roasting Notes
    const tab3 = document.getElementById('tab3');
    result.roasting_notes = tab3 ? tab3.textContent.trim() : '';

    // Story sections from shopify sections below the tabs
    const storyParts = [];

    // image-with-text sections
    const imageTextSections = document.querySelectorAll(
        '.shopify-section--image-with-text'
    );
    for (const section of imageTextSections) {
        const els = section.querySelectorAll('p, h2, h3, h4');
        for (const el of els) {
            const text = el.textContent.trim();
            if (text.length > 10) storyParts.push(text);
        }
        // Also capture div-based headings (non-paragraph text blocks)
        const divTexts = section.querySelectorAll(
            '.block__heading, [class*="heading"], [class*="title"]'
        );
        for (const el of divTexts) {
            const text = el.textContent.trim();
            if (text.length > 5 && !storyParts.includes(text)) {
                storyParts.unshift(text);
            }
        }
    }

    // rich-text-and-image sections
    const richTextSections = document.querySelectorAll(
        '.shopify-section--rich-text-and-image'
    );
    for (const section of richTextSections) {
        const els = section.querySelectorAll('p, h2, h3, h4');
        for (const el of els) {
            const text = el.textContent.trim();
            if (text.length > 10) storyParts.push(text);
        }
    }

    result.story = storyParts.join(' ');

    return result;
}"""


def extract_detail_data(page, url: str) -> dict:
    """Visit a product detail page, return raw extracted data (cacheable)."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout loading %s — trying domcontentloaded", url)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3000)  # give JS time to render tabs
        except PlaywrightTimeout:
            log.warning("Second timeout loading %s — returning empty detail", url)
            return {}

    # Wait for tabs to be present
    try:
        page.wait_for_selector("#tab1", timeout=10_000)
    except PlaywrightTimeout:
        log.warning("No #tab1 found on %s — extracting what we can", url)

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
    record["country"] = listing.get("country", "")
    record["tags"] = listing.get("tags", "")
    record["image_url"] = listing.get("image_url", "")

    if not detail:
        return record

    specs = detail.get("specs", {})

    # Override country from specs if available (more reliable)
    if specs.get("Country"):
        record["country"] = specs["Country"]

    record["region"] = specs.get("Region", "")
    record["washing_station"] = specs.get("Washing Station", "")
    record["cooperative"] = specs.get("Cooperative", "")
    record["coordinates"] = specs.get("Coordinates", "")
    record["farmer_grower"] = specs.get("Farmer/Grower", "")
    record["varieties"] = specs.get("Varieties", "")
    record["processing"] = specs.get("Processing", "")
    record["altitude"] = specs.get("Altitude", "")
    record["humidity"] = specs.get("Humidity", "")
    record["density"] = specs.get("Density", "")
    record["harvest_date"] = specs.get("Harvest Date", "")
    record["arrival_date"] = specs.get("Arrival Date", "")
    record["grade"] = specs.get("Grade", "")
    record["marks"] = specs.get("Marks", "")
    record["reference_id"] = specs.get("Reference ID", "")
    record["sca_cupping_score"] = specs.get("SCA Cupping Score", "")
    record["tariff_cost"] = specs.get("Tariff Cost", "")

    record["tasting_notes"] = detail.get("tasting_notes", "")
    record["roasting_notes"] = detail.get("roasting_notes", "")
    record["story"] = detail.get("story", "")

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
    """Write docs/hacea-data.json with all coffees that have detail data."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    # Include all coffees that have a price
    priced = [r for r in records if r.get("price")]
    JSON_FILE.write_text(
        json.dumps(priced, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info("Wrote %d coffees to %s", len(priced), JSON_FILE)


# ---------- Main ----------

def main():
    cache = load_cache()

    # Step 1: Fetch listing from Shopify JSON API (no browser needed)
    listings = fetch_product_listing()
    if not listings:
        log.error("No products found from Shopify API. Exiting.")
        sys.exit(1)

    # Step 2: For each product, use cache or scrape detail page
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
                time.sleep(1)  # polite delay

            record = build_record(listing, detail)
            records.append(record)

        browser.close()

    log.info("Cache: %d hits, %d misses", cache_hits, cache_misses)

    # Step 3: Write all outputs
    save_cache(cache)
    write_csv(records)
    write_json(records)

    log.info("Done. %d total coffees.", len(records))


if __name__ == "__main__":
    main()
