"""
Scraper for Coffee Bean Corral (coffeebeancorral.com) green coffee listings.
Uses Playwright for both listing pages (paginated) and detail pages.

Outputs:
  - data/cbc_coffees.csv          Full dataset
  - data/cbc_detail_cache.json    Cached detail-page extractions keyed by URL
  - docs/cbc-data.json            All coffees with detail data (for GitHub Pages)
"""

import csv
import json
import logging
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.coffeebeancorral.com"
ALL_COFFEES_URL = f"{BASE_URL}/categories/Green-Coffee-Beans/All-Coffees.aspx"

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DOCS_DIR = REPO_ROOT / "docs"
CACHE_FILE = DATA_DIR / "cbc_detail_cache.json"
CSV_FILE = DATA_DIR / "cbc_coffees.csv"
JSON_FILE = DOCS_DIR / "cbc-data.json"

ATTRIBUTE_NAMES = ["Brightness", "Body", "Aroma", "Complexity", "Balance", "Sweetness"]
FLAVOR_NAMES = ["Spicy", "Chocolaty", "Nutty", "Buttery", "Fruity", "Flowery", "Winey", "Earthy"]

CSV_COLUMNS = [
    "name", "url", "price", "sku", "rating", "review_count",
    "country", "local_region", "category", "process", "variety",
    "altitude", "harvest", "certifications",
    "organic", "fair_trade", "rainforest_alliance", "decaffeinated",
    "cupping_notes",
    *[f"attr_{a.lower()}" for a in ATTRIBUTE_NAMES],
    *[f"flavor_{f.lower()}" for f in FLAVOR_NAMES],
    "description",
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


# ---------- Listing page extraction ----------

LISTING_JS = """() => {
    const items = [];
    const records = document.querySelectorAll('.productgrid .record, .col-6 .record, .col-lg-4 .record');
    for (const record of records) {
        const nameEl = record.querySelector('.SingleProductDisplayName a, .recordname a');
        if (!nameEl) continue;

        const priceEl = record.querySelector('.PriceLabel');
        const ratingImg = record.querySelector('img.recordrating');
        const reviewLink = record.querySelector('.ProductRating a[href*="#Write"]');

        const name = nameEl.textContent.trim();
        const url = nameEl.href;
        const price = priceEl ? priceEl.textContent.trim() : '';

        // Parse rating from img alt text like "4 1/2 Stars" or "5 Stars"
        let rating = '';
        if (ratingImg) {
            const alt = ratingImg.alt || '';
            // Also try extracting from src: StarsNN.svg where NN is 10-50
            const srcMatch = (ratingImg.src || '').match(/Stars(\\d+)\\.svg/);
            if (srcMatch) {
                rating = (parseInt(srcMatch[1]) / 10).toString();
            } else {
                rating = alt;
            }
        }

        // Parse review count from text like "3 reviews"
        let reviewCount = '';
        if (reviewLink) {
            const match = reviewLink.textContent.match(/(\\d+)/);
            reviewCount = match ? match[1] : '';
        }

        items.push({ name, url, price, rating, review_count: reviewCount });
    }
    return items;
}"""


def extract_listing_page(page, url: str) -> list[dict]:
    """Extract all product cards from a single listing page."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout on listing page %s, trying domcontentloaded", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)

    try:
        page.wait_for_selector(".record", timeout=15_000)
    except PlaywrightTimeout:
        log.warning("No .record elements found on %s", url)
        return []

    return page.evaluate(LISTING_JS)


def get_total_pages(page) -> int:
    """Extract total page count from pagination."""
    try:
        return page.evaluate("""() => {
            const links = document.querySelectorAll('.pagination .page-link[data-facetoption]');
            let maxPage = 1;
            for (const link of links) {
                const num = parseInt(link.getAttribute('data-facetoption'));
                if (num > maxPage) maxPage = num;
            }
            return maxPage;
        }""")
    except Exception:
        return 1


def extract_all_listings(page) -> list[dict]:
    """Scrape all pages of the product listing."""
    log.info("Loading first listing page...")
    items = extract_listing_page(page, ALL_COFFEES_URL)
    total_pages = get_total_pages(page)
    log.info("Found %d products on page 1, %d total pages.", len(items), total_pages)

    for p in range(2, total_pages + 1):
        url = f"{ALL_COFFEES_URL}?p={p}"
        log.info("Loading listing page %d/%d...", p, total_pages)
        page_items = extract_listing_page(page, url)
        log.info("  Found %d products on page %d.", len(page_items), p)
        items.extend(page_items)
        time.sleep(0.5)

    # Deduplicate by URL
    seen = set()
    unique = []
    for item in items:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)

    log.info("Total unique products: %d", len(unique))
    return unique


# ---------- Detail page extraction ----------

DETAIL_JS = """() => {
    const result = {};

    // Name from itemprop
    const nameEl = document.querySelector('span[itemprop="name"]');
    result.full_name = nameEl ? nameEl.textContent.trim() : '';

    // Country, coffee name, region from h1 spans
    const countryEl = document.querySelector('.coff-country');
    const coffNameEl = document.querySelector('.coff-name');
    const regionEl = document.querySelector('.coff-region');
    result.country = countryEl ? countryEl.textContent.trim() : '';
    result.coffee_name = coffNameEl ? coffNameEl.textContent.trim() : '';
    result.coff_region = regionEl ? regionEl.textContent.trim() : '';

    // SKU
    const skuEl = document.querySelector('span[itemprop="sku"]');
    result.sku = skuEl ? skuEl.textContent.trim() : '';

    // Price
    const priceEl = document.querySelector('span[itemprop="price"]');
    result.price = priceEl ? priceEl.getAttribute('content') || priceEl.textContent.trim() : '';

    // Cupping notes
    const cuppingEl = document.getElementById('ctl00_MainContentHolder_lblShortDescription');
    result.cupping_notes = cuppingEl ? cuppingEl.textContent.trim() : '';

    // Attributes (1-7 scale)
    const attributes = ['Brightness', 'Body', 'Aroma', 'Complexity', 'Balance', 'Sweetness'];
    result.attributes = {};
    for (const attr of attributes) {
        const el = document.querySelector('[id$="img' + attr + '"]');
        if (el) {
            const match = el.className.match(/spd-matrix-attribute-(\\d+)/);
            result.attributes[attr] = match ? parseInt(match[1]) : 0;
        }
    }

    // Flavors (1-4 scale)
    const flavors = ['Spicy', 'Chocolaty', 'Nutty', 'Buttery', 'Fruity', 'Flowery', 'Winey', 'Earthy'];
    result.flavors = {};
    for (const flav of flavors) {
        const el = document.querySelector('[id$="img' + flav + '"]');
        if (el) {
            const match = el.className.match(/spd-matrix-flavor-(\\d+)/);
            result.flavors[flav] = match ? parseInt(match[1]) : 0;
        }
    }

    // Specifications
    result.specs = {};
    const specItems = document.querySelectorAll('ul.typedisplay li');
    for (const item of specItems) {
        const label = item.querySelector('.productpropertylabel');
        const value = item.querySelector('.productpropertyvalue');
        if (label && value) {
            result.specs[label.textContent.trim()] = value.textContent.trim();
        }
    }

    // Description
    const descEl = document.querySelector('span[itemprop="description"]');
    result.description = descEl ? descEl.textContent.trim() : '';

    // Rating from detail page
    const ratingImg = document.querySelector('#rating img.recordrating, img.recordrating');
    if (ratingImg) {
        const srcMatch = (ratingImg.src || '').match(/Stars(\\d+)\\.svg/);
        result.rating = srcMatch ? (parseInt(srcMatch[1]) / 10).toString() : '';
    }

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
            page.wait_for_timeout(3000)
        except PlaywrightTimeout:
            log.warning("Second timeout loading %s — returning empty detail", url)
            return {}

    try:
        page.wait_for_selector("#productpage, .prod-template", timeout=10_000)
    except PlaywrightTimeout:
        log.warning("No product page found on %s", url)

    try:
        return page.evaluate(DETAIL_JS)
    except Exception as e:
        log.warning("Error extracting detail from %s: %s", url, e)
        return {}


# ---------- Record building ----------

def parse_rating(rating_str: str) -> str:
    """Normalize rating string to a number."""
    if not rating_str:
        return ""
    # Already a number like "4.5"
    try:
        return str(float(rating_str))
    except ValueError:
        pass
    # Parse "4 1/2 Stars" format
    match = re.match(r"(\d+)(?:\s+(\d+)/(\d+))?\s*Stars?", rating_str, re.I)
    if match:
        val = int(match.group(1))
        if match.group(2) and match.group(3):
            val += int(match.group(2)) / int(match.group(3))
        return str(val)
    return rating_str


def build_record(listing: dict, detail: dict) -> dict:
    """Merge listing-page data with (possibly cached) detail-page data."""
    record = {col: "" for col in CSV_COLUMNS}
    record["name"] = listing.get("name", "")
    record["url"] = listing.get("url", "")
    record["price"] = listing.get("price", "")
    record["rating"] = parse_rating(listing.get("rating", ""))
    record["review_count"] = listing.get("review_count", "")

    if not detail:
        return record

    # Use detail page price if listing didn't have one
    if not record["price"] and detail.get("price"):
        record["price"] = f"${detail['price']}"

    record["sku"] = detail.get("sku", "")
    record["cupping_notes"] = detail.get("cupping_notes", "")
    record["description"] = detail.get("description", "")

    # Use detail page rating if listing didn't have one
    if not record["rating"] and detail.get("rating"):
        record["rating"] = parse_rating(detail["rating"])

    # Country from detail
    if detail.get("country"):
        record["country"] = detail["country"]

    # Specs
    specs = detail.get("specs", {})
    record["local_region"] = specs.get("Local Region", "")
    record["category"] = specs.get("Category", "")
    record["process"] = specs.get("Process", "")
    record["variety"] = specs.get("Variety", "")
    record["altitude"] = specs.get("Altitude (meters)", specs.get("Altitude", ""))
    record["harvest"] = specs.get("Harvest", "")
    record["certifications"] = specs.get("Certifications", "")
    record["organic"] = specs.get("Organic Certification", "")
    record["fair_trade"] = specs.get("Fair Trade", "")
    record["rainforest_alliance"] = specs.get("Rainforest Alliance Certified", "")
    record["decaffeinated"] = specs.get("Decaffeinated", "")

    # Attribute scores
    attrs = detail.get("attributes", {})
    for attr in ATTRIBUTE_NAMES:
        col = f"attr_{attr.lower()}"
        record[col] = str(attrs.get(attr, "")) if attrs.get(attr) else ""

    # Flavor scores
    flavors = detail.get("flavors", {})
    for flav in FLAVOR_NAMES:
        col = f"flavor_{flav.lower()}"
        record[col] = str(flavors.get(flav, "")) if flavors.get(flav) else ""

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
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    priced = [r for r in records if r.get("price")]
    JSON_FILE.write_text(
        json.dumps(priced, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info("Wrote %d coffees to %s", len(priced), JSON_FILE)


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

        # Step 1: Scrape all listing pages
        listings = extract_all_listings(page)
        if not listings:
            log.error("No coffees found on listing pages. Exiting.")
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
