"""
Scraper for Royal Coffee (royalcoffee.com) green coffee listings.
Uses Playwright for listing pages and detail pages.

Three product categories scraped from separate listing pages:
  - /offerings/          (paginated WooCommerce, full bags ~100lb)
  - /crown-jewels/       (single Divi page, 22lb boxes)
  - /50lb-green-coffee-royal-gems/  (single Divi page, 50lb boxes)

Staff picks flagged via /product-tag/staff-picks/.

Outputs:
  - data/royal_coffees.csv          Full dataset
  - data/royal_detail_cache.json    Cached detail-page extractions by URL
  - docs/royal-data.json            Priced coffees (for GitHub Pages)
"""

import asyncio
import csv
import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from match_watchlist import WATCHLIST_FIELDS, match_products

# Allow importing extract_royal from same directory
_SCRAPER_DIR = Path(__file__).resolve().parent
if str(_SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPER_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BASE_URL = "https://royalcoffee.com"
OFFERINGS_URL = f"{BASE_URL}/offerings/"
CROWN_JEWELS_URL = f"{BASE_URL}/crown-jewels/"
ROYAL_GEMS_URL = f"{BASE_URL}/50lb-green-coffee-royal-gems/"
STAFF_PICKS_URL = f"{BASE_URL}/product-tag/staff-picks/"

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DOCS_DIR = REPO_ROOT / "docs"
CACHE_FILE = DATA_DIR / "royal_detail_cache.json"
INFOGRAM_CACHE_FILE = DATA_DIR / "royal_infogram_cache.json"
CSV_FILE = DATA_DIR / "royal_coffees.csv"
JSON_FILE = DOCS_DIR / "royal-data.json"

CSV_COLUMNS = [
    "name", "url", "price_per_lb", "total_price", "bag_size", "bag_type",
    "country", "region", "altitude", "grower", "variety", "process",
    "flavor_notes", "about", "warehouse", "position", "certifications",
    "is_staff_pick", "inventory",
    # Tab text content
    "overview_text", "taste_text", "source_text",
    # Infogram data
    "word_cloud", "sweetness", "acidity", "viscosity", "balance",
    # Watchlist matching
    *WATCHLIST_FIELDS,
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


# ---------- Infogram Cache I/O ----------

def load_infogram_cache() -> dict:
    if INFOGRAM_CACHE_FILE.exists():
        try:
            return json.loads(INFOGRAM_CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Failed to load infogram cache (%s), starting fresh", e)
    return {}


def save_infogram_cache(cache: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    INFOGRAM_CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ---------- Infogram Data Fetcher ----------

def fetch_infogram_data(uuid: str) -> dict:
    """Fetch Infogram page and extract word cloud + gauge data from infographicData."""
    url = f"https://e.infogram.com/{uuid}"
    try:
        resp = requests.get(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch Infogram %s: %s", uuid, e)
        return {}

    match = re.search(r'window\.infographicData\s*=\s*', resp.text)
    if not match:
        log.warning("No infographicData in Infogram %s", uuid)
        return {}

    decoder = json.JSONDecoder()
    try:
        data, _ = decoder.raw_decode(resp.text, match.end())
    except json.JSONDecodeError as e:
        log.warning("Failed to parse infographicData JSON for %s: %s", uuid, e)
        return {}

    return _parse_infogram(data)


def _extract_text_entity(props: dict) -> str:
    """Extract text content from an Infogram TEXT entity's props."""
    try:
        text_blocks = props.get("content", {}).get("blocks", [])
        text = ""
        for tb in text_blocks:
            text += tb.get("text", "")
        return text.strip()
    except (AttributeError, TypeError):
        return ""


def _parse_infogram(data: dict) -> dict:
    """Parse infographicData JSON for word cloud terms and gauge values."""
    try:
        content = data["elements"]["content"]["content"]
        entities = content["entities"]
    except (KeyError, TypeError):
        return {}

    # Get ordered entity IDs from blocks
    blocks = content.get("blocks", {})
    ordered_ids = []
    for block_key in sorted(blocks.keys(), key=str):
        block_entities = blocks[block_key].get("entities", [])
        ordered_ids.extend(block_entities)

    word_cloud = {}
    gauges = []
    text_queue = []

    for eid in ordered_ids:
        entity = entities.get(str(eid)) or entities.get(eid)
        if not entity:
            continue

        etype = entity.get("type", "")
        props = entity.get("props", {})

        if etype == "CHART":
            chart_data_obj = props.get("chartData") or {}
            chart_type = chart_data_obj.get("chart_type_nr")
            chart_data = chart_data_obj.get("data", [[]])
            rows = chart_data[0] if chart_data else []

            if chart_type == 18:  # Word Cloud
                for row in rows[1:]:  # skip header
                    if len(row) >= 2 and row[0] and row[1]:
                        term = (row[0].get("value") or "").strip()
                        weight_str = (row[1].get("value") or "0").strip()
                        if term:
                            try:
                                word_cloud[term] = int(weight_str)
                            except ValueError:
                                word_cloud[term] = 1

            elif chart_type == 5:  # Gauge
                value = None
                for row in rows:
                    if (len(row) >= 2 and row[0]
                            and row[0].get("value") == "Value" and row[1]):
                        try:
                            value = int(float(row[1]["value"]))
                        except (ValueError, TypeError):
                            pass
                        break
                if value is not None and text_queue:
                    gauges.append({"label": text_queue[-1].lower(), "value": value})
                text_queue = []

        elif etype == "TEXT":
            text = _extract_text_entity(props)
            if text:
                text_queue.append(text)

    result = {}
    if word_cloud:
        result["word_cloud"] = word_cloud

    for g in gauges:
        label = g["label"]
        if "sweet" in label:
            result["sweetness"] = g["value"]
        elif "acid" in label:
            result["acidity"] = g["value"]
        elif "viscos" in label:
            result["viscosity"] = g["value"]
        elif "balanc" in label:
            result["balance"] = g["value"]

    return result


# ---------- Listing: WooCommerce paginated (/offerings/) ----------

OFFERINGS_LISTING_JS = """() => {
    const items = [];
    // WooCommerce product grid items
    const products = document.querySelectorAll(
        'ul.products li.product, .products .product, li.product'
    );
    for (const prod of products) {
        const link = prod.querySelector('a[href*="/product/"]');
        if (!link) continue;
        const url = link.href;
        // Avoid duplicates within same page
        if (items.some(i => i.url === url)) continue;
        items.push({ url });
    }
    // Fallback: grab all product links if WooCommerce selectors miss
    if (items.length === 0) {
        const links = document.querySelectorAll('a[href*="/product/"]');
        const seen = new Set();
        for (const link of links) {
            const url = link.href;
            if (url.includes('add-to-cart')) continue;
            if (seen.has(url)) continue;
            seen.add(url);
            items.push({ url });
        }
    }
    return items;
}"""


def get_wc_total_pages(page) -> int:
    """Extract total pages from WooCommerce pagination."""
    try:
        return page.evaluate(r"""() => {
            let maxPage = 1;
            // Primary: parse result count "Showing 1-48 of 921 results"
            const countEl = document.querySelector('.woocommerce-result-count');
            if (countEl) {
                const match = countEl.textContent.match(/of\s+(\d+)/);
                if (match) {
                    const total = parseInt(match[1]);
                    maxPage = Math.ceil(total / 48);
                }
            }
            // Fallback: individual page number <a> links (not the parent <ul>)
            if (maxPage <= 1) {
                const pageLinks = document.querySelectorAll('ul.page-numbers > li > a, ul.page-numbers > li > span');
                for (const link of pageLinks) {
                    const text = link.textContent.trim();
                    const num = parseInt(text);
                    if (/^\d+$/.test(text) && num > maxPage) maxPage = num;
                }
            }
            return maxPage;
        }""")
    except Exception:
        return 1


def extract_offerings_page(page, url: str) -> list[dict]:
    """Extract product URLs from a single offerings listing page."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout on %s, trying domcontentloaded", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)

    # Wait for products to render
    try:
        page.wait_for_selector('a[href*="/product/"]', timeout=15_000)
    except PlaywrightTimeout:
        log.warning("No product links found on %s", url)
        return []

    return page.evaluate(OFFERINGS_LISTING_JS)


def scrape_offerings(page) -> list[dict]:
    """Scrape all paginated offerings pages. Returns list of {url, bag_type}."""
    log.info("Scraping /offerings/ page 1...")
    items = extract_offerings_page(page, OFFERINGS_URL)
    total_pages = get_wc_total_pages(page)
    log.info("Found %d products on page 1, %d total pages.", len(items), total_pages)

    empty_streak = 0
    for p in range(2, total_pages + 1):
        url = f"{OFFERINGS_URL}page/{p}/"
        log.info("Loading offerings page %d/%d...", p, total_pages)
        page_items = extract_offerings_page(page, url)
        log.info("  Found %d products on page %d.", len(page_items), p)
        if not page_items:
            empty_streak += 1
            if empty_streak >= 2:
                log.info("Two consecutive empty pages — stopping pagination.")
                break
        else:
            empty_streak = 0
            items.extend(page_items)
        time.sleep(0.5)

    for item in items:
        item["bag_type"] = "Full Bag"

    log.info("Offerings: %d total product URLs.", len(items))
    return items


# ---------- Listing: Single-page (Crown Jewels, Royal Gems) ----------

SINGLE_PAGE_JS = """() => {
    const items = [];
    const seen = new Set();
    const links = document.querySelectorAll('a[href*="/product/"]');
    for (const link of links) {
        const url = link.href;
        if (url.includes('add-to-cart')) continue;
        if (seen.has(url)) continue;
        seen.add(url);
        items.push({ url });
    }
    return items;
}"""


def scrape_single_page(page, url: str, bag_type: str) -> list[dict]:
    """Scrape a single-page listing (Crown Jewels or Royal Gems)."""
    log.info("Scraping %s...", url)
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout on %s, trying domcontentloaded", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)

    try:
        page.wait_for_selector('a[href*="/product/"]', timeout=15_000)
    except PlaywrightTimeout:
        log.warning("No product links found on %s", url)
        return []

    items = page.evaluate(SINGLE_PAGE_JS)
    for item in items:
        item["bag_type"] = bag_type

    log.info("%s: found %d products.", bag_type, len(items))
    return items


# ---------- Staff picks ----------

def scrape_staff_picks(page) -> set[str]:
    """Scrape staff picks page and return set of product URLs."""
    log.info("Scraping staff picks...")
    try:
        page.goto(STAFF_PICKS_URL, wait_until="networkidle", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Timeout on staff picks, trying domcontentloaded")
        page.goto(STAFF_PICKS_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3000)

    try:
        page.wait_for_selector('a[href*="/product/"]', timeout=15_000)
    except PlaywrightTimeout:
        log.warning("No product links on staff picks page.")
        return set()

    urls = page.evaluate("""() => {
        const urls = new Set();
        const links = document.querySelectorAll('a[href*="/product/"]');
        for (const link of links) {
            const url = link.href;
            if (!url.includes('add-to-cart')) urls.add(url);
        }
        return [...urls];
    }""")

    log.info("Staff picks: %d URLs.", len(urls))
    return set(urls)


# ---------- Detail page extraction ----------

# Using raw string (r"""...""") so JS regex backslashes pass through correctly.
DETAIL_JS = r"""() => {
    const text = document.body.innerText;
    const result = {};

    // 1. JSON-LD structured data (most reliable for price/weight/category)
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const script of scripts) {
        try {
            const data = JSON.parse(script.textContent);
            const graph = data['@graph'] || [data];
            for (const item of graph) {
                if (item['@type'] === 'Product') {
                    result.jsonld_name = item.name || '';
                    result.jsonld_sku = item.sku || '';
                    result.jsonld_category = item.category || '';
                    if (item.weight) {
                        result.jsonld_weight = item.weight.value || '';
                        result.jsonld_weight_unit = item.weight.unitCode || '';
                    }
                    if (item.offers) {
                        const offer = Array.isArray(item.offers)
                            ? item.offers[0] : item.offers;
                        result.jsonld_price = offer.price || '';
                        result.jsonld_availability = offer.availability || '';
                    }
                }
            }
        } catch (e) {}
    }

    // 2. Product title from DOM h1
    const h1 = document.querySelector('h1');
    result.title = h1 ? h1.textContent.trim() : '';

    // 3. Price display text
    const pplMatch = text.match(/\$([0-9,.]+)\s*per\s*pound/i);
    result.price_per_lb_text = pplMatch ? pplMatch[1] : '';

    const ppbMatch = text.match(/\$([0-9,.]+)\s*per\s*box/i);
    result.price_per_box_text = ppbMatch ? ppbMatch[1] : '';

    // 4. Flavor Profile (line following the label)
    const flavorMatch = text.match(/Flavor Profile\s*\n+([^\n]+)/);
    result.flavor_text = flavorMatch ? flavorMatch[1].trim() : '';

    // 5. Inventory: "Bags\n7" or "Boxes\n31" or "7 Bags"
    const invMatch = text.match(/(Bags|Boxes)\s*\n*(\d+)/);
    if (invMatch) {
        result.inventory = invMatch[2] + ' ' + invMatch[1];
    } else {
        const invMatch2 = text.match(/(\d+)\s+(Bags?|Boxes?)/);
        result.inventory = invMatch2 ? invMatch2[0].trim() : '';
    }

    // 6. Warehouse
    const whMatch = text.match(/Warehouses?\s*\n+([^\n]+)/);
    result.warehouse = whMatch ? whMatch[1].trim() : '';

    // 7. Spec fields extracted from page text (label\nvalue pattern)
    const growerMatch = text.match(/Grower\s*\n+([\s\S]*?)(?=\nAltitude|\nVariety|\nRegion|\nProcess|\nRelated|\nHarvest|\n\n)/);
    result.grower = growerMatch ? growerMatch[1].replace(/\n/g, ' ').trim() : '';

    const altMatch = text.match(/Altitude\s*\n+([^\n]+)/);
    result.altitude = altMatch ? altMatch[1].trim() : '';

    const varMatch = text.match(/Variet(?:y|ies)\s*\n+([^\n]+)/);
    result.variety = varMatch ? varMatch[1].trim() : '';

    const regMatch = text.match(/Region\s*\n+([^\n]+)/);
    result.region = regMatch ? regMatch[1].trim() : '';

    // Process: stop at next known label or double newline
    const procMatch = text.match(/Process\s*\n+([\s\S]*?)(?=\nRelated|\nHarvest|\nCertification|\nShop|\nCrown|\n\n)/);
    result.process = procMatch ? procMatch[1].replace(/\n/g, ' ').trim() : '';

    // Harvest (bonus field)
    const harvestMatch = text.match(/Harvest\s*\n+([^\n]+)/);
    result.harvest = harvestMatch ? harvestMatch[1].trim() : '';

    // Certification
    const certMatch = text.match(/Certification\s*\n+([^\n]+)/);
    result.certifications = certMatch ? certMatch[1].trim() : '';

    // 8. Position: parse from title suffix or page text
    // Titles often end with "SPOT DUPUYHOU" or "SPOT RCWHSE"
    const posMatch = text.match(/\b(SPOT|FORWARD|AFLOAT)\b/i);
    result.position = posMatch ? posMatch[1] : '';

    // 8b. Quote-only detection (no standard add-to-cart)
    result.is_quote_only = /get\s+a\s+quote/i.test(text);

    // 9. Tab pane content (for "About This Coffee" / "Overview" sections)
    const panes = document.querySelectorAll('.tab-pane, [role="tabpanel"]');
    const paneTexts = [];
    for (const pane of panes) {
        const t = pane.innerText.trim();
        if (t.length > 30) paneTexts.push(t);
    }
    result.pane_texts = paneTexts;

    // 10. Infogram embeds per tab
    const infogramUrls = {};
    // Method 1: Script tags <script id="infogram_0_{uuid}" title="Taste: CJ1536 ...">
    const infoScripts = document.querySelectorAll('script[id^="infogram_0_"]');
    for (const script of infoScripts) {
        const uuid = script.id.replace('infogram_0_', '');
        const title = script.getAttribute('title') || '';
        const tabName = title.split(':')[0].trim();
        if (tabName && uuid) {
            infogramUrls[tabName] = uuid;
        }
    }
    // Method 2: Iframes with infogram.com src (fallback)
    if (Object.keys(infogramUrls).length === 0) {
        const iframes = document.querySelectorAll('iframe[src*="infogram.com"]');
        for (const iframe of iframes) {
            const src = iframe.src || '';
            const uuidMatch = src.match(/infogram\.com\/([a-f0-9-]+)/);
            if (uuidMatch) {
                const parent = iframe.closest('[role="tabpanel"], .tab-pane');
                let tabName = '';
                if (parent) {
                    tabName = parent.innerText.substring(0, 30).trim().split('\n')[0];
                }
                infogramUrls[tabName || ('tab_' + Object.keys(infogramUrls).length)] = uuidMatch[1];
            }
        }
    }
    result.infogram_urls = infogramUrls;

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

    # Wait for product content
    try:
        page.wait_for_selector('h1', timeout=10_000)
    except PlaywrightTimeout:
        log.warning("No h1 found on %s", url)

    try:
        return page.evaluate(DETAIL_JS)
    except Exception as e:
        log.warning("Error extracting detail from %s: %s", url, e)
        return {}


# ---------- Record building ----------

def parse_price(s: str) -> str:
    """Clean a price string to a numeric value."""
    if not s:
        return ""
    s = s.replace(",", "").replace("$", "").strip()
    try:
        return f"{float(s):.2f}"
    except ValueError:
        return ""


def compute_price_per_lb(total_price: str, weight_lbs: str) -> str:
    """Calculate price per pound from total price and weight."""
    try:
        total = float(total_price)
        weight = float(weight_lbs)
        if weight > 0:
            return f"{total / weight:.2f}"
    except (ValueError, TypeError):
        pass
    return ""


# Known coffee-producing countries for title-based extraction
COFFEE_COUNTRIES = [
    "Ethiopia", "Kenya", "Colombia", "Brazil", "Guatemala", "Costa Rica",
    "Honduras", "El Salvador", "Nicaragua", "Mexico", "Peru", "Bolivia",
    "Ecuador", "Panama", "Jamaica", "Haiti", "Dominican Republic",
    "Rwanda", "Burundi", "Tanzania", "Uganda", "DR Congo", "Congo",
    "Malawi", "Zambia", "Zimbabwe", "Cameroon",
    "Indonesia", "Sumatra", "Java", "Bali", "Sulawesi", "Papua New Guinea",
    "India", "Vietnam", "Myanmar", "Thailand", "Laos", "China", "Yemen",
    "Nepal", "Philippines", "East Timor", "Timor-Leste",
    "Hawaii", "Puerto Rico",
]


def extract_country_from_title(title: str) -> str:
    """Try to find a coffee-producing country in the product title."""
    for country in COFFEE_COUNTRIES:
        if country.lower() in title.lower():
            return country
    return ""


def build_record(
    listing: dict, detail: dict, staff_pick_urls: set,
    infogram_data: dict | None = None, llm_data=None,
) -> dict:
    """Merge listing data with detail-page + infogram + LLM data into a flat record."""
    record = {col: "" for col in CSV_COLUMNS}
    url = listing["url"]
    record["url"] = url
    # Bag type: prefer listing source, but use JSON-LD category as authoritative override
    bag_type = listing.get("bag_type", "")
    jsonld_cat = detail.get("jsonld_category", "") if detail else ""
    if "Crown Jewel" in jsonld_cat:
        bag_type = "Crown Jewel"
    elif "Royal Gem" in jsonld_cat or "50 lb" in jsonld_cat:
        bag_type = "Royal Gem"
    elif "Full Size" in jsonld_cat:
        bag_type = "Full Bag"
    record["bag_type"] = bag_type
    record["is_staff_pick"] = "Yes" if url in staff_pick_urls else ""

    if not detail:
        return record

    # Name: prefer clean h1 title over JSON-LD (which has suffixes)
    record["name"] = detail.get("title", "") or detail.get("jsonld_name", "")

    # Total price from JSON-LD
    total_price = parse_price(detail.get("jsonld_price", ""))
    record["total_price"] = f"${total_price}" if total_price else ""

    # Weight / bag size from JSON-LD
    weight = detail.get("jsonld_weight", "")
    weight_unit = detail.get("jsonld_weight_unit", "")
    if weight:
        record["bag_size"] = f"{weight} lb" if weight_unit == "LBR" else f"{weight} {weight_unit}"

    # Price per lb: try page text first, then compute from total/weight
    # Skip pricing entirely for quote-only products
    if detail.get("is_quote_only"):
        ppl_text = ""
        ppb_text = ""
        has_visible_price = False
    else:
        # Only use JSON-LD price as fallback if the page shows a visible price
        # (products with only "Get a Quote" have JSON-LD prices but no visible pricing)
        ppl_text = parse_price(detail.get("price_per_lb_text", ""))
        ppb_text = detail.get("price_per_box_text", "")
        has_visible_price = bool(ppl_text or ppb_text)

    if ppl_text:
        record["price_per_lb"] = f"${ppl_text}"
    elif has_visible_price and total_price and weight:
        computed = compute_price_per_lb(total_price, weight)
        if computed:
            record["price_per_lb"] = f"${computed}"

    # For Crown Jewels/Royal Gems: compute from box price / weight
    if not record["price_per_lb"] and ppb_text and weight:
        box_price = parse_price(ppb_text)
        if box_price:
            computed = compute_price_per_lb(box_price, weight)
            if computed:
                record["price_per_lb"] = f"${computed}"

    # Structured fields from page text regex
    record["region"] = detail.get("region", "")
    record["altitude"] = detail.get("altitude", "")
    record["grower"] = detail.get("grower", "")
    record["variety"] = detail.get("variety", "")
    record["process"] = detail.get("process", "")
    record["warehouse"] = detail.get("warehouse", "")
    record["certifications"] = detail.get("certifications", "")
    record["position"] = detail.get("position", "")
    record["inventory"] = detail.get("inventory", "")

    # Country: try to extract from title or region
    name = record["name"]
    jsonld_name = detail.get("jsonld_name", "")
    record["country"] = (
        extract_country_from_title(name)
        or extract_country_from_title(jsonld_name)
        or extract_country_from_title(record["region"])
    )

    # Flavor notes
    record["flavor_notes"] = detail.get("flavor_text", "")

    # About text: pick from tab panes
    # For Full Bags: pane[0] has specs (Grower/Altitude/etc), pane[1] has about text
    # For Crown Jewels: pane[0] has Overview (the about text)
    pane_texts = detail.get("pane_texts", [])
    about_text = ""
    for pane in pane_texts:
        # Skip panes that are just the specs section (start with "Grower\n")
        if pane.strip().startswith("Grower"):
            continue
        # Skip very short panes
        if len(pane) < 50:
            continue
        # Use the first substantial non-specs pane as the about text
        about_text = pane.strip()
        break
    record["about"] = about_text

    # Tab-specific texts (matched by prefix, not index)
    for pane in pane_texts:
        stripped = pane.strip()
        if stripped.startswith("Overview") and not record["overview_text"]:
            record["overview_text"] = stripped
        elif stripped.startswith("Taste") and not record["taste_text"]:
            record["taste_text"] = stripped
        elif stripped.startswith("Source") and not record["source_text"]:
            record["source_text"] = stripped

    # Infogram data (word cloud + gauges)
    if infogram_data:
        wc = infogram_data.get("word_cloud")
        if wc:
            record["word_cloud"] = wc  # dict; serialized to JSON string in write_csv
        for gauge_field in ("sweetness", "acidity", "viscosity", "balance"):
            if gauge_field in infogram_data:
                record[gauge_field] = str(infogram_data[gauge_field])

    # LLM-extracted fields as fallback for empty regex results
    if llm_data:
        if not record["region"] and llm_data.region:
            record["region"] = llm_data.region
        if not record["altitude"] and llm_data.altitude:
            record["altitude"] = llm_data.altitude
        if not record["variety"] and llm_data.variety:
            record["variety"] = ", ".join(llm_data.variety)
        if not record["process"] and llm_data.process:
            record["process"] = llm_data.process
        if not record["grower"] and llm_data.grower:
            record["grower"] = llm_data.grower

    return record


# ---------- Output writers ----------

def write_csv(records: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_records = []
    for r in records:
        row = dict(r)
        # Serialize dict/list fields to JSON strings for CSV
        if isinstance(row.get("word_cloud"), dict):
            row["word_cloud"] = json.dumps(row["word_cloud"], ensure_ascii=False)
        csv_records.append(row)
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(csv_records)
    log.info("Wrote %d coffees to %s", len(records), CSV_FILE)


def write_json(records: list[dict]) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    filtered = [
        r for r in records
        if r.get("price_per_lb")
        and "Shanghai" not in r.get("warehouse", "")
    ]
    # Deduplicate: same coffee at same warehouse and price is a duplicate lot listing
    seen = set()
    deduped = []
    for r in filtered:
        key = (r.get("name", ""), r.get("warehouse", ""), r.get("price_per_lb", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    log.info("Deduped %d → %d coffees", len(filtered), len(deduped))
    filtered = deduped
    JSON_FILE.write_text(
        json.dumps(filtered, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info("Wrote %d coffees to %s (filtered from %d)", len(filtered), JSON_FILE, len(records))


# ---------- Main ----------

def main():
    cache = load_cache()
    infogram_cache = load_infogram_cache()

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
        all_listings = []

        # Offerings (paginated)
        all_listings.extend(scrape_offerings(page))

        # Crown Jewels (single page)
        all_listings.extend(
            scrape_single_page(page, CROWN_JEWELS_URL, "Crown Jewel")
        )

        # Royal Gems (single page)
        all_listings.extend(
            scrape_single_page(page, ROYAL_GEMS_URL, "Royal Gem")
        )

        # Staff picks (for flagging)
        staff_pick_urls = scrape_staff_picks(page)

        # Deduplicate by URL, preferring specific bag_type over "Full Bag"
        BAG_TYPE_PRIORITY = {"Crown Jewel": 0, "Royal Gem": 1, "Full Bag": 2}
        seen = {}
        for item in all_listings:
            url = item["url"]
            if url not in seen:
                seen[url] = item
            else:
                # Prefer more specific bag type
                existing = seen[url]
                if BAG_TYPE_PRIORITY.get(item["bag_type"], 9) < BAG_TYPE_PRIORITY.get(existing["bag_type"], 9):
                    seen[url] = item
        unique_listings = list(seen.values())

        log.info(
            "Total unique products: %d (from %d raw listings)",
            len(unique_listings), len(all_listings),
        )

        # Step 2: For each product, use cache or scrape detail
        # Re-scrape Crown Jewels that don't have infogram_urls yet
        detail_results = {}
        cache_hits = 0
        cache_misses = 0
        total = len(unique_listings)

        for i, listing in enumerate(unique_listings, 1):
            url = listing["url"]
            cached = cache.get(url)
            is_crown_jewel = listing.get("bag_type") == "Crown Jewel"
            needs_rescrape = (
                cached and is_crown_jewel and "infogram_urls" not in cached
            )

            if cached and not needs_rescrape:
                log.info("[%d/%d] CACHED: %s", i, total, url)
                detail = cached
                cache_hits += 1
            else:
                tag = "RESCRAPE" if needs_rescrape else "SCRAPING"
                log.info("[%d/%d] %s: %s", i, total, tag, url)
                detail = extract_detail_data(page, url)
                cache[url] = detail
                cache_misses += 1
                time.sleep(1)  # polite delay

            detail_results[url] = detail

        browser.close()

    log.info("Cache: %d hits, %d misses", cache_hits, cache_misses)
    save_cache(cache)

    # Step 3: Fetch Infogram data for products with infogram_urls
    log.info("Fetching Infogram data...")
    infogram_fetched = 0
    for listing in unique_listings:
        url = listing["url"]
        if url in infogram_cache:
            continue
        detail = detail_results.get(url, {})
        infogram_urls = detail.get("infogram_urls", {})
        # Find Taste tab UUID (key starts with "Taste")
        taste_uuid = None
        for tab_name, uuid in infogram_urls.items():
            if tab_name.lower().startswith("taste"):
                taste_uuid = uuid
                break
        if taste_uuid:
            data = fetch_infogram_data(taste_uuid)
            if data:
                infogram_cache[url] = data
                infogram_fetched += 1
                if infogram_fetched % 10 == 0:
                    save_infogram_cache(infogram_cache)
            time.sleep(0.3)  # polite delay

    save_infogram_cache(infogram_cache)
    log.info("Infogram: %d new fetches, %d cached", infogram_fetched,
             len(infogram_cache) - infogram_fetched)

    # Step 4: LLM extraction for products with tab text (Crown Jewels)
    llm_items = []
    for listing in unique_listings:
        url = listing["url"]
        detail = detail_results.get(url, {})
        pane_texts = detail.get("pane_texts", [])
        if len(pane_texts) < 3:
            continue
        # Concatenate Overview + Source text for LLM
        overview = ""
        source = ""
        for pane in pane_texts:
            stripped = pane.strip()
            if stripped.startswith("Overview"):
                overview = stripped
            elif stripped.startswith("Source"):
                source = stripped
        text = f"{overview}\n\n{source}".strip()
        if len(text) > 100:
            name = detail.get("title", "") or detail.get("jsonld_name", "")
            llm_items.append({"url": url, "name": name, "text": text})

    llm_results = {}
    if llm_items:
        log.info("Running LLM extraction on %d products...", len(llm_items))
        try:
            from extract_royal import extract_royal_products
            llm_results = asyncio.run(extract_royal_products(llm_items))
        except ImportError:
            log.warning(
                "extract_royal not available (install google-genai and pydantic). "
                "Skipping LLM extraction."
            )
        except Exception as e:
            log.warning("LLM extraction failed: %s", e)
    else:
        log.info("No products with sufficient tab text for LLM extraction.")

    # Step 5: Build records with all data sources merged
    records = []
    for listing in unique_listings:
        url = listing["url"]
        detail = detail_results.get(url, {})
        info_data = infogram_cache.get(url)
        llm_data = llm_results.get(url)
        record = build_record(listing, detail, staff_pick_urls, info_data, llm_data)
        records.append(record)

    # Step 6: Watchlist producer matching
    match_products(records, corpus_fields=[
        "name", "grower", "country", "region", "about",
        "overview_text", "source_text",
    ])

    # Use LLM watchlist_match as fallback for unmatched products
    from match_watchlist import load_watchlist as _load_wl, _normalize as _wl_normalize
    _wl_entries = _load_wl()
    _wl_by_name = {
        _wl_normalize(p.get("producer_name", "")): p
        for p in _wl_entries if p.get("producer_name")
    }
    llm_fallback_count = 0
    for rec in records:
        if not rec.get("watchlist_match"):
            url = rec.get("url", "")
            llm = llm_results.get(url)
            if llm and getattr(llm, "watchlist_match", None):
                producer = _wl_by_name.get(_wl_normalize(llm.watchlist_match))
                if producer:
                    rec["watchlist_match"] = producer.get("producer_name", "")
                    rec["watchlist_farm"] = producer.get("farm_or_station", "")
                    rec["watchlist_tier"] = producer.get("tier", "")
                    rec["watchlist_credential_type"] = producer.get("credential_type", "")
                    rec["watchlist_credential_detail"] = producer.get("credential_detail", "")
                    rec["watchlist_notes"] = producer.get("notes", "")
                    rec["watchlist_url"] = producer.get("direct_sales_url", "")
                    llm_fallback_count += 1
    if llm_fallback_count:
        log.info("Watchlist LLM fallback: %d additional matches", llm_fallback_count)

    # Step 7: Write all outputs
    write_csv(records)
    write_json(records)

    log.info("Done. %d total coffees.", len(records))


if __name__ == "__main__":
    main()
