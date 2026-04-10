"""
Daily CME Futures Price Scraper (Multi-Product)
================================================
Scrapes settlement data from CME Group's API for multiple products.
Products are configured in products.json.
All scraped data is appended into a single CSV file in the data/ directory.

CSV columns: Date, Product, Month, Open, High, Low, Last, Change, Settle,
             Est. Volume, Prior Day OI

Usage:
    python scraper.py                    # Scrape all products once
    python scraper.py --cron             # Run on schedule (12:30 PM Bangkok time)
    python scraper.py --discover <URL>   # Auto-discover product_id from a CME page
"""

import csv
import json
import sys
import time
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# ─── Configuration ───────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
PRODUCTS_FILE = BASE_DIR / "products.json"
DATA_DIR = BASE_DIR / "data"
ALL_PRODUCTS_CSV = DATA_DIR / "cme_daily_prices.csv"

TRADE_DATE_URL = (
    "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/TradeDate/{product_id}"
    "?isProtected"
)
SETTLEMENTS_URL = (
    "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/{product_id}/FUT"
    "?strategy=DEFAULT&tradeDate={trade_date}&pageSize=500&isProtected"
)

CSV_HEADERS = [
    "Date",
    "Product",
    "Month",
    "Open",
    "High",
    "Low",
    "Last",
    "Change",
    "Settle",
    "Est. Volume",
    "Prior Day OI",
]

FIELD_MAP = [
    "month",
    "open",
    "high",
    "low",
    "last",
    "change",
    "settle",
    "volume",
    "openInterest",
]

# Bangkok timezone (UTC+7)
BANGKOK_TZ = timezone(timedelta(hours=7))
MAX_RETRIES = 3


def log(msg):
    """Print a timestamped log message."""
    now_str = datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S ICT")
    print(f"[{now_str}] {msg}")


def load_products():
    """Load product list from products.json."""
    if not PRODUCTS_FILE.exists():
        log(f"[ERROR] {PRODUCTS_FILE} not found")
        sys.exit(1)
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as f:
        products = json.load(f)
    if not products:
        log("[ERROR] No products configured in products.json")
        sys.exit(1)
    return products


def create_browser(playwright):
    """Create a browser context with anti-detection settings."""
    browser = playwright.chromium.launch(headless=True, channel="msedge")
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
        ),
    )
    context.add_init_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
    )
    page = context.new_page()
    return browser, page


def fetch_json(page, url, label="data"):
    """Fetch a URL and parse as JSON with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = page.goto(url, timeout=30000)
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}")
            return json.loads(page.inner_text("body"))
        except Exception as e:
            log(f"  Attempt {attempt}/{MAX_RETRIES} to fetch {label} failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(random.uniform(2, 5))


# ─── Scrape One Product ─────────────────────────────────────────
def scrape_product(page, product):
    """Scrape settlement data for a single product."""
    name = product["name"]
    pid = product["product_id"]
    slug = product["slug"]

    log(f"--- Scraping: {name} (ID: {pid}) ---")

    # Step 1: Get latest trade date
    trade_date_url = TRADE_DATE_URL.format(product_id=pid)
    trade_dates = fetch_json(page, trade_date_url, label=f"{name} trade date")
    latest_date = trade_dates[0][0]
    log(f"  Trade date: {latest_date}")

    # Step 2: Fetch settlement data
    settlements_url = SETTLEMENTS_URL.format(
        product_id=pid, trade_date=latest_date
    )
    data = fetch_json(page, settlements_url, label=f"{name} settlements")

    settlements = data.get("settlements", [])
    if not settlements:
        raise ValueError(f"No settlement data for {name}")

    # Step 3: Extract first row
    first = settlements[0]
    row_values = [first.get(field, "-") for field in FIELD_MAP]

    # Step 4: Write to CSV
    today = datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d")
    csv_row = [today, slug] + row_values

    log(f"  Data: {csv_row}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = ALL_PRODUCTS_CSV.exists()

    with open(ALL_PRODUCTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(CSV_HEADERS)
        writer.writerow(csv_row)

    log(f"  [OK] -> {ALL_PRODUCTS_CSV.name}")
    return csv_row


# ─── Scrape All Products ────────────────────────────────────────
def scrape_all():
    """Scrape settlement data for all configured products."""
    products = load_products()
    log(f"Loaded {len(products)} product(s) from {PRODUCTS_FILE.name}")

    results = {}
    errors = []

    with sync_playwright() as p:
        browser, page = create_browser(p)
        try:
            for product in products:
                try:
                    csv_row = scrape_product(page, product)
                    results[product["name"]] = csv_row
                    # Polite delay between products
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    log(f"  [ERROR] Failed to scrape {product['name']}: {e}")
                    errors.append(product["name"])
        finally:
            browser.close()

    # Summary
    log("=" * 50)
    log(f"Completed: {len(results)}/{len(products)} product(s)")
    if errors:
        log(f"Failed: {', '.join(errors)}")

    return results


# ─── Auto-Discover Product ID ───────────────────────────────────
def discover_product_id(url):
    """
    Auto-discover the CME product ID from a settlements page URL.
    Navigates to the page, intercepts API calls, and extracts the ID.
    """
    log(f"Discovering product ID from: {url}")

    with sync_playwright() as p:
        browser, page = create_browser(p)
        found_ids = []

        # Intercept API requests to capture product ID
        def handle_request(request):
            req_url = request.url
            if "/Settlements/Futures/TradeDate/" in req_url:
                # Extract: .../TradeDate/470?... -> 470
                parts = req_url.split("/TradeDate/")[1]
                pid = parts.split("?")[0]
                found_ids.append(pid)

        page.on("request", handle_request)

        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(3000)
        except Exception:
            pass  # Page might timeout but we may have captured the ID
        finally:
            browser.close()

    if found_ids:
        product_id = found_ids[0]
        # Extract product name from URL slug
        slug = url.rstrip("/").split("/")[-1].replace(".settlements.html", "")
        slug_name = slug.replace("-", " ").title()

        log(f"")
        log(f"  Product ID : {product_id}")
        log(f"  Name (guess): {slug_name}")
        log(f"")
        log(f"Add this to products.json:")
        print()
        entry = {
            "name": slug_name,
            "product_id": int(product_id),
            "csv_name": f"{slug.replace('-', '_')}_daily.csv",
        }
        print(json.dumps(entry, indent=2))
        print()
        return product_id
    else:
        log("[ERROR] Could not discover product ID. Make sure the URL is a CME settlements page.")
        return None


# ─── Cron Scheduler ──────────────────────────────────────────────
def run_cron():
    """Run scraper on schedule: every day at 12:30 PM Bangkok time."""
    import schedule

    products = load_products()

    print("=" * 55)
    print("  CME Daily Price Scraper (Multi-Product)")
    print(f"  Products: {len(products)}")
    for prod in products:
        print(f"    - {prod['name']} (ID: {prod['product_id']})")
    print("  Schedule: Every day at 12:30 PM (Asia/Bangkok)")
    print("=" * 55)

    def job():
        try:
            scrape_all()
        except Exception as e:
            log(f"[ERROR] Scheduled scrape failed: {e}")

    schedule.every().day.at("12:30").do(job)

    log("Cron job started. Waiting for 12:30 PM...")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ─── Main Entry Point ────────────────────────────────────────────
def main():
    if "--discover" in sys.argv:
        idx = sys.argv.index("--discover")
        if idx + 1 >= len(sys.argv):
            print("Usage: python scraper.py --discover <CME_SETTLEMENTS_URL>")
            print()
            print("Example:")
            print("  python scraper.py --discover https://www.cmegroup.com/markets/agriculture/lumber-and-softs/sugar-no11.settlements.html")
            sys.exit(1)
        url = sys.argv[idx + 1]
        discover_product_id(url)
    elif "--cron" in sys.argv:
        run_cron()
    else:
        try:
            scrape_all()
            print("Done!")
        except Exception as e:
            print(f"[ERROR] Scrape failed: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
