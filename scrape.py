"""
bina.az Scraper — Playwright ilə TAM brauzer-driven yanaşma.

Bu versiyada:
- wait_until="domcontentloaded" istifadə olunur (networkidle timeout verirdi)
- Debug print-lər əlavə olunub ki, problem harda olduğu görünsün
- headless=False (debug üçün) — problem tapıldıqdan sonra True-ya qaytar
"""

import json
import time
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

# ======================================================
# CONFIG
# ======================================================
BASE_URL = "https://bina.az/alqi-satqi"
MAX_ITEMS = 500000
SCROLL_PAUSE = 2.0
MAX_SCROLLS_WITHOUT_NEW = 15

# __file__ əsasında mütləq yol qururuq ki, harddan işə salsan da düzgün işləsin
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FOLDER = BASE_DIR.parent / "data_lake" / "bronze"
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


def parse_items(raw_json):
    """GraphQL SearchItems cavabından lazımi sahələri çıxarır."""
    items = []
    connection = raw_json.get("data", {}).get("itemsConnection", {})
    edges = connection.get("edges", [])

    for edge in edges:
        node = edge.get("node", {})
        area = node.get("area") or {}
        price = node.get("price") or {}
        items.append({
            "id": node.get("id"),
            "price": price.get("total"),
            "currency": price.get("currency"),
            "price_per_are": price.get("perAre"),
            "area_value": area.get("value"),
            "area_unit": area.get("units"),
            "rooms": node.get("rooms"),
            "floor": node.get("floor"),
            "floors_total": node.get("floors"),
            "city": (node.get("city") or {}).get("name"),
            "district": (node.get("location") or {}).get("name"),
            "district_full": (node.get("location") or {}).get("fullName"),
            "is_leased": node.get("isLeased"),
            "has_repair": node.get("hasRepair"),
            "has_mortgage": node.get("hasMortgage"),
            "has_bill_of_sale": node.get("hasBillOfSale"),
            "has_internal_loan": node.get("hasInternalLoan"),
            "is_vipped": node.get("isVipped"),
            "is_featured": node.get("isFeatured"),
            "is_business": node.get("isBusiness"),
            "photos_count": node.get("photosCount"),
            "company": (node.get("company") or {}).get("name"),
            "company_type": (node.get("company") or {}).get("targetType"),
            "updated_at": node.get("updatedAt"),
            "path": node.get("path"),
        })

    page_info = connection.get("pageInfo", {})
    total_count = connection.get("totalCount")
    return items, page_info, total_count


def run_scraper(max_items=MAX_ITEMS):
    collected = {}
    scrolls_without_new = 0
    total_count_reported = None

    with sync_playwright() as p:
        # DEBUG: brauzeri gözlə göstəririk ki, nə baş verdiyini görək.
        # Problem tapılandan sonra headless=True et.
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        )
        page = context.new_page()

        def handle_response(response):
            nonlocal total_count_reported
            if "operationName=SearchItems" not in response.url:
                return

            print(f"[DEBUG] SearchItems tutuldu, status={response.status}")

            if response.status != 200:
                print(f"[XƏBƏRDARLIQ] SearchItems status={response.status}")
                try:
                    print(f"[XƏBƏRDARLIQ] Body: {response.text()[:300]}")
                except Exception:
                    pass
                return

            try:
                raw = response.json()
            except Exception as e:
                print(f"[XƏTA] JSON parse alınmadı: {e}")
                try:
                    print(f"[XƏTA] Body: {response.text()[:300]}")
                except Exception:
                    pass
                return

            items, page_info, total_count = parse_items(raw)
            if total_count is not None:
                total_count_reported = total_count

            new_count = 0
            for it in items:
                if it["id"] not in collected:
                    collected[it["id"]] = it
                    new_count += 1

            if new_count > 0:
                print(f"[OK] +{new_count} yeni elan (cəmi: {len(collected)} / {total_count_reported})")
            else:
                print(f"[DEBUG] Bu cavabda yeni elan yoxdur (artıq toplanmışdı)")

        page.on("response", handle_response)

        print("Bina.az açılır...")
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
        print(f"[DEBUG] Səhifə açıldı, başlıq: {page.title()}")

        time.sleep(SCROLL_PAUSE)
        print(f"[DEBUG] İlk gözləmədən sonra toplanan: {len(collected)}")

        while len(collected) < max_items and scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:
            before = len(collected)
            page.mouse.wheel(0, 3000)
            time.sleep(SCROLL_PAUSE)
            after = len(collected)

            if after == before:
                scrolls_without_new += 1
                print(f"[DEBUG] Yeni data gəlmədi ({scrolls_without_new}/{MAX_SCROLLS_WITHOUT_NEW})")
            else:
                scrolls_without_new = 0

        print(f"[DEBUG] Scroll dövrü bitdi. Yekun toplanan: {len(collected)}")
        browser.close()

    all_items = list(collected.values())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_FOLDER / f"listings_{timestamp}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"\n[TAMAMLANDI] Cəmi {len(all_items)} unikal elan -> {out_path}")
    return out_path


if __name__ == "__main__":
    run_scraper(max_items=MAX_ITEMS)
