"""
bina.az Scraper — Playwright ilə TAM brauzer-driven yanaşma.

YENİ (bu versiyada):
- .env faylı bu faylın da başında yüklənir (tək başına "python scrape.py"
  ilə işə salsan belə USE_AZURE / connection string düzgün oxunsun deyə).
- Scrape etməzdən əvvəl processed_ids metadata faylı oxunur.
- Yalnız YENİ və ya QİYMƏTİ DƏYİŞMİŞ elanlar Bronze-a yazılır.
  Heç nə dəyişməyən elanlar Bronze-a təkrar yazılmır (duplicate yoxdur).
- processed_ids faylı hər run-dan sonra yenilənir (first_seen / last_seen /
  last_updated_at / price).
- Azure Data Lake ilə lokal fayl sistemi arasında keçid
  azure_lake_client.py modulu vasitəsilə şəffaf şəkildə edilir
  (USE_AZURE env dəyişəni ilə idarə olunur — sənin .env-də USE_AZURE=1).

Struktur:
    Bina.az → Scraper → processed_ids yoxlanışı → Bronze-a yaz (yalnız yeni/dəyişmiş)
"""

# Bu skript tək başına ("python scrape.py") işə salına bilər, ona görə
# .env-i burda da yükləyirik — main.py-dan işə düşəndə artıq yüklənib
# olacaq, amma bu, təhlükəsizlik üçün əlavə bir sığortadır (iki dəfə
# çağırmaq zərər vermir).
from dotenv import load_dotenv
load_dotenv()

import json
import time
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

from metadata_manager import load_processed_ids, save_processed_ids, classify_and_update
from azure_lake_client import (
    get_bronze_dir,
    get_metadata_path,
    download_metadata,
    upload_metadata,
    upload_bronze_file,
)

# ======================================================
# CONFIG
# ======================================================
BASE_URL = "https://bina.az/alqi-satqi"
MAX_ITEMS = 2000
SCROLL_PAUSE = 2.0
MAX_SCROLLS_WITHOUT_NEW = 5


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


def collect_all_items(max_items=MAX_ITEMS):
    """Yalnız brauzer-driven scraping addımı — bütün görünən elanları toplayır."""
    collected = {}
    scrolls_without_new = 0
    total_count_reported = None

    with sync_playwright() as p:
        # DEBUG: brauzeri gözlə göstəririk ki, nə baş verdiyini görək.
        # Problem tapılandan sonra headless=True et.
        browser = p.chromium.launch(headless=False)
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
                print("[DEBUG] Bu cavabda yeni elan yoxdur (artıq toplanmışdı)")

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

    return list(collected.values())


def run_scraper(max_items=MAX_ITEMS):
    """
    Tam axın:
      1. processed_ids-i yüklə (lazım olsa Azure-dan endir)
      2. bina.az-ı scrape et
      3. yeni / dəyişmiş elanları ayır (duplicate-ləri Bronze-a yazma)
      4. yalnız yeni + dəyişmiş elanları Bronze-a yaz
      5. processed_ids-i yenilə və saxla (lazım olsa Azure-a yüklə)
    """
    # 1. Metadata-nı hazırla
    download_metadata()  # Azure rejimində no-op deyil, lokal rejimdə heç nə etmir
    metadata_path = get_metadata_path()
    processed_df = load_processed_ids(metadata_path)
    print(f"[DEBUG] processed_ids-də {len(processed_df)} mövcud ID var")

    # 2. Scrape et
    all_items = collect_all_items(max_items=max_items)
    print(f"[DEBUG] Bu run-da toplanan cəmi elan: {len(all_items)}")

    # 3. Yeni / dəyişmiş / dəyişməyən elanları ayır
    new_items, updated_items, unchanged_ids, updated_df = classify_and_update(all_items, processed_df)
    print(f"[DEBUG] Yeni: {len(new_items)} | Dəyişmiş: {len(updated_items)} | Dəyişməyən: {len(unchanged_ids)}")

    to_write = new_items + updated_items

    out_path = None
    if to_write:
        bronze_dir = get_bronze_dir()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = bronze_dir / f"listings_{timestamp}.json"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(to_write, f, ensure_ascii=False, indent=2)

        print(f"[TAMAMLANDI] {len(to_write)} elan Bronze-a yazıldı -> {out_path}")
        upload_bronze_file(out_path)  # Azure rejimində yüklənir, lokalda no-op
    else:
        print("[TAMAMLANDI] Yeni və ya dəyişmiş elan yoxdur, Bronze-a yazılmadı")

    # 5. processed_ids-i yadda saxla
    save_processed_ids(updated_df, metadata_path)
    upload_metadata()  # Azure rejimində yüklənir, lokalda no-op
    print(f"[DEBUG] processed_ids yeniləndi, cəmi {len(updated_df)} ID izlənir")

    return out_path


if __name__ == "__main__":
    run_scraper(max_items=MAX_ITEMS)