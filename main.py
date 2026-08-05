"""
bina.az Scraper — Playwright ilə TAM brauzer-driven yanaşma.

YENİ (bu versiyada):
- İKİ kateqoriya scrape olunur: mənzil VƏ həyət evi (ayrı URL-lər).
- Hər elana "property_type" sahəsi əlavə olunur.
- Composite ID (property_type:id) istifadə olunur ki, iki kateqoriya
  arasında ID toqquşması olmasın.
- WATERMARK əsaslı erkən dayanma: artıq bilinən (processed_ids-də olan)
  elanlara ardıcıl KNOWN_STREAK_LIMIT dəfə rast gəlinsə, scroll dayandırılır
  — bu, hər saat MİNLƏRLƏ artıq-tanış elanı təkrar scroll etməkdən qorur,
  resurs sərfiyyatını kəskin azaldır.
"""

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
PROPERTY_SOURCES = {
    "menzil": "https://bina.az/alqi-satqi",
    "heyet_evi": "https://bina.az/baki/alqi-satqi/heyet-evleri",
}

MAX_ITEMS = 10000
SCROLL_PAUSE = 2.0
MAX_SCROLLS_WITHOUT_NEW = 15
KNOWN_STREAK_LIMIT = 40   # ardıcıl bu qədər "artıq tanış" ID görünsə, dayan


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


def collect_category_items(base_url, property_type, known_ids, max_items=MAX_ITEMS,
                            known_streak_limit=KNOWN_STREAK_LIMIT):
    """
    Bir kateqoriya (mənzil YA DA həyət evi) üçün scrape edir.

    known_ids: composite formatda (property_type:id) əvvəlcədən bilinən ID-lərin set-i.
    Watermark: ardıcıl `known_streak_limit` dəfə "artıq bilinən" elana rast
    gəlinsə, deməli yeni ərazi bitib — scroll dayandırılır.
    """
    collected = {}
    scrolls_without_new = 0
    consecutive_known = 0
    total_count_reported = None

    with sync_playwright() as p:
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
            nonlocal total_count_reported, consecutive_known
            if "operationName=SearchItems" not in response.url:
                return

            if response.status != 200:
                print(f"[XƏBƏRDARLIQ] SearchItems status={response.status}")
                return

            try:
                raw = response.json()
            except Exception as e:
                print(f"[XƏTA] JSON parse alınmadı: {e}")
                return

            items, page_info, total_count = parse_items(raw)
            if total_count is not None:
                total_count_reported = total_count

            new_count = 0
            for it in items:
                composite_id = f"{property_type}:{it['id']}"
                it["property_type"] = property_type
                it["id"] = composite_id

                if composite_id in collected:
                    continue

                collected[composite_id] = it
                new_count += 1

                if composite_id in known_ids:
                    consecutive_known += 1
                else:
                    consecutive_known = 0

            if new_count > 0:
                print(f"[OK][{property_type}] +{new_count} yeni (cəmi: {len(collected)} / "
                      f"{total_count_reported}) | ardıcıl-tanış: {consecutive_known}")

        page.on("response", handle_response)

        print(f"[{property_type}] {base_url} açılır...")
        page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(SCROLL_PAUSE)
        print(f"[DEBUG][{property_type}] İlk gözləmədən sonra toplanan: {len(collected)}")

        while len(collected) < max_items and scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:
            if consecutive_known >= known_streak_limit:
                print(f"[DEBUG][{property_type}] {known_streak_limit} ardıcıl tanış elan — "
                      f"yeni ərazi bitdi, scroll dayandırılır.")
                break

            before = len(collected)
            page.mouse.wheel(0, 3000)
            time.sleep(SCROLL_PAUSE)
            after = len(collected)

            if after == before:
                scrolls_without_new += 1
            else:
                scrolls_without_new = 0

        print(f"[DEBUG][{property_type}] Scroll bitdi. Yekun: {len(collected)}")
        browser.close()

    return list(collected.values())


def run_scraper(max_items=MAX_ITEMS):
    """
    Tam axın:
      1. processed_ids-i yüklə
      2. HƏR kateqoriya üçün (mənzil, həyət evi) ayrıca scrape et, watermark ilə
      3. Nəticələri birləşdir, yeni/dəyişmiş elanları ayır
      4. Yalnız yeni+dəyişmiş elanları Bronze-a yaz
      5. processed_ids-i yenilə və saxla
    """
    download_metadata()
    metadata_path = get_metadata_path()
    processed_df = load_processed_ids(metadata_path)
    known_ids = set(processed_df["id"].astype(str)) if len(processed_df) > 0 else set()
    print(f"[DEBUG] processed_ids-də {len(known_ids)} mövcud ID var")

    all_items = []
    for property_type, base_url in PROPERTY_SOURCES.items():
        print(f"[DEBUG] === {property_type} scrape başlayır ===")
        items = collect_category_items(
            base_url=base_url,
            property_type=property_type,
            known_ids=known_ids,
            max_items=max_items,
        )
        all_items.extend(items)
        print(f"[DEBUG] {property_type}: {len(items)} elan toplandı")

    print(f"[DEBUG] Bu run-da toplanan cəmi elan (hər iki kateqoriya): {len(all_items)}")

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
        upload_bronze_file(out_path)
    else:
        print("[TAMAMLANDI] Yeni və ya dəyişmiş elan yoxdur, Bronze-a yazılmadı")

    save_processed_ids(updated_df, metadata_path)
    upload_metadata()
    print(f"[DEBUG] processed_ids yeniləndi, cəmi {len(updated_df)} ID izlənir")

    return out_path


if __name__ == "__main__":
    run_scraper(max_items=MAX_ITEMS)
