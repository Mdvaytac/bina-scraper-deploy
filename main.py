"""
bina.az Scraper — v3

v2-dən fərqlər:
  1. DÜZGÜN KATEQORİYA URL-LƏRİ. Əvvəl "menzil" üçün /alqi-satqi işlədilirdi —
     bu, bütün kateqoriyaların ümumi səhifəsidir (mənzil + ev + torpaq + obyekt).
     Nəticədə datanın ~31%-i səhv etiketlənirdi və həyət evlərinin 24%-i
     iki dəfə saxlanılırdı. İndi hər kateqoriyanın öz URL-i var.
  2. COMPOSITE ID LƏĞV EDİLDİ. bina.az id-si onsuz da qlobal unikaldır.
     Composite ID dublikatın qarşısını almırdı, əksinə maskalayırdı.
     İndi: id = orijinal id, property_type = ayrıca sütun.
  3. YENİ SAHƏLƏR: company_id, city_id, district_id, thumbnail, scraped_at.
     photos_count SİLİNDİ — GraphQL node-unda belə sahə yoxdur, həmişə NULL idi.
  4. SCROLL YAXŞILAŞDIRILDI: mouse.wheel əvəzinə səhifənin sonuna scroll,
     üstəgəl mənbə başına vaxt limiti (konteyner timeout-una qarşı qoruma).
  5. ŞƏHƏR ƏHATƏSİ KONFİQURASİYA İLƏ: CITY_SLUGS bir sətirdə dəyişir.
"""

from dotenv import load_dotenv
load_dotenv()

import json
import time
from datetime import datetime, timezone
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
# KONFİQURASİYA
# ======================================================

# property_type -> bina.az kateqoriya slug-ı
CATEGORIES = {
    "menzil": "menziller",
    "heyet_evi": "heyet-evleri",
    # Sonra əlavə etmək istəsən (URL-i saytdan yoxla):
    # "torpaq": "torpaq",
    # "obyekt": "obyektler",
}

# [None] = bütün ölkə üzrə tək siyahı.
# Regionlardan real data istəyəndə şəhər slug-larını bura yaz, məsələn:
#     CITY_SLUGS = ["baki", "sumqayit", "gence", "xirdalan"]
# Hər şəhərin siyahısı qısa olduğu üçün scroll axıra çatır.
CITY_SLUGS = [None]

MAX_ITEMS_PER_SOURCE = 10000
SCROLL_PAUSE = 2.0
MAX_SCROLLS_WITHOUT_NEW = 25
KNOWN_STREAK_LIMIT = 40          # ardıcıl bu qədər tanış elan görünsə, dayan
MAX_SECONDS_PER_SOURCE = 900     # bir mənbəyə maksimum 15 dəqiqə


def build_sources():
    """CATEGORIES × CITY_SLUGS kombinasiyalarından mənbə siyahısı qurur."""
    sources = []
    for property_type, slug in CATEGORIES.items():
        for city in CITY_SLUGS:
            url = (f"https://bina.az/{city}/alqi-satqi/{slug}" if city
                   else f"https://bina.az/alqi-satqi/{slug}")
            sources.append({
                "property_type": property_type,
                "city_slug": city or "all",
                "url": url,
            })
    return sources


# ======================================================
# PARSING
# ======================================================

def parse_items(raw_json):
    """GraphQL SearchItems cavabından lazımi sahələri çıxarır."""
    items = []
    connection = raw_json.get("data", {}).get("itemsConnection", {})

    for edge in connection.get("edges", []):
        node = edge.get("node", {})
        area = node.get("area") or {}
        price = node.get("price") or {}
        city = node.get("city") or {}
        location = node.get("location") or {}
        company = node.get("company") or {}
        preview = node.get("preview") or {}

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
            "city_id": city.get("id"),
            "city": city.get("name"),
            "district_id": location.get("id"),
            "district": location.get("name"),
            "district_full": location.get("fullName"),
            "is_leased": node.get("isLeased"),
            "has_repair": node.get("hasRepair"),
            "has_mortgage": node.get("hasMortgage"),
            "has_bill_of_sale": node.get("hasBillOfSale"),
            "has_internal_loan": node.get("hasInternalLoan"),
            "is_vipped": node.get("isVipped"),
            "is_featured": node.get("isFeatured"),
            "is_business": node.get("isBusiness"),
            "company_id": company.get("id"),
            "company": company.get("name"),
            "company_type": company.get("targetType"),
            "thumbnail": preview.get("thumbnail"),
            "updated_at": node.get("updatedAt"),
            "path": node.get("path"),
        })

    return items, connection.get("pageInfo", {}), connection.get("totalCount")


# ======================================================
# SCRAPING
# ======================================================

def collect_source_items(source, known_ids, max_items=MAX_ITEMS_PER_SOURCE):
    """
    Bir mənbə (kateqoriya + şəhər kombinasiyası) üçün scrape edir.
    known_ids: processed_ids-də mövcud olan ID-lər (plain, string).
    """
    property_type = source["property_type"]
    label = f"{property_type}/{source['city_slug']}"

    collected = {}
    scrolls_without_new = 0
    consecutive_known = 0
    total_count_reported = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
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
                print(f"[XƏBƏRDARLIQ][{label}] SearchItems status={response.status}")
                return
            try:
                raw = response.json()
            except Exception as e:
                print(f"[XƏTA][{label}] JSON parse alınmadı: {e}")
                return

            items, _page_info, total_count = parse_items(raw)
            if total_count is not None:
                total_count_reported = total_count

            new_count = 0
            for it in items:
                item_id = it.get("id")
                if item_id is None or item_id in collected:
                    continue

                it["property_type"] = property_type
                collected[item_id] = it
                new_count += 1

                if str(item_id) in known_ids:
                    consecutive_known += 1
                else:
                    consecutive_known = 0

            if new_count > 0:
                print(f"[OK][{label}] +{new_count} yeni "
                      f"(cəmi: {len(collected)} / {total_count_reported}) "
                      f"| ardıcıl-tanış: {consecutive_known}")

        page.on("response", handle_response)

        print(f"[{label}] {source['url']} açılır...")
        page.goto(source["url"], wait_until="domcontentloaded", timeout=60000)
        time.sleep(SCROLL_PAUSE)
        print(f"[DEBUG][{label}] İlk yüklənmədən sonra: {len(collected)}")

        started = time.time()
        while len(collected) < max_items and scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:

            if consecutive_known >= KNOWN_STREAK_LIMIT:
                print(f"[DEBUG][{label}] {KNOWN_STREAK_LIMIT} ardıcıl tanış elan — "
                      f"yeni ərazi bitdi, dayanılır.")
                break

            if time.time() - started > MAX_SECONDS_PER_SOURCE:
                print(f"[DEBUG][{label}] Vaxt limiti ({MAX_SECONDS_PER_SOURCE}s) doldu.")
                break

            before = len(collected)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(SCROLL_PAUSE)

            if len(collected) == before:
                scrolls_without_new += 1
                time.sleep(1.0)   # səhifə hələ yüklənə bilər, bir az da gözlə
            else:
                scrolls_without_new = 0

        elapsed = round(time.time() - started, 1)
        print(f"[DEBUG][{label}] Bitdi. Yekun: {len(collected)} elan, {elapsed}s")
        browser.close()

    return list(collected.values())


# ======================================================
# ƏSAS AXIN
# ======================================================

def run_scraper():
    download_metadata()
    metadata_path = get_metadata_path()
    processed_df = load_processed_ids(metadata_path)
    known_ids = set(processed_df["id"].astype(str)) if len(processed_df) > 0 else set()
    print(f"[DEBUG] processed_ids-də {len(known_ids)} mövcud ID var")

    scraped_at = datetime.now(timezone.utc).isoformat()
    sources = build_sources()
    print(f"[DEBUG] {len(sources)} mənbə scrape ediləcək")

    merged = {}
    for source in sources:
        print(f"[DEBUG] === {source['property_type']} / {source['city_slug']} ===")
        items = collect_source_items(source, known_ids)

        for it in items:
            key = str(it["id"])
            if key in merged:
                # Kateqoriya URL-ləri düzgündürsə bu baş verməməlidir.
                print(f"[XƏBƏRDARLIQ] {key} birdən çox mənbədə: "
                      f"{merged[key]['property_type']} / {it['property_type']}")
                continue
            it["scraped_at"] = scraped_at
            merged[key] = it

        print(f"[DEBUG] {source['property_type']}/{source['city_slug']}: "
              f"{len(items)} elan (cəmi unikal: {len(merged)})")

    all_items = list(merged.values())
    print(f"[DEBUG] Bu run-da unikal elan: {len(all_items)}")

    new_items, updated_items, unchanged_ids, updated_df = classify_and_update(
        all_items, processed_df
    )
    print(f"[DEBUG] Yeni: {len(new_items)} | Dəyişmiş: {len(updated_items)} | "
          f"Dəyişməyən: {len(unchanged_ids)}")

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
        print("[TAMAMLANDI] Yeni və ya dəyişmiş elan yoxdur")

    save_processed_ids(updated_df, metadata_path)
    upload_metadata()
    print(f"[DEBUG] processed_ids yeniləndi, cəmi {len(updated_df)} ID izlənir")

    return out_path


if __name__ == "__main__":
    run_scraper()
