"""
bina.az Scraper — v4

v3-dən fərqlər:
  1. FULL_ARCHIVE AÇARI. True olanda bir dəfəlik TAM YIĞIM rejimi işləyir:
     watermark söndürülür, şəhər-şəhər gəzilir, hər şey Bronze-a yazılır.
     False olanda əvvəlki saatlıq artımlı davranış qayıdır.
  2. ŞƏHƏR-ŞƏHƏR BÖLGÜ. 77 min elanı bir səhifədə scroll etmək brauzerin
     DOM-unu 5-8 GB-a çıxarır — konteynerin 4 GiB limitini aşır və çökür.
     Hər şəhər üçün brauzer yenidən açılır, yaddaş sıfırlanır. Üstəlik
     qısa siyahılarda scroll həqiqətən sona çatır (uzun siyahılarda sayt
     limit qoyur).
  3. HƏR MƏNBƏ AYRICA YAZILIR VƏ YÜKLƏNİR. 5 saatlıq run-ın 4-cü saatında
     çökmə olsa, əvvəlki şəhərlər itmir.
  4. ÜMUMİ VAXT BÜDCƏSİ. MAX_TOTAL_SECONDS konteynerin replica timeout-undan
     əvvəl işi nəzarətli dayandırır — DeadlineExceeded əvəzinə təmiz bitiş.
  5. indent=None (tam arxivdə). 40 min elanda fayl 100 MB əvəzinə ~18 MB.

TAM YIĞIMDAN SONRA: FULL_ARCHIVE = False et, push et, cron-u bərpa et.
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
# REJİM
# ======================================================

FULL_ARCHIVE = True     # <<< tam yığımdan sonra False et

# ======================================================
# KONFİQURASİYA
# ======================================================

CATEGORIES = {
    "menzil": "menziller",
    "heyet_evi": "heyet-evleri",
    # "torpaq": "torpaq",
    # "obyekt": "obyektler",
}

if FULL_ARCHIVE:
    # Bakı birincidir — elanların böyük hissəsi oradadır, vaxt büdcəsi
    # tükənsə belə ən dəyərli hissə yığılmış olsun.
    CITY_SLUGS = [
        "baki", "sumqayit", "xirdalan", "gence", "quba", "qebele",
        "xacmaz", "lenkeran", "seki", "samaxi", "qusar", "ismayilli",
        "qazax", "goygol", "qax", "samux", "goranboy", "sabirabad",
    ]
    MAX_ITEMS_PER_SOURCE = 60000
    MAX_SECONDS_PER_SOURCE = 1800     # şəhər başına 30 dəqiqə
    MAX_TOTAL_SECONDS = 21600         # ümumi 6 saat (replica timeout 7 saat)
    MAX_SCROLLS_WITHOUT_NEW = 60
    SCROLL_PAUSE = 1.5
    KNOWN_STREAK_LIMIT = None         # watermark söndürülüb
    WRITE_ALL = True                  # dəyişməyən elanlar da yazılır
    JSON_INDENT = None
    PROGRESS_EVERY = 500
else:
    CITY_SLUGS = [None]               # ölkə üzrə tək siyahı
    MAX_ITEMS_PER_SOURCE = 4000
    MAX_SECONDS_PER_SOURCE = 600
    MAX_TOTAL_SECONDS = 3000
    MAX_SCROLLS_WITHOUT_NEW = 25
    SCROLL_PAUSE = 2.0
    KNOWN_STREAK_LIMIT = 40           # ardıcıl tanış elan sayı
    WRITE_ALL = False                 # yalnız yeni + dəyişmiş
    JSON_INDENT = 2
    PROGRESS_EVERY = 200


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

def collect_source_items(source, known_ids, deadline=None):
    """
    Bir mənbəni (kateqoriya + şəhər) scrape edir.
    known_ids: watermark üçün mövcud ID-lər. FULL_ARCHIVE-də istifadə olunmur.
    deadline: ümumi vaxt büdcəsinin bitmə anı (time.time() dəyəri).
    """
    property_type = source["property_type"]
    label = f"{property_type}/{source['city_slug']}"

    collected = {}
    scrolls_without_new = 0
    consecutive_known = 0
    total_count_reported = None
    last_progress = 0

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
            nonlocal total_count_reported, consecutive_known, last_progress
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

            for it in items:
                item_id = it.get("id")
                if item_id is None or item_id in collected:
                    continue

                it["property_type"] = property_type
                collected[item_id] = it

                if KNOWN_STREAK_LIMIT is not None:
                    if str(item_id) in known_ids:
                        consecutive_known += 1
                    else:
                        consecutive_known = 0

            if len(collected) - last_progress >= PROGRESS_EVERY:
                last_progress = len(collected)
                pct = (100 * len(collected) / total_count_reported
                       if total_count_reported else 0)
                extra = (f" | ardıcıl-tanış: {consecutive_known}"
                         if KNOWN_STREAK_LIMIT is not None else "")
                print(f"[{label}] {len(collected)} / {total_count_reported} "
                      f"({pct:.1f}%){extra}")

        page.on("response", handle_response)

        print(f"\n[{label}] {source['url']} açılır...")
        try:
            page.goto(source["url"], wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"[XƏTA][{label}] Səhifə açılmadı: {str(e)[:120]}")
            browser.close()
            return []
        time.sleep(SCROLL_PAUSE)

        started = time.time()
        stop_reason = "scroll bitdi"

        while len(collected) < MAX_ITEMS_PER_SOURCE and \
              scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:

            if KNOWN_STREAK_LIMIT is not None and \
               consecutive_known >= KNOWN_STREAK_LIMIT:
                stop_reason = f"{KNOWN_STREAK_LIMIT} ardıcıl tanış elan (watermark)"
                break

            if time.time() - started > MAX_SECONDS_PER_SOURCE:
                stop_reason = f"mənbə vaxt limiti ({MAX_SECONDS_PER_SOURCE}s)"
                break

            if deadline and time.time() > deadline:
                stop_reason = "ümumi vaxt büdcəsi bitdi"
                break

            before = len(collected)
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception as e:
                stop_reason = f"scroll xətası: {str(e)[:60]}"
                break
            time.sleep(SCROLL_PAUSE)

            if len(collected) == before:
                scrolls_without_new += 1
                time.sleep(1.0)
            else:
                scrolls_without_new = 0

        elapsed = round(time.time() - started, 1)
        got = len(collected)
        total = total_count_reported or 0
        print(f"[{label}] BİTDİ: {got} / {total} elan, {elapsed}s — {stop_reason}")

        if FULL_ARCHIVE and total and got < total * 0.6:
            print(f"[{label}] !!! Saytda {total} elan var, yalnız {got} alındı.")
            print(f"[{label}] !!! Scroll limitə çatıb — bu şəhəri otaq sayına "
                  f"görə də bölmək lazım ola bilər.")

        browser.close()

    return list(collected.values())


# ======================================================
# ƏSAS AXIN
# ======================================================

def run_scraper():
    mode = "TAM ARXİV" if FULL_ARCHIVE else "SAATLIQ ARTIMLI"
    print(f"=== REJİM: {mode} ===")

    download_metadata()
    metadata_path = get_metadata_path()
    processed_df = load_processed_ids(metadata_path)
    known_ids = set(processed_df["id"].astype(str)) if len(processed_df) > 0 else set()
    print(f"[DEBUG] processed_ids-də {len(known_ids)} mövcud ID var")

    scraped_at = datetime.now(timezone.utc).isoformat()
    bronze_dir = get_bronze_dir()
    sources = build_sources()
    print(f"[DEBUG] {len(sources)} mənbə scrape ediləcək")

    t0 = time.time()
    deadline = t0 + MAX_TOTAL_SECONDS

    all_items = []
    seen = set()
    written_files = []

    for i, source in enumerate(sources, 1):
        if time.time() > deadline:
            print(f"\n[DAYANDI] Ümumi vaxt büdcəsi bitdi. "
                  f"{i - 1}/{len(sources)} mənbə tamamlandı.")
            break

        print(f"\n[{i}/{len(sources)}] === {source['property_type']} / "
              f"{source['city_slug']} ===")

        try:
            items = collect_source_items(source, known_ids, deadline)
        except Exception as e:
            print(f"[XƏTA] {source['city_slug']} uğursuz oldu: {str(e)[:150]}")
            continue

        fresh = []
        for it in items:
            key = str(it["id"])
            if key in seen:
                continue
            seen.add(key)
            it["scraped_at"] = scraped_at
            fresh.append(it)

        all_items.extend(fresh)

        # TAM ARXİV: hər mənbə bitən kimi yaz və yüklə
        if WRITE_ALL and fresh:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = (f"listings_full_{source['property_type']}_"
                    f"{source['city_slug']}_{ts}.json")
            out_path = bronze_dir / name
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(fresh, f, ensure_ascii=False, indent=JSON_INDENT)
            size_mb = round(out_path.stat().st_size / 1024 / 1024, 2)
            print(f"[YAZILDI] {len(fresh)} elan -> {name} ({size_mb} MB)")

            try:
                upload_bronze_file(out_path)
                written_files.append(name)
            except Exception as e:
                print(f"[XƏTA] Yükləmə alınmadı: {str(e)[:120]}")
                print(f"[QEYD] Fayl lokalda qalıb: {out_path}")

        print(f"[DEBUG] Cəmi unikal: {len(all_items)} | "
              f"keçən vaxt: {round((time.time() - t0) / 60, 1)} dəq")

    print(f"\n[DEBUG] Bu run-da unikal elan: {len(all_items)}")

    new_items, updated_items, unchanged_ids, updated_df = classify_and_update(
        all_items, processed_df
    )
    print(f"[DEBUG] Yeni: {len(new_items)} | Dəyişmiş: {len(updated_items)} | "
          f"Dəyişməyən: {len(unchanged_ids)}")

    # SAATLIQ REJİM: yalnız yeni + dəyişmiş, tək fayl
    if not WRITE_ALL:
        to_write = new_items + updated_items
        if to_write:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = bronze_dir / f"listings_{ts}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(to_write, f, ensure_ascii=False, indent=JSON_INDENT)
            print(f"[TAMAMLANDI] {len(to_write)} elan Bronze-a yazıldı")
            upload_bronze_file(out_path)
            written_files.append(out_path.name)
        else:
            print("[TAMAMLANDI] Yeni və ya dəyişmiş elan yoxdur")

    save_processed_ids(updated_df, metadata_path)
    try:
        upload_metadata()
    except Exception as e:
        print(f"[XƏTA] Metadata yüklənmədi: {str(e)[:120]}")
    print(f"[DEBUG] processed_ids: {len(updated_df)} ID izlənir")

    # Zaman oxunun genişliyi — trend qrafiklərinin əsası budur
    dates = sorted(it["updated_at"][:10] for it in all_items
                   if it.get("updated_at"))
    if dates:
        print(f"\n[NƏTİCƏ] updated_at aralığı: {dates[0]} → {dates[-1]}")

    print(f"[NƏTİCƏ] {len(written_files)} fayl Bronze-a yükləndi")
    print(f"[NƏTİCƏ] Ümumi vaxt: {round((time.time() - t0) / 60, 1)} dəqiqə")

    return written_files


if __name__ == "__main__":
    run_scraper()
