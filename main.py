"""
bina.az Scraper — TAM ARXİV rejimi (bir dəfəlik)
 
main.py-dan fərqlər:
  1. WATERMARK SÖNDÜRÜLÜB — "artıq tanış" elanlar scroll-u dayandırmır.
     Məqsəd siyahının SONUNA qədər getməkdir.
  2. LİMİTLƏR QALDIRILIB — mənbə başına 60 min elan, 6 saat.
  3. HƏR MƏNBƏ AYRICA YAZILIR — menzil bitəndə dərhal Bronze-a yüklənir.
     4 saatlıq run-ın 3-cü saatında çökmə olsa, birinci mənbə itmir.
  4. HƏR ŞEY YAZILIR — dəyişməyən elanlar da Bronze-a düşür.
     Tam snapshot arxivi lazımdır ki, gələcəkdə sıfırdan emal edə biləsən.
  5. indent=None — 77 min elanda JSON faylı 100 MB əvəzinə ~35 MB olur.
 
İŞLƏTMƏ:
    python main_full.py
 
QEYD: bu, lokalda işlədilmək üçündür. .env-də USE_AZURE=1 qalsın —
fayllar yenə Azure Storage-a yüklənəcək, ETL avtomatik işə düşəcək.
"""
 
from dotenv import load_dotenv
load_dotenv()
 
import json
import time
from datetime import datetime, timezone
from pathlib import Path
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
# KONFİQURASİYA — TAM ARXİV
# ======================================================
 
CATEGORIES = {
    "menzil": "menziller",
    "heyet_evi": "heyet-evleri",
}
 
# [None] = ölkə üzrə tək siyahı.
# Scroll siyahının sonuna çatmasa (aşağıdakı qeydə bax), buranı doldur:
#     CITY_SLUGS = ["baki", "sumqayit", "gence", "xirdalan", "quba", "qebele"]
CITY_SLUGS = [None]
 
MAX_ITEMS_PER_SOURCE = 60000      # praktiki olaraq limitsiz
MAX_SECONDS_PER_SOURCE = 21600    # 6 saat
MAX_SCROLLS_WITHOUT_NEW = 60      # şəbəkə ləngiməsinə dözümlü
SCROLL_PAUSE = 1.5
PROGRESS_EVERY = 500              # neçə yeni elandan bir vəziyyət yaz
 
 
def build_sources():
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
 
 
def parse_items(raw_json):
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
 
 
def collect_source_items(source):
    """Bir mənbəni SONA qədər scrape edir. Watermark yoxdur."""
    property_type = source["property_type"]
    label = f"{property_type}/{source['city_slug']}"
 
    collected = {}
    scrolls_without_new = 0
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
            nonlocal total_count_reported, last_progress
            if "operationName=SearchItems" not in response.url:
                return
            if response.status != 200:
                print(f"[XƏBƏRDARLIQ][{label}] status={response.status}")
                return
            try:
                raw = response.json()
            except Exception as e:
                print(f"[XƏTA][{label}] JSON parse: {e}")
                return
 
            items, _pi, total_count = parse_items(raw)
            if total_count is not None:
                total_count_reported = total_count
 
            for it in items:
                item_id = it.get("id")
                if item_id is None or item_id in collected:
                    continue
                it["property_type"] = property_type
                collected[item_id] = it
 
            if len(collected) - last_progress >= PROGRESS_EVERY:
                last_progress = len(collected)
                pct = (100 * len(collected) / total_count_reported
                       if total_count_reported else 0)
                print(f"[{label}] {len(collected)} / {total_count_reported} "
                      f"({pct:.1f}%)")
 
        page.on("response", handle_response)
 
        print(f"\n[{label}] {source['url']} açılır...")
        page.goto(source["url"], wait_until="domcontentloaded", timeout=60000)
        time.sleep(SCROLL_PAUSE)
 
        started = time.time()
        while len(collected) < MAX_ITEMS_PER_SOURCE and \
              scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:
 
            if time.time() - started > MAX_SECONDS_PER_SOURCE:
                print(f"[{label}] Vaxt limiti ({MAX_SECONDS_PER_SOURCE}s) doldu.")
                break
 
            before = len(collected)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(SCROLL_PAUSE)
 
            if len(collected) == before:
                scrolls_without_new += 1
                time.sleep(1.0)
            else:
                scrolls_without_new = 0
 
        elapsed = round(time.time() - started, 1)
        got = len(collected)
        total = total_count_reported or 0
        print(f"[{label}] BİTDİ: {got} / {total} elan, {elapsed}s")
 
        if total and got < total * 0.6:
            print(f"[{label}] !!! Saytda {total} elan var, yalnız {got} alındı.")
            print(f"[{label}] !!! Scroll limitə çatıb — CITY_SLUGS ilə "
                  f"şəhər-şəhər yığmaq lazımdır (fayl başındakı qeydə bax).")
 
        browser.close()
 
    return list(collected.values())
 
 
def run_full_archive():
    download_metadata()
    metadata_path = get_metadata_path()
    processed_df = load_processed_ids(metadata_path)
    print(f"[DEBUG] processed_ids-də {len(processed_df)} mövcud ID var")
 
    scraped_at = datetime.now(timezone.utc).isoformat()
    bronze_dir = get_bronze_dir()
    sources = build_sources()
 
    all_items = []
    seen = set()
 
    for source in sources:
        items = collect_source_items(source)
 
        fresh = []
        for it in items:
            key = str(it["id"])
            if key in seen:
                print(f"[XƏBƏRDARLIQ] {key} birdən çox mənbədə")
                continue
            seen.add(key)
            it["scraped_at"] = scraped_at
            fresh.append(it)
 
        # Mənbə bitən kimi yaz və yüklə — sonrakı mənbə çöksə də bu itmir
        if fresh:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = f"listings_full_{source['property_type']}_{ts}.json"
            out_path = bronze_dir / name
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(fresh, f, ensure_ascii=False)
            size_mb = round(out_path.stat().st_size / 1024 / 1024, 1)
            print(f"[YAZILDI] {len(fresh)} elan -> {name} ({size_mb} MB)")
 
            try:
                upload_bronze_file(out_path)
                print(f"[YÜKLƏNDİ] {name}")
            except Exception as e:
                print(f"[XƏTA] Yükləmə alınmadı: {e}")
                print(f"[QEYD] Fayl lokalda qalıb: {out_path}")
 
        all_items.extend(fresh)
 
    print(f"\n[DEBUG] Cəmi unikal elan: {len(all_items)}")
 
    # Metadata-nı yenilə (Bronze yazma qərarını bu dəfə buna tapşırmırıq —
    # tam arxiv rejimində hər şey onsuz da yazılıb)
    _new, _upd, _unch, updated_df = classify_and_update(all_items, processed_df)
    save_processed_ids(updated_df, metadata_path)
    try:
        upload_metadata()
    except Exception as e:
        print(f"[XƏTA] Metadata yüklənmədi: {e}")
 
    print(f"[DEBUG] processed_ids: {len(updated_df)} ID izlənir")
 
    # Tarix yayılması — zaman oxunun nə qədər geniş olduğunu göstərir
    dates = sorted(it["updated_at"][:10] for it in all_items
                   if it.get("updated_at"))
    if dates:
        print(f"\n[NƏTİCƏ] updated_at aralığı: {dates[0]} → {dates[-1]}")
        print(f"[NƏTİCƏ] Bu, trend qrafiklərinin zaman oxudur.")
 
    return all_items
 
 
if __name__ == "__main__":
    t0 = time.time()
    run_full_archive()
    print(f"\nÜmumi vaxt: {round((time.time() - t0) / 60, 1)} dəqiqə")
 
