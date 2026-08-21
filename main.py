"""
bina.az Scraper — v5

KRİTİK DƏYİŞİKLİK: scroll ləğv edildi, CURSOR PAGİNASİYASI gəldi.

Problem: v4-də hər mənbədən yalnız ~16 elan gəlirdi (bir GraphQL səhifəsi).
Scroll etməyə cəhd olunurdu, amma sayt yeni sorğu göndərmirdi. bina.az
sonsuz-scroll davranışını dəyişib və `window.scrollTo` artıq yükləməni
tetikləmir.

Həll: brauzerin ilk `SearchItems` sorğusunu tuturuq, ondan URL şablonunu
(persistedQuery hash daxil) çıxarırıq, sonra `variables.after` sahəsinə
cursor qoyub səhifə-səhifə özümüz çağırırıq. Sorğu brauzerin öz konteksti
daxilində (`fetch`) gedir — cookie və header-lər avtomatik düzgün olur.

Üstünlükləri:
  * Scroll davranışından asılı deyil
  * Səhifə render olunmur -> yaddaş sabit qalır (77 min elanda da)
  * ~10 dəfə sürətli
  * `first` 16-dan 40-a qaldırılıb -> daha az sorğu

Cursor işləməsə (API dəyişsə), avtomatik scroll üsuluna qayıdır.

LOKALDA TEST: TEST_MODE = True et, `python main.py` işlət.
Yalnız bir mənbə, 3 səhifə çəkir, hər addımı çap edir. 30 saniyə.
"""

from dotenv import load_dotenv
load_dotenv()

import json
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode
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

TEST_MODE = False       # True: bir mənbə, 3 səhifə, Azure-a yazmır
FULL_ARCHIVE = True     # tam yığımdan sonra False et

# ======================================================
# KONFİQURASİYA
# ======================================================

CATEGORIES = {
    "menzil": "menziller",
    "heyet_evi": "heyet-evleri",
}

PAGE_SIZE = 40              # bir sorğuda neçə elan (sayt defoltu 16)
REQUEST_PAUSE = 0.35        # sorğular arası fasilə — saytı yormamaq üçün

if FULL_ARCHIVE:
    CITY_SLUGS = [None]                # cursor ilə ölkə üzrə tam gəzmək olur
    MAX_ITEMS_PER_SOURCE = 80000
    MAX_SECONDS_PER_SOURCE = 3600
    MAX_TOTAL_SECONDS = 21600
    KNOWN_STREAK_LIMIT = None          # watermark söndürülüb
    WRITE_ALL = True
    JSON_INDENT = None
    PROGRESS_EVERY = 1000
else:
    CITY_SLUGS = [None]
    MAX_ITEMS_PER_SOURCE = 6000
    MAX_SECONDS_PER_SOURCE = 900
    MAX_TOTAL_SECONDS = 3000
    KNOWN_STREAK_LIMIT = 200           # ardıcıl tanış elan sayı
    WRITE_ALL = False
    JSON_INDENT = 2
    PROGRESS_EVERY = 400

# Scroll fallback parametrləri (yalnız cursor işləməsə)
SCROLL_PAUSE = 2.0
MAX_SCROLLS_WITHOUT_NEW = 20


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


# ======================================================
# PARSING
# ======================================================

def parse_items(raw_json):
    items = []
    data = raw_json.get("data") or {}
    connection = data.get("itemsConnection") or {}

    for edge in connection.get("edges", []) or []:
        node = edge.get("node") or {}
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

    page_info = connection.get("pageInfo") or {}
    return items, page_info, connection.get("totalCount")


# ======================================================
# CURSOR PAGİNASİYASI
# ======================================================

FETCH_JS = """
async (u) => {
  const r = await fetch(u, { credentials: 'include',
                             headers: { 'accept': 'application/json' } });
  if (!r.ok) return { __err: r.status };
  return await r.json();
}
"""


def build_query_url(template_url, variables, extensions_raw):
    """Tutulmuş sorğu şablonundan yeni cursor ilə URL qurur."""
    p = urlparse(template_url)
    params = {
        "operationName": "SearchItems",
        "variables": json.dumps(variables, separators=(",", ":")),
    }
    if extensions_raw:
        params["extensions"] = extensions_raw
    return f"{p.scheme}://{p.netloc}{p.path}?{urlencode(params)}"


def collect_via_cursor(page, template_url, label, deadline, known_ids,
                       max_items, max_seconds):
    """
    Tutulmuş SearchItems sorğusunu cursor ilə səhifə-səhifə təkrarlayır.
    Uğursuz olsa None qaytarır -> çağıran tərəf scroll-a keçir.
    """
    p = urlparse(template_url)
    q = parse_qs(p.query)

    try:
        variables = json.loads(q["variables"][0])
    except Exception as e:
        print(f"[{label}] Cursor: variables oxunmadı ({e}) -> scroll-a keçilir")
        return None

    extensions_raw = q.get("extensions", [None])[0]
    variables["first"] = PAGE_SIZE

    collected = {}
    cursor = None
    total = None
    consecutive_known = 0
    page_no = 0
    last_progress = 0
    started = time.time()

    while True:
        if len(collected) >= max_items:
            print(f"[{label}] Element limitinə çatdı ({max_items})")
            break
        if time.time() - started > max_seconds:
            print(f"[{label}] Mənbə vaxt limiti ({max_seconds}s)")
            break
        if deadline and time.time() > deadline:
            print(f"[{label}] Ümumi vaxt büdcəsi bitdi")
            break
        if KNOWN_STREAK_LIMIT is not None and consecutive_known >= KNOWN_STREAK_LIMIT:
            print(f"[{label}] {KNOWN_STREAK_LIMIT} ardıcıl tanış elan (watermark)")
            break

        if cursor:
            variables["after"] = cursor
        url = build_query_url(template_url, variables, extensions_raw)

        try:
            raw = page.evaluate(FETCH_JS, url)
        except Exception as e:
            print(f"[{label}] Cursor fetch xətası: {str(e)[:100]}")
            return None if page_no == 0 else list(collected.values())

        if not isinstance(raw, dict) or raw.get("__err"):
            code = raw.get("__err") if isinstance(raw, dict) else "?"
            print(f"[{label}] Cursor HTTP {code}")
            return None if page_no == 0 else list(collected.values())

        if raw.get("errors"):
            msg = str(raw["errors"])[:150]
            print(f"[{label}] GraphQL xətası: {msg}")
            return None if page_no == 0 else list(collected.values())

        items, page_info, total_count = parse_items(raw)
        if total_count is not None:
            total = total_count

        page_no += 1

        if page_no == 1 and not items:
            print(f"[{label}] Cursor: ilk səhifə boş -> scroll-a keçilir")
            return None

        new_count = 0
        for it in items:
            iid = it.get("id")
            if iid is None or iid in collected:
                continue
            it["property_type"] = None      # çağıran tərəf dolduracaq
            collected[iid] = it
            new_count += 1
            if KNOWN_STREAK_LIMIT is not None:
                if str(iid) in known_ids:
                    consecutive_known += 1
                else:
                    consecutive_known = 0

        if len(collected) - last_progress >= PROGRESS_EVERY:
            last_progress = len(collected)
            pct = (100 * len(collected) / total) if total else 0
            print(f"[{label}] {len(collected)} / {total} ({pct:.1f}%) "
                  f"| səhifə {page_no}")

        has_next = page_info.get("hasNextPage")
        cursor = page_info.get("endCursor")

        if TEST_MODE and page_no >= 3:
            print(f"[{label}] TEST_MODE: 3 səhifədən sonra dayanıldı")
            break

        if has_next is False or not cursor:
            print(f"[{label}] Siyahının sonu (səhifə {page_no})")
            break

        if new_count == 0:
            print(f"[{label}] Yeni elan gəlmədi, dayanılır (səhifə {page_no})")
            break

        time.sleep(REQUEST_PAUSE)

    print(f"[{label}] CURSOR NƏTİCƏSİ: {len(collected)} / {total} elan, "
          f"{page_no} sorğu, {round(time.time() - started, 1)}s")
    return list(collected.values())


# ======================================================
# SCROLL FALLBACK
# ======================================================

def collect_via_scroll(page, label, collected_from_listener, max_items, max_seconds):
    """Cursor işləməsə köhnə üsul. collected_from_listener response dinləyicisi
    tərəfindən doldurulur."""
    print(f"[{label}] SCROLL üsuluna keçilir...")
    started = time.time()
    scrolls_without_new = 0

    while len(collected_from_listener) < max_items and \
          scrolls_without_new < MAX_SCROLLS_WITHOUT_NEW:
        if time.time() - started > max_seconds:
            break
        before = len(collected_from_listener)
        try:
            page.evaluate(
                "window.scrollBy(0, document.body.scrollHeight * 0.8)")
            page.keyboard.press("End")
        except Exception:
            break
        time.sleep(SCROLL_PAUSE)
        if len(collected_from_listener) == before:
            scrolls_without_new += 1
        else:
            scrolls_without_new = 0

    print(f"[{label}] SCROLL NƏTİCƏSİ: {len(collected_from_listener)} elan")
    return list(collected_from_listener.values())


# ======================================================
# BİR MƏNBƏ
# ======================================================

def collect_source_items(source, known_ids, deadline=None):
    property_type = source["property_type"]
    label = f"{property_type}/{source['city_slug']}"

    captured = {"url": None}
    scroll_bucket = {}

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

        def on_request(req):
            if captured["url"] is None and "operationName=SearchItems" in req.url:
                captured["url"] = req.url

        def on_response(resp):
            # yalnız scroll fallback üçün lazımdır
            if "operationName=SearchItems" not in resp.url or resp.status != 200:
                return
            try:
                items, _pi, _tc = parse_items(resp.json())
            except Exception:
                return
            for it in items:
                if it.get("id") and it["id"] not in scroll_bucket:
                    scroll_bucket[it["id"]] = it

        page.on("request", on_request)
        page.on("response", on_response)

        print(f"\n[{label}] {source['url']} açılır...")
        try:
            page.goto(source["url"], wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"[XƏTA][{label}] Səhifə açılmadı: {str(e)[:120]}")
            browser.close()
            return []

        # ilk GraphQL sorğusunun gəlməsini gözlə
        for _ in range(30):
            if captured["url"]:
                break
            time.sleep(0.5)

        if not captured["url"]:
            print(f"[{label}] !!! SearchItems sorğusu tutulmadı. "
                  f"URL səhv ola bilər və ya səhifə boşdur.")
            browser.close()
            return []

        print(f"[{label}] Sorğu şablonu tutuldu.")

        result = collect_via_cursor(
            page, captured["url"], label, deadline, known_ids,
            MAX_ITEMS_PER_SOURCE, MAX_SECONDS_PER_SOURCE)

        if result is None:
            result = collect_via_scroll(
                page, label, scroll_bucket,
                MAX_ITEMS_PER_SOURCE, MAX_SECONDS_PER_SOURCE)

        browser.close()

    for it in result:
        it["property_type"] = property_type
    return result


# ======================================================
# ƏSAS AXIN
# ======================================================

def run_scraper():
    mode = "TAM ARXİV" if FULL_ARCHIVE else "SAATLIQ ARTIMLI"
    print(f"=== REJİM: {mode}{' | TEST' if TEST_MODE else ''} ===")
    print(f"=== ÜSUL: cursor paginasiyası (səhifə ölçüsü {PAGE_SIZE}) ===")

    if TEST_MODE:
        sources = build_sources()[:1]
        known_ids, processed_df = set(), None
    else:
        download_metadata()
        metadata_path = get_metadata_path()
        processed_df = load_processed_ids(metadata_path)
        known_ids = (set(processed_df["id"].astype(str))
                     if len(processed_df) > 0 else set())
        print(f"[DEBUG] processed_ids-də {len(known_ids)} ID var")
        sources = build_sources()

    print(f"[DEBUG] {len(sources)} mənbə\n")

    t0 = time.time()
    deadline = t0 + MAX_TOTAL_SECONDS
    scraped_at = datetime.now(timezone.utc).isoformat()
    bronze_dir = get_bronze_dir()

    all_items, seen, written = [], set(), []

    for i, source in enumerate(sources, 1):
        if time.time() > deadline:
            print(f"\n[DAYANDI] Vaxt büdcəsi bitdi ({i-1}/{len(sources)})")
            break

        print(f"[{i}/{len(sources)}] === {source['property_type']} / "
              f"{source['city_slug']} ===")

        try:
            items = collect_source_items(source, known_ids, deadline)
        except Exception as e:
            print(f"[XƏTA] {source['city_slug']}: {str(e)[:150]}")
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

        if TEST_MODE:
            print(f"\n[TEST] {len(fresh)} elan yığıldı. Nümunə:")
            for it in fresh[:3]:
                print(f"   {it['id']} | {it['district']} | {it['rooms']} otaq "
                      f"| {it['area_value']} {it['area_unit']} | {it['price']}")
            continue

        if WRITE_ALL and fresh:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = (f"listings_full_{source['property_type']}_"
                    f"{source['city_slug']}_{ts}.json")
            out_path = bronze_dir / name
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(fresh, f, ensure_ascii=False, indent=JSON_INDENT)
            mb = round(out_path.stat().st_size / 1024 / 1024, 2)
            print(f"[YAZILDI] {len(fresh)} elan -> {name} ({mb} MB)")
            try:
                upload_bronze_file(out_path)
                written.append(name)
            except Exception as e:
                print(f"[XƏTA] Yükləmə: {str(e)[:120]}")

        print(f"[DEBUG] Cəmi: {len(all_items)} | "
              f"{round((time.time()-t0)/60, 1)} dəq\n")

    if TEST_MODE:
        print(f"\n=== TEST BİTDİ: {len(all_items)} elan ===")
        print("Rəqəm 100-dən çoxdursa, cursor işləyir. "
              "16 və ya 40 qalıbsa, mənə log-u göndər.")
        return []

    print(f"\n[DEBUG] Unikal elan: {len(all_items)}")

    new_items, updated_items, unchanged, updated_df = classify_and_update(
        all_items, processed_df)
    print(f"[DEBUG] Yeni: {len(new_items)} | Dəyişmiş: {len(updated_items)} | "
          f"Dəyişməyən: {len(unchanged)}")

    if not WRITE_ALL:
        to_write = new_items + updated_items
        if to_write:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = bronze_dir / f"listings_{ts}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(to_write, f, ensure_ascii=False, indent=JSON_INDENT)
            print(f"[TAMAMLANDI] {len(to_write)} elan yazıldı")
            upload_bronze_file(out_path)
            written.append(out_path.name)
        else:
            print("[TAMAMLANDI] Yeni elan yoxdur")

    save_processed_ids(updated_df, metadata_path)
    try:
        upload_metadata()
    except Exception as e:
        print(f"[XƏTA] Metadata: {str(e)[:120]}")

    dates = sorted(it["updated_at"][:10] for it in all_items
                   if it.get("updated_at"))
    if dates:
        print(f"\n[NƏTİCƏ] updated_at aralığı: {dates[0]} → {dates[-1]}")
    print(f"[NƏTİCƏ] {len(written)} fayl yükləndi")
    print(f"[NƏTİCƏ] Ümumi vaxt: {round((time.time()-t0)/60, 1)} dəqiqə")

    return written


if __name__ == "__main__":
    run_scraper()
