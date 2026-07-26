"""
ETL Addım 3: Gold Insights — analitik "kəşflər"

Bu skript sadə aqreqasiya deyil, HEÇ bir düşünülmüş dəyər əlavə edir:
- Sərfəli təkliflər (bazar qiymətindən aşağı)
- Şübhəli baha elanlar (bazar qiymətindən qat-qat yuxarı)
- Şübhəli satıcı fəaliyyəti (fərdi satıcı kimi görünən, amma çoxlu elanı olan)
- Dublikat/spam elanlar (eyni parametrlərlə bir neçə fərqli ID)

QEYD: Bunlar statistik EHTİMALLARDIR, "sübut" deyil — real fraud/spam
təyin etmək üçün insan yoxlaması lazımdır. Dashboard-da bunu vurğulamaq
vacibdir (məs. "Potensial" sözü ilə).
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SILVER_DIR = BASE_DIR.parent / "data_lake" / "silver"
GOLD_DIR = BASE_DIR.parent / "data_lake" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

MIN_GROUP_SIZE = 5          # rayon daxilində statistik hesablama üçün minimum elan sayı
DEAL_PERCENTILE = 0.15      # ən aşağı 15% -> potensial sərfəli təklif
OVERPRICED_PERCENTILE = 0.85  # ən yuxarı 15% -> potensial şübhəli baha
SUSPICIOUS_LISTING_COUNT = 4   # bundan çox elanı olan "fərdi" satıcı şübhəli sayılır
BURST_MIN_COUNT = 3            # eyni satıcı, eyni dəqiqədə bundan çox elan -> bot/skript əlaməti


def compute_insights():
    silver_path = SILVER_DIR / "listings.parquet"
    if not silver_path.exists():
        print("[XƏBƏRDARLIQ] Silver fayl tapılmadı. Əvvəlcə bronze_to_silver.py işə sal.")
        return

    df = pd.read_parquet(silver_path)
    buildings = df[df["property_type"] == "building"].copy()
    buildings = buildings.dropna(subset=["price", "area_value", "district"])
    buildings = buildings[buildings["area_value"] > 0]
    buildings["price_per_m2"] = buildings["price"] / buildings["area_value"]

    # ================================================================
    # 1) QİYMƏT ANOMALİYALARI (rayon daxilində nisbi mövqe)
    # ================================================================
    group_stats = buildings.groupby("district")["price_per_m2"].agg(
        district_avg_price_m2="mean",
        district_std_price_m2="std",
        district_count="count",
    ).reset_index()
    buildings = buildings.merge(group_stats, on="district", how="left")

    buildings["price_per_m2_percentile"] = buildings.groupby("district")["price_per_m2"].rank(pct=True)

    buildings["price_zscore"] = np.where(
        (buildings["district_count"] >= MIN_GROUP_SIZE) & (buildings["district_std_price_m2"] > 0),
        (buildings["price_per_m2"] - buildings["district_avg_price_m2"]) / buildings["district_std_price_m2"],
        np.nan,
    )

    buildings["deal_flag"] = (
        (buildings["district_count"] >= MIN_GROUP_SIZE)
        & (buildings["price_per_m2_percentile"] <= DEAL_PERCENTILE)
    )
    buildings["overpriced_flag"] = (
        (buildings["district_count"] >= MIN_GROUP_SIZE)
        & (buildings["price_per_m2_percentile"] >= OVERPRICED_PERCENTILE)
    )

    cols = ["id", "path", "district", "price", "area_value", "rooms", "floor",
            "price_per_m2", "district_avg_price_m2", "price_zscore",
            "price_per_m2_percentile", "company", "company_type", "updated_at"]

    deals = buildings[buildings["deal_flag"]].sort_values("price_per_m2_percentile")[cols]
    overpriced = buildings[buildings["overpriced_flag"]].sort_values(
        "price_per_m2_percentile", ascending=False
    )[cols]

    deals.to_parquet(GOLD_DIR / "deals_underpriced.parquet", index=False)
    overpriced.to_parquet(GOLD_DIR / "overpriced_listings.parquet", index=False)

    # ================================================================
    # 2) ŞÜBHƏLİ SATICI FƏALİYYƏTİ
    # ================================================================
    company_stats = df.groupby(["company", "company_type"], dropna=False).agg(
        listing_count=("id", "count"),
        avg_price=("price", "mean"),
        districts_covered=("district", "nunique"),
    ).reset_index()

    # "AGENCY" olmayan (yəni fərdi kimi görünən) satıcı
    company_stats["is_individual"] = (
        company_stats["company_type"].isna() | (company_stats["company_type"] != "AGENCY")
    )
    company_stats["suspicious_bulk_posting"] = (
        company_stats["is_individual"] & (company_stats["listing_count"] >= SUSPICIOUS_LISTING_COUNT)
    )
    company_stats["suspicious_multi_district"] = (
        company_stats["is_individual"] & (company_stats["districts_covered"] >= 3)
    )
    company_stats["suspicion_reason"] = np.select(
        [
            company_stats["suspicious_bulk_posting"] & company_stats["suspicious_multi_district"],
            company_stats["suspicious_bulk_posting"],
            company_stats["suspicious_multi_district"],
        ],
        [
            "Çoxlu elan + çoxlu rayon (fərdi üçün qeyri-adi)",
            "Fərdi satıcıdan gözlənilməz sayda elan",
            "Fərdi satıcı çoxlu rayonda elan yerləşdirib",
        ],
        default="",
    )

    company_stats = company_stats.sort_values("listing_count", ascending=False)
    company_stats.to_parquet(GOLD_DIR / "suspicious_sellers.parquet", index=False)

    # ================================================================
    # 3) DUBLİKAT / SPAM ELANLAR
    # ================================================================
    dup_check = df.groupby(["price", "area_value", "rooms", "district"]).agg(
        duplicate_count=("id", "count"),
        ids=("id", lambda x: ", ".join(x.astype(str))),
        companies_involved=("company", lambda x: ", ".join(sorted(set(x.dropna())))),
    ).reset_index()

    duplicates = dup_check[dup_check["duplicate_count"] > 1].sort_values(
        "duplicate_count", ascending=False
    )
    duplicates.to_parquet(GOLD_DIR / "duplicate_listings.parquet", index=False)

    # ================================================================
    # 4) SAATLIQ FƏALİYYƏT VƏ "BULK POSTING BURST" AŞKARLANMASI
    # ================================================================
    df["updated_at_dt"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["hour"] = df["updated_at_dt"].dt.hour
    df["minute_bucket"] = df["updated_at_dt"].dt.floor("min")

    # Ümumi saatlıq fəaliyyət — hansı saatlarda bazarda daha çox hərəkət var
    hourly_activity = df.dropna(subset=["hour"]).groupby("hour").agg(
        listing_count=("id", "count"),
        avg_price=("price", "mean"),
    ).reset_index().sort_values("hour")
    hourly_activity["hour"] = hourly_activity["hour"].astype(int)
    hourly_activity.to_parquet(GOLD_DIR / "posting_activity_by_hour.parquet", index=False)

    # Burst aşkarlanması: eyni satıcı, EYNİ DƏQİQƏDƏ 3+ elan yeniləyib/yerləşdirib
    # -> insan davranışına uyğun gəlmir, bot/skript əlaməti ola bilər
    burst_check = df.dropna(subset=["minute_bucket"]).groupby(
        ["company", "minute_bucket"]
    ).agg(
        burst_count=("id", "count"),
        ids=("id", lambda x: ", ".join(x.astype(str))),
        districts=("district", lambda x: ", ".join(sorted(set(x.dropna())))),
    ).reset_index()

    bursts = burst_check[burst_check["burst_count"] >= BURST_MIN_COUNT].sort_values(
        "burst_count", ascending=False
    )
    bursts.to_parquet(GOLD_DIR / "bulk_posting_bursts.parquet", index=False)

    # ================================================================
    # 5) MƏRKƏZİ "ENRICHED" CƏDVƏL — Power BI-da relationship problemi
    #    yaşamamaq üçün BÜTÜN bayraqlar tək cədvələ (hər elan = 1 sətir)
    # ================================================================
    # Dublikat elanların id-lərini "flat" siyahıya çeviririk ki, hər elana
    # tək-tək "is_duplicate" bayrağı verə bilək
    duplicate_ids = set()
    for ids_str in duplicates["ids"]:
        duplicate_ids.update(ids_str.split(", "))

    # Burst-a düşən elanların id-lərini də flat edirik
    burst_ids = set()
    for ids_str in bursts["ids"]:
        burst_ids.update(ids_str.split(", "))

    # Şübhəli satıcıların adlarını çıxarırıq
    suspicious_companies = set(
        company_stats[company_stats["suspicious_bulk_posting"]]["company"].dropna()
    )

    enriched = buildings.copy()
    enriched["hour"] = pd.to_datetime(enriched["updated_at"], errors="coerce").dt.hour
    enriched["deal_flag"] = enriched["deal_flag"].astype(bool)
    enriched["overpriced_flag"] = enriched["overpriced_flag"].astype(bool)
    enriched["is_duplicate"] = enriched["id"].astype(str).isin(duplicate_ids)
    enriched["is_burst_listing"] = enriched["id"].astype(str).isin(burst_ids)
    enriched["is_suspicious_seller"] = enriched["company"].isin(suspicious_companies)
    enriched["risk_flag_count"] = (
        enriched["overpriced_flag"].astype(int)
        + enriched["is_duplicate"].astype(int)
        + enriched["is_burst_listing"].astype(int)
        + enriched["is_suspicious_seller"].astype(int)
    )

    enriched_cols = [
        "id", "path", "district", "district_full", "price", "area_value", "rooms",
        "floor", "floors_total", "price_per_m2", "district_avg_price_m2",
        "price_zscore", "price_per_m2_percentile", "company", "company_type",
        "has_repair", "has_mortgage", "has_bill_of_sale", "is_business",
        "photos_count", "updated_at", "hour",
        "deal_flag", "overpriced_flag", "is_duplicate", "is_burst_listing",
        "is_suspicious_seller", "risk_flag_count",
    ]

    enriched[enriched_cols].to_parquet(GOLD_DIR / "listings_enriched.parquet", index=False)

    # ================================================================
    print(f"[OK] {len(deals)} sərfəli təklif -> deals_underpriced.parquet")
    print(f"[OK] {len(overpriced)} şübhəli-baha elan -> overpriced_listings.parquet")
    n_suspicious = int(company_stats["suspicious_bulk_posting"].sum())
    print(f"[OK] {n_suspicious} şübhəli satıcı -> suspicious_sellers.parquet")
    print(f"[OK] {len(duplicates)} dublikat qrupu -> duplicate_listings.parquet")
    print(f"[OK] Saatlıq fəaliyyət cədvəli -> posting_activity_by_hour.parquet")
    print(f"[OK] {len(bursts)} 'bulk posting burst' aşkarlandı -> bulk_posting_bursts.parquet")
    print(f"[OK] Mərkəzi zənginləşdirilmiş cədvəl ({len(enriched)} sətir) -> listings_enriched.parquet")


if __name__ == "__main__":
    compute_insights()