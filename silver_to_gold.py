"""
ETL Addım 2: Silver -> Gold

Silver qatındakı təmizlənmiş datanı Power BI dashboard-u üçün hazır
aqreqasiya cədvəllərinə çevirir.
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SILVER_DIR = BASE_DIR.parent / "data_lake" / "silver"
GOLD_DIR = BASE_DIR.parent / "data_lake" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def silver_to_gold():
    silver_path = SILVER_DIR / "listings.parquet"
    if not silver_path.exists():
        print("[XƏBƏRDARLIQ] Silver fayl tapılmadı. Əvvəlcə bronze_to_silver.py işə sal.")
        return

    df = pd.read_parquet(silver_path)

    # Yalnız mənzil/ev elanları üzərində qiymət/m² hesablayırıq
    # (torpaq elanlarını qarışdırmamaq üçün ayırırıq)
    buildings = df[df["property_type"] == "building"].copy()
    buildings["price_per_m2"] = buildings["price"] / buildings["area_value"]

    # --- Gold 1: Rayon üzrə xülasə ---
    district_summary = buildings.groupby("district").agg(
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        avg_price_per_m2=("price_per_m2", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max"),
        avg_area=("area_value", "mean"),
        listing_count=("id", "count"),
    ).reset_index().sort_values("listing_count", ascending=False)

    district_summary.to_parquet(GOLD_DIR / "district_summary.parquet", index=False)
    district_summary.to_csv(GOLD_DIR / "district_summary.csv", index=False)

    # --- Gold 2: Otaq sayı üzrə xülasə ---
    rooms_summary = buildings.groupby("rooms").agg(
        avg_price=("price", "mean"),
        avg_area=("area_value", "mean"),
        listing_count=("id", "count"),
    ).reset_index().sort_values("rooms")

    rooms_summary.to_parquet(GOLD_DIR / "rooms_summary.parquet", index=False)
    rooms_summary.to_csv(GOLD_DIR / "rooms_summary.csv", index=False)

    # --- Gold 3: Ümumi KPI-lar (bir sətirlik xülasə cədvəli) ---
    kpi = pd.DataFrame([{
        "total_listings": len(df),
        "total_buildings": len(buildings),
        "total_land": len(df[df["property_type"] == "land"]),
        "avg_price_azn": buildings["price"].mean(),
        "avg_price_per_m2": buildings["price_per_m2"].mean(),
        "last_updated": pd.Timestamp.now(),
    }])
    kpi.to_parquet(GOLD_DIR / "kpi_summary.parquet", index=False)
    kpi.to_csv(GOLD_DIR / "kpi_summary.csv", index=False)

    print(f"[OK] Gold qatı hazırdır -> {GOLD_DIR}")
    print(f"[INFO] {len(district_summary)} rayon, {len(rooms_summary)} otaq-kateqoriyası")


if __name__ == "__main__":
    silver_to_gold()