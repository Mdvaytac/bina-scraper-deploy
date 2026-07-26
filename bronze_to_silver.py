"""
ETL Addım 1: Bronze -> Silver

Bronze qatındakı BÜTÜN JSON fayllarını (hər scraping run-u ayrı fayldır)
oxuyur, birləşdirir, təmizləyir, tipləri düzəldir və Silver qatına
Parquet formatında yazır.

Vacib məntiq:
- area_unit "m²" (mənzil/ev) və "sot" (torpaq) qarışıq gəlir — bunları
  ayrı saxlayırıq ki, sonrakı analizdə (məs. "orta qiymət/m²") səhv
  hesablama olmasın.
- Fərqli scraping run-larından gələn eyni "id"-li elanlar üçün YALNız
  ən son "updated_at" olanı saxlayırıq (elan yenilənə bilər: qiymət,
  təmir statusu və s. dəyişə bilər).
"""

import pandas as pd
import json
import glob
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR.parent / "data_lake" / "bronze"
SILVER_DIR = BASE_DIR.parent / "data_lake" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)


def bronze_to_silver():
    files = sorted(glob.glob(str(BRONZE_DIR / "*.json")))
    if not files:
        print("[XƏBƏRDARLIQ] Bronze qatında heç bir fayl tapılmadı.")
        return None

    print(f"[INFO] {len(files)} bronze fayl tapıldı, oxunur...")

    all_rows = []
    for f in files:
        with open(f, encoding="utf-8") as fh:
            try:
                rows = json.load(fh)
                all_rows.extend(rows)
            except json.JSONDecodeError:
                print(f"[XƏTA] {f} JSON kimi oxunmadı, keçildi.")

    df = pd.DataFrame(all_rows)
    print(f"[INFO] Cəmi {len(df)} sətir (dublikatlar daxil) oxundu.")

    # --- Təmizləmə ---
    # ID və qiymət olmayan sətirləri at (bu, əsas identifikator və metrikadır)
    df = df.dropna(subset=["id", "price"])

    # Tip düzəlişləri
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["area_value"] = pd.to_numeric(df["area_value"], errors="coerce")
    df["rooms"] = pd.to_numeric(df["rooms"], errors="coerce")
    df["floor"] = pd.to_numeric(df["floor"], errors="coerce")
    df["floors_total"] = pd.to_numeric(df["floors_total"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # Dublikatlar: eyni id fərqli scraping run-larında görünə bilər.
    # Ən son updated_at-ə malik olanı saxlayırıq.
    df = df.sort_values("updated_at", ascending=False)
    df = df.drop_duplicates(subset=["id"], keep="first")

    # Sahə vahidinə görə iki alt-kateqoriya (mənzil/ev vs torpaq)
    df["property_type"] = df["area_unit"].map({
        "m²": "building",   # mənzil, ev, ofis və s.
        "sot": "land",      # torpaq sahəsi
    }).fillna("unknown")

    df["loaded_at"] = pd.Timestamp.now()

    out_path = SILVER_DIR / "listings.parquet"
    df.to_parquet(out_path, index=False)

    # CSV nüsxəsi — Excel/VS Code-da gözlə baxmaq üçün (Parquet binar olduğu üçün açılmır)
    csv_path = SILVER_DIR / "listings.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")  # utf-8-sig: Excel Azərbaycan hərflərini düzgün göstərsin

    print(f"[OK] Silver qatına {len(df)} unikal sətir yazıldı -> {out_path}")
    print(f"[OK] CSV nüsxəsi -> {csv_path}")
    print(f"[INFO] Bölgü: {df['property_type'].value_counts().to_dict()}")
    return out_path


if __name__ == "__main__":
    bronze_to_silver()