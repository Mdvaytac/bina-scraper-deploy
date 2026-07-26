"""
metadata_manager.py

Bronze qatına yazılmadan əvvəl duplicate yoxlanışı üçün
processed_ids metadata faylını idarə edir.

Sxem:
    id                -> elanın unikal ID-si
    first_seen        -> ilk dəfə görüldüyü tarix (YYYY-MM-DD)
    last_seen         -> scraper-in onu son dəfə gördüyü tarix
    last_updated_at   -> qiymət/məlumat son dəyişdiyi tarix
    price             -> son bilinən qiymət (dəyişiklik aşkarlamaq üçün)

Bu fayl həm lokal, həm də Azure Data Lake üzərində işləyə bilər —
sadəcə ona ötürülən `metadata_path` Path obyektinin haradan gəldiyi
fərqlidir (bax: azure_lake_client.py).
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from datetime import datetime

METADATA_COLUMNS = ["id", "first_seen", "last_seen", "last_updated_at", "price"]


def load_processed_ids(metadata_path: Path) -> pd.DataFrame:
    """processed_ids.parquet faylını oxuyur. Yoxdursa, boş DataFrame qaytarır."""
    if metadata_path.exists():
        df = pd.read_parquet(metadata_path)
        for col in METADATA_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[METADATA_COLUMNS]
    return pd.DataFrame(columns=METADATA_COLUMNS)


def save_processed_ids(df: pd.DataFrame, metadata_path: Path) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(metadata_path, index=False)


def classify_and_update(items: list[dict], processed_df: pd.DataFrame):
    """
    Yeni scrape edilmiş item-ləri mövcud processed_ids ilə müqayisə edir.

    Qaytarır:
        new_items      -> Bronze-a yazılacaq TAMAMİLƏ YENİ elanlar
        updated_items  -> qiyməti/dəyəri dəyişmiş, Bronze-a yazılacaq elanlar
        unchanged_ids  -> heç nə dəyişməyib, Bronze-a yazılmır
        updated_df     -> yenilənmiş, save edilməyə hazır processed_ids DataFrame-i
    """
    today = datetime.now().strftime("%Y-%m-%d")

    known = processed_df.set_index("id").to_dict(orient="index") if not processed_df.empty else {}

    new_items: list[dict] = []
    updated_items: list[dict] = []
    unchanged_ids: list = []

    records = dict(known)

    for item in items:
        item_id = item["id"]
        if item_id is None:
            continue
        price = item.get("price")

        if item_id not in known:
            new_items.append(item)
            records[item_id] = {
                "first_seen": today,
                "last_seen": today,
                "last_updated_at": today,
                "price": price,
            }
        else:
            existing = known[item_id]
            if existing.get("price") != price:
                updated_items.append(item)
                records[item_id] = {
                    "first_seen": existing.get("first_seen", today),
                    "last_seen": today,
                    "last_updated_at": today,
                    "price": price,
                }
            else:
                unchanged_ids.append(item_id)
                records[item_id] = {
                    "first_seen": existing.get("first_seen", today),
                    "last_seen": today,
                    "last_updated_at": existing.get("last_updated_at", today),
                    "price": price,
                }

    updated_df = pd.DataFrame(
        [{"id": k, **v} for k, v in records.items()],
        columns=METADATA_COLUMNS,
    )

    return new_items, updated_items, unchanged_ids, updated_df