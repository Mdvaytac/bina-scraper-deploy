"""
azure_lake_client.py

Bronze qovluğuna (batch/bronze) və metadata qovluğuna
(metadata/processed_ids.parquet) yazmaq üçün vahid interfeys.

İki rejim dəstəklənir:
  1) LOKAL  (default) — .env-də USE_AZURE=0 və ya təyin edilməyib.
     Fayllar sənin kompüterində data_lake/... altında saxlanılır.
     Bu, VS Code-da development və test üçün ideal rejimdir.

  2) AZURE  — .env-də USE_AZURE=1 və AZURE_STORAGE_CONNECTION_STRING təyin edilib.
     Fayllar birbaşa Azure Data Lake Storage Gen2-yə yazılır
     (azure-storage-file-datalake paketi tələb olunur:
      pip install azure-storage-file-datalake).

Bu modul sayəsində scrape.py Azure-un öz SDK-sını bilmək məcburiyyətində
deyil — sadəcə get_bronze_dir() / get_metadata_path() çağırır və
Path kimi davranan obyekt alır (lokal rejimdə əsl Path, Azure rejimində
isə eyni interfeysə malik kiçik bir wrapper).
"""

from __future__ import annotations

import os
from pathlib import Path

USE_AZURE = os.getenv("USE_AZURE", "0") == "1"
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "batch")

BASE_DIR = Path(__file__).resolve().parent
LOCAL_LAKE_ROOT = BASE_DIR.parent / "data_lake"


def _local_bronze_dir() -> Path:
    d = LOCAL_LAKE_ROOT / "bronze"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _local_metadata_path() -> Path:
    d = LOCAL_LAKE_ROOT / "metadata"
    d.mkdir(parents=True, exist_ok=True)
    return d / "processed_ids.parquet"


def get_bronze_dir() -> Path:
    """
    Bronze qovluğunun yolunu qaytarır.

    Qeyd: hazırkı versiyada Azure rejimi üçün faktiki upload məntiqi
    `upload_bronze_file()` funksiyasında ayrıca idarə olunur (aşağıya bax),
    çünki ADLS-ə yazmaq Path deyil, DataLakeFileClient tələb edir.
    Bu funksiya yalnız lokal keşləmə/əvvəlcədən yazma üçün istifadə olunur.
    """
    return _local_bronze_dir()


def get_metadata_path() -> Path:
    """
    processed_ids.parquet-in lokal keş yolunu qaytarır.

    Azure rejimində scraper işə başlamazdan əvvəl bu fayl Azure-dan
    endirilir (download_metadata), iş bitəndən sonra isə geri yüklənir
    (upload_metadata) — beləliklə pandas həmişə lokal fayl üzərində işləyir.
    """
    return _local_metadata_path()


def _get_datalake_service_client():
    from azure.storage.filedatalake import DataLakeServiceClient

    if not AZURE_CONN_STR:
        raise RuntimeError(
            "USE_AZURE=1 təyin edilib, amma AZURE_STORAGE_CONNECTION_STRING yoxdur. "
            ".env faylını yoxla."
        )
    return DataLakeServiceClient.from_connection_string(AZURE_CONN_STR)


def download_metadata() -> None:
    """Azure rejimindədirsə, processed_ids.parquet-i Azure-dan lokal keşə endirir."""
    if not USE_AZURE:
        return

    local_path = _local_metadata_path()
    service_client = _get_datalake_service_client()
    fs_client = service_client.get_file_system_client(AZURE_CONTAINER)
    remote_path = "metadata/processed_ids.parquet"

    try:
        file_client = fs_client.get_file_client(remote_path)
        download = file_client.download_file()
        with open(local_path, "wb") as f:
            download.readinto(f)
        print(f"[Azure] Metadata endirildi: {remote_path}")
    except Exception:
        # İlk run zamanı fayl mövcud olmaya bilər — bu normaldır.
        print("[Azure] Uzaqda processed_ids.parquet tapılmadı, boş başlanılır.")


def upload_metadata() -> None:
    """Azure rejimindədirsə, yenilənmiş processed_ids.parquet-i Azure-a geri yükləyir."""
    if not USE_AZURE:
        return

    local_path = _local_metadata_path()
    service_client = _get_datalake_service_client()
    fs_client = service_client.get_file_system_client(AZURE_CONTAINER)
    remote_path = "metadata/processed_ids.parquet"

    file_client = fs_client.get_file_client(remote_path)
    with open(local_path, "rb") as f:
        data = f.read()
    file_client.upload_data(data, overwrite=True)
    print(f"[Azure] Metadata yükləndi: {remote_path}")


def upload_bronze_file(local_file_path: Path) -> None:
    """Azure rejimindədirsə, yaradılmış bronze batch faylını Azure-a yükləyir."""
    if not USE_AZURE:
        return

    service_client = _get_datalake_service_client()
    fs_client = service_client.get_file_system_client(AZURE_CONTAINER)
    remote_path = f"bronze/{local_file_path.name}"

    file_client = fs_client.get_file_client(remote_path)
    with open(local_file_path, "rb") as f:
        data = f.read()
    file_client.upload_data(data, overwrite=True)
    print(f"[Azure] Bronze faylı yükləndi: {remote_path}")