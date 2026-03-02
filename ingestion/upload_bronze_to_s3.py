"""Sube archivos CSV de Bronze local a S3 preservando particiones."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3

BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
BRONZE_LOCAL = Path(os.getenv("DATA_BRONZE", "data/bronze"))
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


def resolve_run_date(run_date: str | None = None) -> str:
    """Resuelve run_date desde argumento/env/fecha actual."""
    from datetime import datetime

    return run_date or os.getenv("RUN_DATE") or datetime.today().strftime("%Y-%m-%d")


def upload_file(s3_client, local_path: Path, s3_key: str) -> bool:
    """Sube un archivo a S3 y muestra el progreso."""
    try:
        s3_client.upload_file(str(local_path), BUCKET, s3_key)
        size_kb = local_path.stat().st_size / 1024
        print(f"  ✓ {s3_key} ({size_kb:.1f} KB)")
        return True
    except Exception as e:  # pragma: no cover - boto/network/runtime path
        print(f"  ✗ Falló {s3_key}: {e}")
        return False


def upload_bronze_to_s3(run_date: str | None = None, full_refresh: bool = False) -> int:
    """Sube Bronze a S3; por defecto solo la partición de run_date."""
    resolved_run_date = resolve_run_date(run_date)

    print("\n" + "=" * 55)
    print("UPLOAD: Bronze Local -> S3 Bronze")
    print("=" * 55)
    print(f"  Origen:      {BRONZE_LOCAL}")
    print(f"  Destino:     s3://{BUCKET}/bronze/")

    s3 = boto3.client("s3", region_name=REGION)
    if full_refresh:
        csv_files = sorted(BRONZE_LOCAL.rglob("*.csv"))
        print("  Modo:        recarga completa")
    else:
        csv_files = sorted(BRONZE_LOCAL.rglob(f"ingestion_date={resolved_run_date}/*.csv"))
        print(f"  Modo:        incremental ({resolved_run_date})")
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found for run_date={resolved_run_date} in {BRONZE_LOCAL}. "
            "Ejecuta primero la ingesta o usa full_refresh=True."
        )

    print(f"\n  Se encontraron {len(csv_files)} archivos para subir:\n")
    uploaded = 0
    failed = 0

    for local_path in csv_files:
        relative = local_path.relative_to(BRONZE_LOCAL)
        s3_key = f"bronze/{relative.as_posix()}"
        if upload_file(s3, local_path, s3_key):
            uploaded += 1
        else:
            failed += 1

    print(f"\n{'=' * 55}")
    print(f"  Subidos: {uploaded} archivos")
    if failed:
        print(f"  Fallidos: {failed} archivos")
        raise RuntimeError(f"{failed} archivos fallaron al subir")
    print("  ✓ Todos los archivos se subieron correctamente")
    return uploaded


def verify_s3_upload() -> bool:
    """Verifica que los prefijos esperados en Bronze tengan objetos en S3."""
    print("\n-- Verificando contenido en S3 ---------------")
    s3 = boto3.client("s3", region_name=REGION)

    expected_prefixes = [
        "bronze/pos/transactions/",
        "bronze/synthetic/product_costs/",
        "bronze/synthetic/recipes_bom/",
        "bronze/synthetic/daily_inventory/",
        "bronze/synthetic/staff_shifts/",
        "bronze/synthetic/promotions/",
    ]

    all_good = True
    for prefix in expected_prefixes:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=5)
        contents = [c for c in resp.get("Contents", []) if not c["Key"].endswith("/")]
        if contents:
            key = contents[0]["Key"]
            size = contents[0]["Size"]
            print(f"  ✓ {prefix}")
            print(f"    -> {key.split('/')[-1]} ({size / 1024:.1f} KB)")
        else:
            print(f"  ✗ No se encontraron archivos en {prefix}")
            all_good = False
    return all_good


if __name__ == "__main__":
    full_refresh = "--all" in sys.argv
    run_date = None
    for arg in sys.argv[1:]:
        if arg != "--all":
            run_date = arg
            break

    upload_bronze_to_s3(run_date=run_date, full_refresh=full_refresh)
    ok = verify_s3_upload()
    if not ok:
        raise SystemExit(1)
    print("\n✓ Capa Bronze lista en S3")
