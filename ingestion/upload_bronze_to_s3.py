"""Upload local Bronze CSV files to S3 preserving partitioned paths."""

from __future__ import annotations

import os
from pathlib import Path

import boto3

BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
BRONZE_LOCAL = Path(os.getenv("DATA_BRONZE", "data/bronze"))
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


def upload_file(s3_client, local_path: Path, s3_key: str) -> bool:
    """Upload one file to S3 and print progress."""
    try:
        s3_client.upload_file(str(local_path), BUCKET, s3_key)
        size_kb = local_path.stat().st_size / 1024
        print(f"  ✓ {s3_key} ({size_kb:.1f} KB)")
        return True
    except Exception as e:  # pragma: no cover - boto/network/runtime path
        print(f"  ✗ Failed {s3_key}: {e}")
        return False


def upload_bronze_to_s3() -> int:
    """
    Upload all files from local Bronze to s3://bucket/bronze/
    preserving the existing folder/partition structure.
    """
    print("\n" + "=" * 55)
    print("UPLOAD: Local Bronze -> S3 Bronze")
    print("=" * 55)
    print(f"  Source:      {BRONZE_LOCAL}")
    print(f"  Destination: s3://{BUCKET}/bronze/")

    s3 = boto3.client("s3", region_name=REGION)
    csv_files = sorted(BRONZE_LOCAL.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {BRONZE_LOCAL}. "
            "Run M1 first to generate data/bronze/."
        )

    print(f"\n  Found {len(csv_files)} files to upload:\n")
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
    print(f"  Uploaded: {uploaded} files")
    if failed:
        print(f"  Failed:   {failed} files")
        raise RuntimeError(f"{failed} files failed to upload")
    print("  ✓ All files uploaded successfully")
    return uploaded


def verify_s3_upload() -> bool:
    """Verify expected Bronze prefixes have at least one object in S3."""
    print("\n-- Verifying S3 contents --------------------")
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
            print(f"  ✗ No files found in {prefix}")
            all_good = False
    return all_good


if __name__ == "__main__":
    upload_bronze_to_s3()
    ok = verify_s3_upload()
    if not ok:
        raise SystemExit(1)
    print("\n✓ Bronze layer ready in S3")
