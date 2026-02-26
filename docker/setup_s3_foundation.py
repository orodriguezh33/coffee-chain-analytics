"""Create/verify S3 bucket and foundational prefixes for the project."""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")

PREFIXES = [
    "bronze/pos/transactions/",
    "bronze/synthetic/product_costs/",
    "bronze/synthetic/recipes_bom/",
    "bronze/synthetic/daily_inventory/",
    "bronze/synthetic/staff_shifts/",
    "bronze/synthetic/promotions/",
    "silver/pos/transactions/",
    "silver/product_costs/",
    "silver/recipes_bom/",
    "silver/daily_inventory/",
    "silver/staff_shifts/",
    "silver/promotions/",
    "gold/dimensions/",
    "gold/facts/",
    "athena-results/",
]


def _fail(msg: str) -> int:
    print(f"✗ {msg}")
    return 1


def ensure_bucket(s3) -> bool:
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"✓ Bucket exists: {BUCKET}")
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            print(f"Bucket not found, creating: {BUCKET} (region={REGION})")
            kwargs = {"Bucket": BUCKET}
            if REGION != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": REGION}
            s3.create_bucket(**kwargs)
            print(f"✓ Bucket created: {BUCKET}")
            return True
        raise


def create_prefixes(s3) -> None:
    print("\nCreating/verifying prefixes...")
    for key in PREFIXES:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"")
        print(f"  ✓ {key}")


def verify_top_level_prefixes(s3) -> None:
    resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/")
    tops = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    print(f"\nTop-level prefixes: {tops}")


def main() -> int:
    print("=" * 50)
    print("S3 FOUNDATION SETUP")
    print("=" * 50)
    print(f"Region: {REGION}")
    print(f"Bucket: {BUCKET}")

    try:
        s3 = boto3.client("s3", region_name=REGION)
        ensure_bucket(s3)
        create_prefixes(s3)
        verify_top_level_prefixes(s3)
        print("\n✓ S3 foundation ready")
        return 0
    except (NoCredentialsError, PartialCredentialsError):
        return _fail("Missing or incomplete AWS credentials")
    except ClientError as e:
        err = e.response.get("Error", {})
        return _fail(f"{err.get('Code')} — {err.get('Message')}")
    except Exception as e:  # pragma: no cover
        return _fail(str(e))


if __name__ == "__main__":
    raise SystemExit(main())
