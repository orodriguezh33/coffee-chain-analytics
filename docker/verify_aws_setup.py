"""Verify S3/Athena AWS setup for the project."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS", f"s3://{BUCKET}/athena-results/")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")


def _safe_err(prefix: str, e: Exception) -> bool:
    if isinstance(e, (NoCredentialsError, PartialCredentialsError)):
        print(f"  ✗ {prefix}: Missing or incomplete AWS credentials")
        return False
    if isinstance(e, ClientError):
        print(f"  ✗ {prefix}: {e.response.get('Error', {}).get('Code')} — {e.response.get('Error', {}).get('Message')}")
        return False
    print(f"  ✗ {prefix}: {e}")
    return False


def _list_all_athena_databases(athena) -> list[str]:
    names: list[str] = []
    next_token = None
    while True:
        kwargs = {"CatalogName": "AwsDataCatalog"}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena.list_databases(**kwargs)
        names.extend([d["Name"] for d in resp.get("DatabaseList", [])])
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return names


def verify_s3() -> bool:
    print("\n-- S3 --------------------------------------")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"  ✓ Bucket exists: {BUCKET}")

        resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/")
        existing = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        print(f"  ✓ Top-level prefixes: {existing}")
        return True
    except Exception as e:
        return _safe_err("S3 error", e)


def verify_s3_write() -> bool:
    print("\n-- S3 Write Test ---------------------------")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        test_key = "athena-results/_connection_test.txt"
        s3.put_object(Bucket=BUCKET, Key=test_key, Body=b"connection test")
        s3.delete_object(Bucket=BUCKET, Key=test_key)
        print("  ✓ Can write and delete from S3")
        return True
    except Exception as e:
        return _safe_err("S3 write error", e)


def verify_athena() -> bool:
    print("\n-- Athena ----------------------------------")
    athena = boto3.client("athena", region_name=REGION)
    try:
        db_names = _list_all_athena_databases(athena)
        if ATHENA_DB in db_names:
            print(f"  ✓ Database exists: {ATHENA_DB}")
        else:
            print(f"  ✗ Database not found: {ATHENA_DB}")
            print(f"    Existing databases: {db_names}")
            return False

        resp = athena.start_query_execution(
            QueryString=f"SHOW TABLES IN {ATHENA_DB}",
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
        )
        print(f"  ✓ Can execute queries (id: {resp['QueryExecutionId'][:8]}...)")
        return True
    except Exception as e:
        return _safe_err("Athena error", e)


def _validate_env() -> bool:
    parsed = urlparse(ATHENA_RESULTS)
    ok = parsed.scheme == "s3" and bool(parsed.netloc)
    if not ok:
        print(f"  ✗ Invalid ATHENA_RESULTS: {ATHENA_RESULTS}")
    return ok


def main() -> bool:
    print("=" * 50)
    print("AWS SETUP VERIFICATION")
    print("=" * 50)
    print(f"  Region: {REGION}")
    print(f"  Bucket: {BUCKET}")
    print(f"  Athena DB: {ATHENA_DB}")
    print(f"  Athena WG: {ATHENA_WORKGROUP}")
    print(f"  Athena Results: {ATHENA_RESULTS}")

    if not _validate_env():
        print("\n✗ Fix ATHENA_RESULTS before continuing")
        return False

    results = {
        "s3": verify_s3(),
        "s3_write": verify_s3_write(),
        "athena": verify_athena(),
    }

    print("\n-- Summary ---------------------------------")
    all_passed = all(results.values())
    for check_name, passed in results.items():
        icon = "✓" if passed else "✗"
        print(f"  {icon} {check_name}")

    if all_passed:
        print("\n✓ AWS setup complete - ready for M3")
    else:
        print("\n✗ Fix the issues above before continuing")
    return all_passed


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
