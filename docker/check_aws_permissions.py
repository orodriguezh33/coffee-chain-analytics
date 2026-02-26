"""Verify minimum AWS permissions needed for the project."""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
EXPECTED_BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")


def check(service_name: str, fn, success_msg: str) -> bool:
    """Run a permission check and print a friendly result."""
    try:
        fn()
        print(f"  ✓ {service_name}: {success_msg}")
        return True
    except (NoCredentialsError, PartialCredentialsError):
        print(f"  ✗ {service_name}: Missing or incomplete AWS credentials")
        return False
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        print(f"  ✗ {service_name}: {code} — {msg}")
        return False
    except Exception as e:  # pragma: no cover - defensive fallback
        print(f"  ✗ {service_name}: {e}")
        return False


def main() -> None:
    print("\n=== AWS PERMISSIONS CHECK ===\n")
    print(f"Region: {REGION}")
    print(f"Bucket (expected): {EXPECTED_BUCKET}\n")

    results: dict[str, bool] = {}

    s3 = boto3.client("s3", region_name=REGION)
    results["s3_list"] = check("S3", lambda: s3.list_buckets(), "Can list buckets")
    results["s3_access"] = check(
        "S3",
        lambda: s3.head_bucket(Bucket=EXPECTED_BUCKET),
        f"Can access bucket '{EXPECTED_BUCKET}'",
    )

    athena = boto3.client("athena", region_name=REGION)
    results["athena_list"] = check(
        "Athena", lambda: athena.list_work_groups(), "Can list workgroups"
    )
    results["athena_query"] = check(
        "Athena",
        lambda: athena.list_databases(CatalogName="AwsDataCatalog"),
        "Can list databases",
    )

    glue = boto3.client("glue", region_name=REGION)
    results["glue_databases"] = check(
        "Glue", lambda: glue.get_databases(MaxResults=1), "Can access Data Catalog"
    )

    rs = boto3.client("redshift-serverless", region_name=REGION)
    results["redshift_list"] = check(
        "Redshift Serverless", lambda: rs.list_namespaces(), "Can list namespaces"
    )

    iam = boto3.client("iam", region_name=REGION)
    results["iam_list"] = check("IAM", lambda: iam.list_roles(MaxItems=1), "Can list roles")

    print("\n=== SUMMARY ===")
    passed = sum(results.values())
    total = len(results)
    print(f"\n  {passed}/{total} permissions verified")

    critical_missing = []
    if not results.get("s3_list"):
        critical_missing.append("S3 — CRITICAL: project cannot run without S3")
    if not results.get("athena_query"):
        critical_missing.append("Athena — CRITICAL: needed for quality gates and dbt")
    if not results.get("glue_databases"):
        critical_missing.append("Glue — CRITICAL: needed for Athena Data Catalog")

    if critical_missing:
        print("\n  CRITICAL permissions missing:")
        for item in critical_missing:
            print(f"    - {item}")
        print("\n  -> Ask your AWS admin for these permissions before continuing")
    else:
        print("\n  ✓ All critical permissions available")
        print("  -> Ready to proceed with M2")

    if not results.get("iam_list"):
        print("\n  Info: IAM is limited; create/use role manually via console if needed")


if __name__ == "__main__":
    main()
