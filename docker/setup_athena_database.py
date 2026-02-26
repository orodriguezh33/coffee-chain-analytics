"""Create Athena database for the project and wait for completion."""

from __future__ import annotations

import os
import time

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS", f"s3://{BUCKET}/athena-results/")
POLL_SECONDS = int(os.getenv("ATHENA_POLL_SECONDS", "2"))
TIMEOUT_SECONDS = int(os.getenv("ATHENA_TIMEOUT_SECONDS", "90"))


def _status(athena, qid: str) -> tuple[str, str]:
    resp = athena.get_query_execution(QueryExecutionId=qid)
    status = resp["QueryExecution"]["Status"]["State"]
    reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
    return status, reason


def _db_exists(athena) -> bool:
    next_token = None
    while True:
        kwargs = {"CatalogName": "AwsDataCatalog"}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena.list_databases(**kwargs)
        if any(d["Name"] == ATHENA_DB for d in resp.get("DatabaseList", [])):
            return True
        next_token = resp.get("NextToken")
        if not next_token:
            return False


def main() -> int:
    print("=" * 50)
    print("ATHENA DATABASE SETUP")
    print("=" * 50)
    print(f"Region: {REGION}")
    print(f"Workgroup: {ATHENA_WORKGROUP}")
    print(f"Database: {ATHENA_DB}")
    print(f"Results: {ATHENA_RESULTS}")

    query = (
        f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB} "
        f"COMMENT 'Coffee Chain Analytics Platform' "
        f"LOCATION 's3://{BUCKET}/'"
    )

    try:
        athena = boto3.client("athena", region_name=REGION)
        resp = athena.start_query_execution(
            QueryString=query,
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
        )
        qid = resp["QueryExecutionId"]
        print(f"Started query: {qid}")

        waited = 0
        while waited <= TIMEOUT_SECONDS:
            status, reason = _status(athena, qid)
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                print(f"Final status: {status}")
                if reason:
                    print(f"Reason: {reason}")
                if status != "SUCCEEDED":
                    return 1
                break
            time.sleep(POLL_SECONDS)
            waited += POLL_SECONDS

        if waited > TIMEOUT_SECONDS:
            print("✗ Timed out waiting for Athena query completion")
            return 1

        if _db_exists(athena):
            print(f"✓ Database verified: {ATHENA_DB}")
            return 0
        print(f"✗ Database not found after successful query: {ATHENA_DB}")
        return 1
    except (NoCredentialsError, PartialCredentialsError):
        print("✗ Missing or incomplete AWS credentials")
        return 1
    except ClientError as e:
        err = e.response.get("Error", {})
        print(f"✗ {err.get('Code')} — {err.get('Message')}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
