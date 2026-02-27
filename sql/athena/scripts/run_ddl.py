"""Run Athena DDL and verification queries for Bronze external tables."""

from __future__ import annotations

import os
import time
from pathlib import Path

import boto3

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")


def run_athena_query(query: str, description: str = "", wait: bool = True) -> str:
    """Execute an Athena query and optionally wait for completion."""
    athena = boto3.client("athena", region_name=REGION)
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
    )
    exec_id = resp["QueryExecutionId"]
    if not wait:
        return exec_id

    for _ in range(120):
        status_resp = athena.get_query_execution(QueryExecutionId=exec_id)
        status = status_resp["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            if description:
                print(f"  ✓ {description}")
            return exec_id
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "Unknown error")
            raise RuntimeError(
                f"Query failed: {description or exec_id}\n"
                f"Reason: {reason}\n"
                f"Query: {query[:400]}"
            )
        time.sleep(1)
    raise TimeoutError(f"Query timed out after 120s: {description or exec_id}")


def _split_sql_statements(sql_content: str) -> list[str]:
    """Split SQL by semicolons while preserving comments inside statements."""
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    for ch in sql_content:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if ch == ";" and not in_single and not in_double:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _normalize_sql(sql_content: str) -> str:
    """Replace template placeholders before execution."""
    return sql_content.replace("{{S3_BUCKET}}", BUCKET)


def _statement_description(stmt: str, idx: int) -> str:
    for line in stmt.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            text = stripped.lstrip("-").strip()
            if text:
                return text
    first = stmt.strip().splitlines()[0].strip()
    return f"Statement {idx}: {first[:60]}"


def run_ddl_file(filepath: Path) -> None:
    """Execute a SQL file containing multiple statements."""
    print(f"\n  Executing {filepath.name}...")
    sql_content = _normalize_sql(filepath.read_text())
    for i, stmt in enumerate(_split_sql_statements(sql_content), 1):
        run_athena_query(stmt, _statement_description(stmt, i))


def create_bronze_tables() -> None:
    print("\n" + "=" * 55)
    print("ATHENA DDL: Creating Bronze tables")
    print("=" * 55)
    ddl_file = Path(__file__).parent.parent / "ddl" / "create_bronze_tables.sql"
    run_ddl_file(ddl_file)


def repair_partitions() -> None:
    print("\n-- Repairing partitions ---------------------")
    tables = [
        "bronze_pos_transactions",
        "bronze_product_costs",
        "bronze_recipes_bom",
        "bronze_daily_inventory",
        "bronze_staff_shifts",
    ]
    for table in tables:
        run_athena_query(
            f"MSCK REPAIR TABLE coffee_chain.{table}",
            f"Partitions registered: {table}",
        )
    run_athena_query(
        "SELECT COUNT(*) FROM coffee_chain.bronze_promotions",
        "Promotions table accessible",
    )


def _get_scalar_query_result(athena, exec_id: str) -> str:
    results = athena.get_query_results(QueryExecutionId=exec_id)
    rows = results["ResultSet"]["Rows"]
    if len(rows) < 2:
        return "0"
    return rows[1]["Data"][0].get("VarCharValue", "0")


def verify_tables() -> None:
    print("\n-- Verifying table contents -----------------")
    checks = [
        ("bronze_pos_transactions", "SELECT COUNT(*) FROM coffee_chain.bronze_pos_transactions"),
        ("bronze_product_costs", "SELECT COUNT(*) FROM coffee_chain.bronze_product_costs"),
        ("bronze_recipes_bom", "SELECT COUNT(*) FROM coffee_chain.bronze_recipes_bom"),
        ("bronze_daily_inventory", "SELECT COUNT(*) FROM coffee_chain.bronze_daily_inventory"),
        ("bronze_staff_shifts", "SELECT COUNT(*) FROM coffee_chain.bronze_staff_shifts"),
        ("bronze_promotions", "SELECT COUNT(*) FROM coffee_chain.bronze_promotions"),
    ]
    athena = boto3.client("athena", region_name=REGION)
    for table_name, query in checks:
        exec_id = run_athena_query(query, wait=True)
        count = _get_scalar_query_result(athena, exec_id)
        print(f"  ✓ {table_name:<30} {int(count):>10,} rows")


if __name__ == "__main__":
    create_bronze_tables()
    repair_partitions()
    verify_tables()
    print("\n✓ Bronze layer fully configured in Athena")
