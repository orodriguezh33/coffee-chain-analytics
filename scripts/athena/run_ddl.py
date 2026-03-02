"""Run Athena DDL and verification queries for Bronze external tables."""

from __future__ import annotations

import os
from pathlib import Path

from scripts.shared.athena_runner import fetch_scalar, get_athena_client, run_query

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")


def run_athena_query(query: str, description: str = "") -> str:
    """Execute an Athena query and wait for completion."""
    exec_id = run_query(
        query,
        description=description,
        database=ATHENA_DB,
        timeout_seconds=120,
    )
    if description:
        print(f"  ✓ {description}")
    return exec_id


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
    ddl_file = Path(__file__).parents[2] / "sql" / "athena" / "ddl" / "create_bronze_tables.sql"
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
    athena = get_athena_client()
    for table_name, query in checks:
        exec_id = run_athena_query(query)
        count = fetch_scalar(exec_id)
        print(f"  ✓ {table_name:<30} {int(count):>10,} rows")


if __name__ == "__main__":
    create_bronze_tables()
    repair_partitions()
    verify_tables()
    print("\n✓ Bronze layer fully configured in Athena")
