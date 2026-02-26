"""Run Athena-based quality gates and stop on critical failures."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")
BUCKET = os.getenv("S3_BUCKET", "coffee-chain-datalake")

SQL_DIR = Path(
    os.getenv(
        "QUALITY_GATES_SQL_DIR",
        "/app/sql/quality_gates",
    )
)
if not SQL_DIR.exists():
    alt_sql_dir = Path("/opt/airflow/include/sql/quality_gates")
    if alt_sql_dir.exists():
        SQL_DIR = alt_sql_dir

GATES: dict[str, tuple[str, str]] = {
    "01_row_count": ("01_row_count_check.sql", "CRITICAL"),
    "02_all_sources": ("02_all_sources_arrived.sql", "CRITICAL"),
    "03_negative_values": ("03_negative_values.sql", "CRITICAL"),
    "04_referential": ("04_referential_integrity.sql", "WARNING"),
    "05_cost_coverage": ("05_product_cost_coverage.sql", "WARNING"),
    "06_inventory": ("06_inventory_continuity.sql", "WARNING"),
    "07_labor_ratio": ("07_labor_revenue_ratio.sql", "INFO"),
    "08_silver_parity": ("08_silver_row_parity.sql", "CRITICAL"),
}

BLOCKS: dict[str, list[str]] = {
    "completeness": ["01_row_count", "02_all_sources"],
    "integrity": ["03_negative_values", "04_referential", "05_cost_coverage"],
    "consistency": ["06_inventory", "07_labor_ratio"],
    "parity": ["08_silver_parity"],
    "all": list(GATES.keys()),
}


def _athena_client():
    return boto3.client("athena", region_name=REGION)


def run_athena_query(query: str, description: str = "") -> list[dict]:
    """Run Athena query and return result rows (empty list means PASS)."""
    athena = _athena_client()
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
    )
    exec_id = resp["QueryExecutionId"]

    for _ in range(120):
        q = athena.get_query_execution(QueryExecutionId=exec_id)
        state = q["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = q["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query failed [{description}]: {reason}")
        time.sleep(1)
    else:
        raise TimeoutError(f"Athena query timed out [{description}] after 120s")

    rows = athena.get_query_results(QueryExecutionId=exec_id)["ResultSet"]["Rows"]
    if len(rows) <= 1:
        return []

    headers = [col.get("VarCharValue", f"col_{i}") for i, col in enumerate(rows[0]["Data"])]
    parsed_rows: list[dict] = []
    for row in rows[1:]:
        data = row.get("Data", [])
        parsed_rows.append(
            {headers[i]: data[i].get("VarCharValue", "") if i < len(data) else "" for i in range(len(headers))}
        )
    return parsed_rows


def run_gate(gate_name: str) -> dict:
    """Run a single gate and return structured result."""
    sql_file, level = GATES[gate_name]
    sql_path = SQL_DIR / sql_file
    if not sql_path.exists():
        return {
            "gate": gate_name,
            "level": level,
            "status": "ERROR",
            "failures": [{"error": f"SQL file not found: {sql_path}"}],
        }

    failures = run_athena_query(sql_path.read_text(), gate_name)
    return {
        "gate": gate_name,
        "level": level,
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
    }


def _expand_blocks(blocks: list[str] | None) -> list[str]:
    if not blocks:
        return BLOCKS["all"]
    expanded: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        if block not in BLOCKS:
            print(f"  Warning: unknown block '{block}' (ignored)")
            continue
        for gate in BLOCKS[block]:
            if gate not in seen:
                expanded.append(gate)
                seen.add(gate)
    return expanded


def run_all_gates(blocks: list[str] | None = None, run_date: str | None = None) -> dict:
    """Run selected blocks and raise on any critical failure."""
    run_date = run_date or datetime.today().strftime("%Y-%m-%d")
    gates_to_run = _expand_blocks(blocks)
    if not gates_to_run:
        raise ValueError("No valid quality-gate blocks selected.")

    print("\n" + "=" * 55)
    print(f"QUALITY GATES - {run_date}")
    print("=" * 55)
    print(f"  Running {len(gates_to_run)} gates: {', '.join(gates_to_run)}\n")

    results = {
        "run_date": run_date,
        "failed_critical": [],
        "warnings": [],
        "info": [],
        "passed": [],
    }

    for gate_name in gates_to_run:
        _, level = GATES[gate_name]
        print(f"  [{level:<8}] {gate_name}...", end=" ", flush=True)
        result = run_gate(gate_name)

        if result["status"] == "PASS":
            print("PASS")
            results["passed"].append(gate_name)
            continue

        failures = result["failures"]
        print(f"FAIL ({len(failures)} issue(s))")
        for failure in failures[:3]:
            print("    -> " + " | ".join(str(v) for v in failure.values()))

        entry = {"gate": gate_name, "failures": failures}
        if level == "CRITICAL":
            results["failed_critical"].append(entry)
        elif level == "WARNING":
            results["warnings"].append(entry)
        else:
            results["info"].append(entry)

    results_s3_key = f"quality-gate-results/{run_date}/results.json"
    try:
        boto3.client("s3", region_name=REGION).put_object(
            Bucket=BUCKET,
            Key=results_s3_key,
            Body=json.dumps(results, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"\n  Results saved -> s3://{BUCKET}/{results_s3_key}")
    except Exception as exc:  # pragma: no cover - best effort
        print(f"\n  Warning: could not save results to S3: {exc}")

    print("\n" + "-" * 55)
    print(f"  Passed:   {len(results['passed'])}")
    print(f"  Warnings: {len(results['warnings'])}")
    print(f"  Info:     {len(results['info'])}")
    print(f"  Critical: {len(results['failed_critical'])}")

    if results["failed_critical"]:
        failed_names = [e["gate"] for e in results["failed_critical"]]
        raise ValueError(
            f"PIPELINE STOPPED: {len(results['failed_critical'])} critical validation(s) failed: "
            f"{failed_names}. Check s3://{BUCKET}/{results_s3_key} for details."
        )

    print("\nAll critical gates passed - pipeline continues")
    return results


if __name__ == "__main__":
    run_all_gates(blocks=sys.argv[1:] or None)
