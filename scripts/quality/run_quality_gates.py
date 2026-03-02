"""Ejecuta quality gates en Athena y detiene el flujo ante fallos críticos."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3

from scripts.shared.athena_runner import fetch_result_dicts, run_query

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
    alt_sql_dir = Path("/opt/airflow/sql/quality_gates")
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


def run_athena_query(query: str, description: str = "") -> list[dict]:
    """Ejecuta una query en Athena y retorna filas; lista vacía = PASS."""
    query_execution_id = run_query(
        query,
        description=description,
        database=ATHENA_DB,
        timeout_seconds=120,
    )
    return fetch_result_dicts(query_execution_id)


def run_gate(gate_name: str) -> dict:
    """Ejecuta un gate individual y retorna resultado estructurado."""
    sql_file, level = GATES[gate_name]
    sql_path = SQL_DIR / sql_file
    if not sql_path.exists():
        return {
            "gate": gate_name,
            "level": level,
            "status": "ERROR",
            "failures": [{"error": f"Archivo SQL no encontrado: {sql_path}"}],
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
            print(f"  Advertencia: bloque desconocido '{block}' (omitido)")
            continue
        for gate in BLOCKS[block]:
            if gate not in seen:
                expanded.append(gate)
                seen.add(gate)
    return expanded


def run_all_gates(blocks: list[str] | None = None, run_date: str | None = None) -> dict:
    """Ejecuta bloques seleccionados y lanza excepción si falla un CRITICAL."""
    run_date = run_date or datetime.today().strftime("%Y-%m-%d")
    gates_to_run = _expand_blocks(blocks)
    if not gates_to_run:
        raise ValueError("No hay bloques válidos de quality gates para ejecutar.")

    print("\n" + "=" * 55)
    print(f"QUALITY GATES - {run_date}")
    print("=" * 55)
    print(f"  Ejecutando {len(gates_to_run)} gates: {', '.join(gates_to_run)}\n")

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
        print(f"FAIL ({len(failures)} incidencia(s))")
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
        print(f"\n  Advertencia: no se pudo guardar resultado en S3: {exc}")

    print("\n" + "-" * 55)
    print(f"  Passed:    {len(results['passed'])}")
    print(f"  Warnings:  {len(results['warnings'])}")
    print(f"  Info:      {len(results['info'])}")
    print(f"  Critical:  {len(results['failed_critical'])}")

    if results["failed_critical"]:
        failed_names = [e["gate"] for e in results["failed_critical"]]
        raise ValueError(
            f"PIPELINE STOPPED: {len(results['failed_critical'])} validación(es) "
            f"crítica(s) fallaron: {failed_names}. Revisa "
            f"s3://{BUCKET}/{results_s3_key} para más detalle."
        )

    print("\nTodas las validaciones críticas pasaron; el pipeline continúa")
    return results


if __name__ == "__main__":
    run_all_gates(blocks=sys.argv[1:] or None)
