"""
Exporta un snapshot BI desde Athena a archivos CSV locales.

Uso recomendado:
    python /app/scripts/bi/export_powerbi_snapshot.py

Salida:
    exports/bi_snapshot/<snapshot_id>/*.csv
    exports/bi_snapshot/<snapshot_id>/manifest.json
"""

from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import boto3

from scripts.shared.athena_runner import iter_result_rows, run_query

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "coffee_chain")
BI_DATABASE = os.getenv("BI_DATABASE")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")

TABLES = [
    "fct_sales",
    "fct_waste",
    "fct_labor",
    "fct_inventory_snapshot",
    "dim_date",
    "dim_store",
    "dim_product",
    "dim_ingredient",
]


@dataclass
class ExportResult:
    table: str
    query_execution_id: str
    rows: int
    output_file: str

def _resolve_bi_database() -> str:
    if BI_DATABASE:
        return BI_DATABASE

    glue = boto3.client("glue", region_name=REGION)
    candidates = ["gold", ATHENA_DATABASE, "coffee_chain_dev", "silver"]

    for database_name in candidates:
        try:
            response = glue.get_tables(DatabaseName=database_name, MaxResults=100)
            table_names = {table["Name"] for table in response.get("TableList", [])}
            if all(table in table_names for table in TABLES):
                return database_name
        except Exception:
            continue

    raise RuntimeError(
        "No se pudo detectar la base BI automaticamente. "
        "Define BI_DATABASE en .env (ejemplo: BI_DATABASE=gold)."
    )


def _run_query(query: str, database_name: str) -> str:
    if not ATHENA_RESULTS:
        raise ValueError("ATHENA_RESULTS no esta definido en variables de entorno.")
    retryable_markers = ("TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND")

    for attempt in range(1, 6):
        try:
            return run_query(
                query,
                description=f"export {database_name}",
                database=database_name,
                timeout_seconds=300,
            )
        except RuntimeError as exc:
            reason = str(exc)
            if any(marker in reason for marker in retryable_markers) and attempt < 5:
                wait_seconds = attempt * 4
                print(
                    f"    Reintento {attempt}/5 por error transitorio "
                    f"({reason[:80]}...). Esperando {wait_seconds}s"
                )
                time.sleep(wait_seconds)
                continue
            raise

    raise RuntimeError("Query fallo tras 5 intentos.")


def _iterate_rows(query_execution_id: str) -> Iterable[list[str]]:
    yield from iter_result_rows(query_execution_id, include_header=True)


def _export_table(table: str, output_dir: Path, database_name: str) -> ExportResult:
    query = f'SELECT * FROM "{table}"'
    print(f"  Ejecutando export: {database_name}.{table}")
    query_id = _run_query(query, database_name)

    output_file = output_dir / f"{table}.csv"
    rows_written = 0

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        for row in _iterate_rows(query_id):
            writer.writerow(row)
            rows_written += 1

    data_rows = max(rows_written - 1, 0)
    print(f"    OK -> {output_file} ({data_rows:,} filas)")

    return ExportResult(
        table=table,
        query_execution_id=query_id,
        rows=data_rows,
        output_file=str(output_file),
    )


def main() -> None:
    snapshot_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    base_dir = Path(os.getenv("BI_EXPORT_LOCAL_DIR", "exports/bi_snapshot"))
    output_dir = base_dir / snapshot_id
    output_dir.mkdir(parents=True, exist_ok=True)

    database_name = _resolve_bi_database()

    print("=" * 64)
    print("EXPORT POWER BI SNAPSHOT")
    print("=" * 64)
    print(f"  Region:      {REGION}")
    print(f"  Database BI: {database_name}")
    print(f"  Database DQ: {ATHENA_DATABASE}")
    print(f"  Snapshot ID: {snapshot_id}")
    print(f"  Output dir:  {output_dir}\n")

    results: list[ExportResult] = []
    for table in TABLES:
        results.append(_export_table(table, output_dir, database_name))

    manifest = {
        "snapshot_id": snapshot_id,
        "region": REGION,
        "athena_database": ATHENA_DATABASE,
        "bi_database": database_name,
        "generated_at": datetime.now().isoformat(),
        "tables": [result.__dict__ for result in results],
    }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("\nResumen:")
    for result in results:
        print(f"  - {result.table:<24} {result.rows:>10,} filas")
    print(f"\n  Manifest -> {manifest_path}")
    print("\nExport finalizado.")


if __name__ == "__main__":
    main()
