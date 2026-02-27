"""
Exporta un snapshot BI desde Athena a archivos CSV locales.

Uso recomendado:
    python /app/sql/bi/scripts/export_powerbi_snapshot.py

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


def _athena_client():
    return boto3.client("athena", region_name=REGION)


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

    athena = _athena_client()
    retryable_markers = ("TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND")

    for attempt in range(1, 6):
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database_name},
            ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
        )
        query_id = response["QueryExecutionId"]

        for _ in range(300):
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return query_id
            if state in {"FAILED", "CANCELLED"}:
                reason = status["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Error no identificado"
                )
                if (
                    any(marker in reason for marker in retryable_markers)
                    and attempt < 5
                ):
                    wait_seconds = attempt * 4
                    print(
                        f"    Reintento {attempt}/5 por error transitorio "
                        f"({reason[:80]}...). Esperando {wait_seconds}s"
                    )
                    time.sleep(wait_seconds)
                    break
                raise RuntimeError(f"Query fallo ({query_id}): {reason}")
            time.sleep(1)
        else:
            raise TimeoutError(f"Query timeout ({query_id})")

    raise RuntimeError("Query fallo tras 5 intentos.")


def _iterate_rows(query_execution_id: str) -> Iterable[list[str]]:
    athena = _athena_client()
    next_token = None
    first_page = True

    while True:
        params = {"QueryExecutionId": query_execution_id}
        if next_token:
            params["NextToken"] = next_token

        response = athena.get_query_results(**params)
        rows = response["ResultSet"]["Rows"]

        if first_page:
            first_page = False
            for row in rows:
                values = [col.get("VarCharValue", "") for col in row["Data"]]
                yield values
        else:
            for row in rows:
                values = [col.get("VarCharValue", "") for col in row["Data"]]
                yield values

        next_token = response.get("NextToken")
        if not next_token:
            break


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

    # El primer row es el header de Athena.
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
