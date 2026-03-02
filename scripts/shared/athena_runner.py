"""Utilidades compartidas para ejecutar queries en Athena."""

from __future__ import annotations

import os
import time
from typing import Iterable

import boto3

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")


def get_athena_client():
    """Retorna un cliente de Athena con la región configurada."""
    return boto3.client("athena", region_name=REGION)


def start_query(query: str, database: str | None = None) -> str:
    """Inicia una query en Athena y retorna el QueryExecutionId."""
    if not ATHENA_RESULTS:
        raise ValueError("ATHENA_RESULTS no está definido.")

    response = get_athena_client().start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database or ATHENA_DB},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS},
    )
    return response["QueryExecutionId"]


def wait_for_query(query_execution_id: str, description: str = "", timeout_seconds: int = 120) -> None:
    """Espera hasta que una query termine o falle."""
    athena = get_athena_client()
    for _ in range(timeout_seconds):
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = status["QueryExecution"]["Status"]
        state = query_status["State"]
        if state == "SUCCEEDED":
            return
        if state in {"FAILED", "CANCELLED"}:
            reason = query_status.get("StateChangeReason", "Unknown error")
            raise RuntimeError(f"Athena query failed [{description or query_execution_id}]: {reason}")
        time.sleep(1)
    raise TimeoutError(f"Athena query timed out [{description or query_execution_id}] after {timeout_seconds}s")


def run_query(query: str, description: str = "", database: str | None = None, timeout_seconds: int = 120) -> str:
    """Ejecuta una query completa y retorna el QueryExecutionId."""
    query_execution_id = start_query(query, database=database)
    wait_for_query(query_execution_id, description=description, timeout_seconds=timeout_seconds)
    return query_execution_id


def iter_result_rows(query_execution_id: str, include_header: bool = True) -> Iterable[list[str]]:
    """Itera las filas devueltas por Athena."""
    athena = get_athena_client()
    next_token = None
    first_page = True

    while True:
        params = {"QueryExecutionId": query_execution_id}
        if next_token:
            params["NextToken"] = next_token

        response = athena.get_query_results(**params)
        rows = response["ResultSet"]["Rows"]

        if not include_header and first_page:
            rows = rows[1:]

        for row in rows:
            yield [column.get("VarCharValue", "") for column in row.get("Data", [])]

        next_token = response.get("NextToken")
        first_page = False
        if not next_token:
            break


def fetch_result_dicts(query_execution_id: str) -> list[dict[str, str]]:
    """Obtiene resultados como lista de diccionarios."""
    rows = list(iter_result_rows(query_execution_id, include_header=True))
    if len(rows) <= 1:
        return []

    headers = rows[0]
    parsed_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        padded = row + [""] * max(0, len(headers) - len(row))
        parsed_rows.append({headers[i] or f"col_{i}": padded[i] for i in range(len(headers))})
    return parsed_rows


def fetch_scalar(query_execution_id: str, default: str = "0") -> str:
    """Retorna el primer valor de la primera fila de resultados."""
    rows = list(iter_result_rows(query_execution_id, include_header=True))
    if len(rows) < 2 or not rows[1]:
        return default
    return rows[1][0] or default
