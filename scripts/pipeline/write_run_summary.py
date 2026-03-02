"""Escribe el resumen de ejecución del DAG en S3."""

from __future__ import annotations

import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def _resolve_bucket() -> str | None:
    return os.getenv("S3_BUCKET")


def write_run_summary(
    *,
    run_date: str,
    run_id: str,
    status: str,
    failed_tasks: list[str] | None = None,
) -> None:
    """Guarda el estado final del pipeline en S3."""
    bucket = _resolve_bucket()
    if not bucket:
        print("S3_BUCKET no configurado; se omite auditoría en S3")
        return

    summary = {
        "run_date": run_date,
        "status": status,
        "completed": datetime.utcnow().isoformat(),
        "dag_run_id": run_id,
    }
    if failed_tasks:
        summary["failed_task"] = failed_tasks[0]
        summary["failed_tasks"] = failed_tasks
        summary["action"] = "Revisar logs de tareas en Airflow"

    try:
        boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")).put_object(
            Bucket=bucket,
            Key=f"pipeline-runs/{run_date}/summary.json",
            Body=json.dumps(summary, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as error:
        print(f"No se pudo escribir el resumen del pipeline en S3: {error}")
