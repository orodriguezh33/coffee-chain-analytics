"""Configuración compartida para tareas ejecutadas desde Airflow."""

from __future__ import annotations

import os


def build_airflow_env() -> dict[str, str]:
    """Retorna variables de entorno comunes para tareas del DAG."""
    return {
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "S3_BUCKET": os.getenv("S3_BUCKET", ""),
        "ATHENA_DATABASE": os.getenv("ATHENA_DATABASE", "coffee_chain"),
        "ATHENA_WORKGROUP": os.getenv("ATHENA_WORKGROUP", "primary"),
        "ATHENA_RESULTS": os.getenv("ATHENA_RESULTS", ""),
        "BI_EXPORT_LOCAL_DIR": "/opt/airflow/exports/bi_snapshot",
        "DBT_TARGET": os.getenv("DBT_TARGET", "dev"),
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": "/opt/airflow",
    }
