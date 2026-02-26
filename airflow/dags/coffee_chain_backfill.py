from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/include/scripts")


def _env_common() -> dict[str, str]:
    return {
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "S3_BUCKET": os.getenv("S3_BUCKET", ""),
        "ATHENA_DATABASE": os.getenv("ATHENA_DATABASE", "coffee_chain"),
        "ATHENA_WORKGROUP": os.getenv("ATHENA_WORKGROUP", "primary"),
        "ATHENA_RESULTS": os.getenv("ATHENA_RESULTS", ""),
        "DBT_TARGET": os.getenv("DBT_TARGET", "dev"),
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    }


with DAG(
    dag_id="coffee_chain_backfill",
    description="Manual backfill for a date range (dbt-only)",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "start_date": Param("2023-01-01", type="string", description="Start date YYYY-MM-DD"),
        "end_date": Param("2023-01-07", type="string", description="End date YYYY-MM-DD"),
        "run_quality_gates": Param(True, type="boolean", description="Run quality gates after dbt"),
    },
    tags=["coffee-chain", "backfill", "manual"],
) as dag:
    t_dbt_backfill = BashOperator(
        task_id="dbt_backfill",
        bash_command="""
        set -e
        export PATH="/home/airflow/.local/bin:$PATH"
        cd /opt/airflow/dbt
        dbt deps --profiles-dir . --target dev --quiet
        dbt run --profiles-dir . --target dev --no-use-colors \
          --vars '{"start_date": "{{ params.start_date }}", "end_date": "{{ params.end_date }}"}'
        """,
        env=_env_common(),
    )

    t_dbt_test_backfill = BashOperator(
        task_id="dbt_test_backfill",
        bash_command="""
        set -e
        export PATH="/home/airflow/.local/bin:$PATH"
        cd /opt/airflow/dbt
        dbt deps --profiles-dir . --target dev --quiet
        dbt test --profiles-dir . --target dev --no-use-colors
        """,
        env=_env_common(),
    )

    def run_gates_backfill(**ctx):
        if not ctx["params"].get("run_quality_gates", True):
            print("Quality gates skipped by parameter")
            return
        from run_quality_gates import run_all_gates

        run_all_gates(blocks=["all"])

    t_quality_backfill = PythonOperator(
        task_id="quality_gates_backfill",
        python_callable=run_gates_backfill,
    )

    t_dbt_backfill >> t_dbt_test_backfill >> t_quality_backfill
