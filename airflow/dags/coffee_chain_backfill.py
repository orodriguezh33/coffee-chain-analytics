from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts.pipeline.airflow_env import build_airflow_env
from scripts.pipeline.tasks import quality_gates_backfill_task


with DAG(
    dag_id="coffee_chain_backfill",
    description="Backfill manual para un rango de fechas (solo dbt)",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "start_date": Param("2023-01-01", type="string", description="Fecha inicio YYYY-MM-DD"),
        "end_date": Param("2023-01-07", type="string", description="Fecha fin YYYY-MM-DD"),
        "run_quality_gates": Param(
            True,
            type="boolean",
            description="Ejecutar quality gates después de dbt",
        ),
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
        env=build_airflow_env(),
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
        env=build_airflow_env(),
    )

    t_quality_backfill = PythonOperator(
        task_id="quality_gates_backfill",
        python_callable=quality_gates_backfill_task,
    )

    t_dbt_backfill >> t_dbt_test_backfill >> t_quality_backfill
