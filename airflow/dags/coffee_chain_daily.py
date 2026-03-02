from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from scripts.pipeline.airflow_env import build_airflow_env
from scripts.pipeline.tasks import (
    export_bi_snapshot_task,
    ingest_pos_task,
    ingest_synthetic_task,
    pipeline_failure_task,
    pipeline_success_task,
    quality_gates_post_task,
    quality_gates_pre_task,
    repair_partitions_task,
    upload_bronze_task,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="coffee_chain_daily",
    description="Pipeline diario: ingesta -> QA -> dbt -> QA -> export BI -> auditoría",
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["coffee-chain", "diario", "athena"],
    doc_md="""
    ## Pipeline Diario Coffee Chain

    Flujo: ingesta -> upload Bronze -> repair partitions -> quality gates pre
    -> dbt run -> dbt test -> quality gates post -> export BI -> marcador de auditoría.
    Redshift queda fuera de alcance; el pipeline termina en S3 Gold + Athena.
    """,
) as dag:
    t_ingest_pos = PythonOperator(
        task_id="ingest_pos_to_bronze",
        python_callable=ingest_pos_task,
        doc_md="Process POS CSV into local Bronze partition.",
    )

    t_ingest_synthetic = PythonOperator(
        task_id="ingest_synthetic_sources",
        python_callable=ingest_synthetic_task,
        doc_md="Generate synthetic ERP/WMS/labor/CRM sources into local Bronze.",
    )

    t_upload_bronze = PythonOperator(
        task_id="upload_bronze_to_s3",
        python_callable=upload_bronze_task,
        doc_md="Upload local Bronze files to S3 Bronze paths.",
    )

    t_repair_partitions = PythonOperator(
        task_id="repair_athena_partitions",
        python_callable=repair_partitions_task,
        doc_md="Register new Bronze partitions in Athena.",
    )

    t_quality_pre = PythonOperator(
        task_id="quality_gates_pre_dbt",
        python_callable=quality_gates_pre_task,
        doc_md="Run pre-dbt completeness and integrity checks.",
    )

    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        set -e
        export PATH="/home/airflow/.local/bin:$PATH"
        cd /opt/airflow/dbt
        dbt deps --profiles-dir . --target dev --quiet
        dbt run --profiles-dir . --target dev --no-use-colors
        """,
        env=build_airflow_env(),
        doc_md="Build Silver and Gold models in Athena.",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        set -e
        export PATH="/home/airflow/.local/bin:$PATH"
        cd /opt/airflow/dbt
        dbt deps --profiles-dir . --target dev --quiet
        dbt test --profiles-dir . --target dev --no-use-colors
        """,
        env=build_airflow_env(),
        doc_md="Run dbt tests after model build.",
    )

    t_quality_post = PythonOperator(
        task_id="quality_gates_post_dbt",
        python_callable=quality_gates_post_task,
        doc_md="Run post-dbt consistency and parity checks.",
    )

    t_export_bi = PythonOperator(
        task_id="export_bi_snapshot",
        python_callable=export_bi_snapshot_task,
        doc_md="Exporta snapshot BI desde Athena a CSV para Power BI.",
    )

    def decide_pipeline_result(**_ctx):
        return "pipeline_success"

    t_branch = BranchPythonOperator(
        task_id="evaluate_pipeline_result",
        python_callable=decide_pipeline_result,
    )

    t_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=pipeline_success_task,
        doc_md="Persist success summary to S3.",
    )

    t_failure = PythonOperator(
        task_id="pipeline_failure",
        python_callable=pipeline_failure_task,
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="Guarda resumen de fallo en S3 cuando cualquier upstream falla.",
    )

    t_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [t_ingest_pos, t_ingest_synthetic] >> t_upload_bronze
    (
        t_upload_bronze
        >> t_repair_partitions
        >> t_quality_pre
        >> t_dbt_run
        >> t_dbt_test
        >> t_quality_post
        >> t_export_bi
        >> t_branch
    )
    t_branch >> t_success >> t_end

    # Failure logger watches the main path and still allows the DAG to close via end.
    [
        t_ingest_pos,
        t_ingest_synthetic,
        t_upload_bronze,
        t_repair_partitions,
        t_quality_pre,
        t_dbt_run,
        t_dbt_test,
        t_quality_post,
        t_export_bi,
        t_success,
    ] >> t_failure
    t_failure >> t_end
