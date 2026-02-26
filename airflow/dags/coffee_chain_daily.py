from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/ingestion")
sys.path.insert(0, "/opt/airflow/include/scripts")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


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
    dag_id="coffee_chain_daily",
    description="Daily pipeline: ingest -> QA -> dbt -> QA -> audit log",
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["coffee-chain", "daily", "athena"],
    doc_md="""
    ## Coffee Chain Daily Pipeline

    Flow: ingest -> upload Bronze -> repair Athena partitions -> quality gates pre -> dbt run -> dbt test -> quality gates post -> audit marker.
    Redshift is intentionally excluded; pipeline ends in S3 Gold + Athena.
    """,
) as dag:
    def ingest_pos(**_ctx):
        from ingest_pos import run_ingest_pos

        os.environ["DATA_RAW"] = "/opt/airflow/data/raw"
        os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
        return run_ingest_pos()

    t_ingest_pos = PythonOperator(
        task_id="ingest_pos_to_bronze",
        python_callable=ingest_pos,
        doc_md="Process POS CSV into local Bronze partition.",
    )

    def ingest_synthetic(**_ctx):
        from generate_synthetic_data import run_generate_synthetic

        os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
        return run_generate_synthetic()

    t_ingest_synthetic = PythonOperator(
        task_id="ingest_synthetic_sources",
        python_callable=ingest_synthetic,
        doc_md="Generate synthetic ERP/WMS/labor/CRM sources into local Bronze.",
    )

    def upload_bronze(**_ctx):
        from upload_bronze_to_s3 import upload_bronze_to_s3, verify_s3_upload

        os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
        upload_bronze_to_s3()
        verify_s3_upload()

    t_upload_bronze = PythonOperator(
        task_id="upload_bronze_to_s3",
        python_callable=upload_bronze,
        doc_md="Upload local Bronze files to S3 Bronze paths.",
    )

    def repair_partitions(**_ctx):
        import time

        import boto3

        athena = boto3.client("athena", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        results = os.getenv("ATHENA_RESULTS")
        workgroup = os.getenv("ATHENA_WORKGROUP", "primary")

        queries = [
            ("bronze_pos_transactions", "MSCK REPAIR TABLE coffee_chain.bronze_pos_transactions"),
            ("bronze_product_costs", "MSCK REPAIR TABLE coffee_chain.bronze_product_costs"),
            ("bronze_recipes_bom", "MSCK REPAIR TABLE coffee_chain.bronze_recipes_bom"),
            ("bronze_daily_inventory", "MSCK REPAIR TABLE coffee_chain.bronze_daily_inventory"),
            ("bronze_staff_shifts", "MSCK REPAIR TABLE coffee_chain.bronze_staff_shifts"),
        ]

        for table, query in queries:
            resp = athena.start_query_execution(
                QueryString=query,
                WorkGroup=workgroup,
                ResultConfiguration={"OutputLocation": results},
            )
            qid = resp["QueryExecutionId"]
            for _ in range(120):
                state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    print(f"OK repaired {table}")
                    break
                if state in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(f"MSCK REPAIR failed for {table}: {state}")
                time.sleep(1)
            else:
                raise TimeoutError(f"MSCK REPAIR timed out for {table}")

    t_repair_partitions = PythonOperator(
        task_id="repair_athena_partitions",
        python_callable=repair_partitions,
        doc_md="Register new Bronze partitions in Athena.",
    )

    def quality_gates_pre(**ctx):
        from run_quality_gates import run_all_gates

        return run_all_gates(blocks=["completeness", "integrity"], run_date=ctx["ds"])

    t_quality_pre = PythonOperator(
        task_id="quality_gates_pre_dbt",
        python_callable=quality_gates_pre,
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
        env=_env_common(),
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
        env=_env_common(),
        doc_md="Run dbt tests after model build.",
    )

    def quality_gates_post(**ctx):
        from run_quality_gates import run_all_gates

        return run_all_gates(blocks=["consistency", "parity"], run_date=ctx["ds"])

    t_quality_post = PythonOperator(
        task_id="quality_gates_post_dbt",
        python_callable=quality_gates_post,
        doc_md="Run post-dbt consistency and parity checks.",
    )

    def decide_pipeline_result(**_ctx):
        return "pipeline_success"

    t_branch = BranchPythonOperator(
        task_id="evaluate_pipeline_result",
        python_callable=decide_pipeline_result,
    )

    def log_success(**ctx):
        import boto3

        bucket = os.getenv("S3_BUCKET") or Variable.get("S3_BUCKET")
        s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        run_date = ctx["ds"]
        summary = {
            "run_date": run_date,
            "status": "SUCCESS",
            "completed": datetime.utcnow().isoformat(),
            "dag_run_id": ctx["run_id"],
        }
        s3.put_object(
            Bucket=bucket,
            Key=f"pipeline-runs/{run_date}/summary.json",
            Body=json.dumps(summary, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Pipeline SUCCESS for {run_date}")

    t_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=log_success,
        doc_md="Persist success summary to S3.",
    )

    def log_failure(**ctx):
        import boto3

        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            try:
                bucket = Variable.get("S3_BUCKET")
            except Exception:
                bucket = None
        if not bucket:
            print("S3_BUCKET not configured; failure summary not persisted")
            return

        s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        run_date = ctx["ds"]
        summary = {
            "run_date": run_date,
            "status": "FAILED",
            "failed_task": ctx["task_instance"].task_id,
            "completed": datetime.utcnow().isoformat(),
            "dag_run_id": ctx["run_id"],
            "action": "Check Airflow task logs",
        }
        s3.put_object(
            Bucket=bucket,
            Key=f"pipeline-runs/{run_date}/summary.json",
            Body=json.dumps(summary, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Pipeline FAILED at {ctx['task_instance'].task_id}")

    t_failure = PythonOperator(
        task_id="pipeline_failure",
        python_callable=log_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="Persist failure summary to S3 when any upstream fails.",
    )

    t_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [t_ingest_pos, t_ingest_synthetic] >> t_upload_bronze
    t_upload_bronze >> t_repair_partitions >> t_quality_pre >> t_dbt_run >> t_dbt_test >> t_quality_post >> t_branch
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
        t_success,
    ] >> t_failure
    t_failure >> t_end
