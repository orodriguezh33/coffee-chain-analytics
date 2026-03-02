"""Wrappers de tareas para que Airflow solo orqueste."""

from __future__ import annotations

import os

def ingest_pos_task(**context):
    """Ejecuta la ingesta POS para la fecha del DAG."""
    from ingestion.ingest_pos import run_ingest_pos

    os.environ["DATA_RAW"] = "/opt/airflow/data/raw"
    os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
    os.environ["RUN_DATE"] = context["ds"]
    return run_ingest_pos(run_date=context["ds"])


def ingest_synthetic_task(**context):
    """Genera datasets sintéticos para la fecha del DAG."""
    from ingestion.generate_synthetic_data import run_generate_synthetic

    os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
    os.environ["RUN_DATE"] = context["ds"]
    return run_generate_synthetic(run_date=context["ds"])


def upload_bronze_task(**context):
    """Sube la partición Bronze del día y valida prefijos en S3."""
    from ingestion.upload_bronze_to_s3 import upload_bronze_to_s3, verify_s3_upload

    os.environ["DATA_BRONZE"] = "/opt/airflow/data/bronze"
    os.environ["RUN_DATE"] = context["ds"]
    upload_bronze_to_s3(run_date=context["ds"], full_refresh=False)
    verify_s3_upload()


def repair_partitions_task(**_context):
    """Registra particiones nuevas en Athena."""
    from scripts.athena.repair_partitions import repair_bronze_partitions

    repair_bronze_partitions()


def quality_gates_pre_task(**context):
    """Corre validaciones pre-dbt."""
    from scripts.quality.run_quality_gates import run_all_gates

    return run_all_gates(blocks=["completeness", "integrity"], run_date=context["ds"])


def quality_gates_post_task(**context):
    """Corre validaciones post-dbt."""
    from scripts.quality.run_quality_gates import run_all_gates

    return run_all_gates(blocks=["consistency", "parity"], run_date=context["ds"])


def quality_gates_backfill_task(**context):
    """Corre quality gates al final del backfill si el parámetro está activo."""
    from scripts.quality.run_quality_gates import run_all_gates

    if not context["params"].get("run_quality_gates", True):
        print("Quality gates omitidos por parámetro")
        return None
    return run_all_gates(blocks=["all"])


def export_bi_snapshot_task(**_context):
    """Genera snapshot CSV para Power BI."""
    from scripts.bi.export_powerbi_snapshot import main as export_bi_snapshot_main

    os.environ.setdefault("BI_EXPORT_LOCAL_DIR", "/opt/airflow/exports/bi_snapshot")
    export_bi_snapshot_main()


def pipeline_success_task(**context):
    """Escribe auditoría de éxito en S3."""
    from scripts.pipeline.write_run_summary import write_run_summary

    write_run_summary(run_date=context["ds"], run_id=context["run_id"], status="SUCCESS")
    print(f"Pipeline SUCCESS for {context['ds']}")


def pipeline_failure_task(**context):
    """Escribe auditoría de fallo en S3."""
    from scripts.pipeline.write_run_summary import write_run_summary

    dag_run = context["dag_run"]
    failed_task_ids = sorted(
        {
            task_instance.task_id
            for task_instance in dag_run.get_task_instances()
            if task_instance.state in {"failed", "upstream_failed"}
            and task_instance.task_id != context["task_instance"].task_id
        }
    )
    write_run_summary(
        run_date=context["ds"],
        run_id=context["run_id"],
        status="FAILED",
        failed_tasks=failed_task_ids,
    )
    print(f"Pipeline FALLÓ. Tareas upstream con error: {failed_task_ids or ['desconocida']}")
