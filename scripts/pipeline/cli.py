"""CLI principal del proyecto.

Este módulo expone un punto de entrada único y delega la ejecución en los
módulos del proyecto sin duplicar lógica de negocio.
"""

from __future__ import annotations

import argparse
import os


def _set_run_date(run_date: str | None) -> None:
    """Expone la fecha de corrida a los módulos que la usan por entorno."""
    if run_date:
        os.environ["RUN_DATE"] = run_date


def cmd_ingest_local(args: argparse.Namespace) -> None:
    """Ejecuta POS y sintéticos en Bronze local."""
    from ingestion.generate_synthetic_data import run_generate_synthetic
    from ingestion.ingest_pos import run_ingest_pos

    _set_run_date(args.run_date)
    run_ingest_pos(run_date=args.run_date)
    run_generate_synthetic(run_date=args.run_date)


def cmd_upload_bronze(args: argparse.Namespace) -> None:
    """Sube Bronze local a S3 y valida prefijos."""
    from ingestion.upload_bronze_to_s3 import upload_bronze_to_s3, verify_s3_upload

    _set_run_date(args.run_date)
    upload_bronze_to_s3(run_date=args.run_date, full_refresh=args.all)
    if not verify_s3_upload():
        raise SystemExit(1)


def cmd_athena_ddl(_args: argparse.Namespace) -> None:
    """Crea tablas Bronze en Athena y valida contenido."""
    from scripts.athena.repair_partitions import repair_bronze_partitions
    from scripts.athena.run_ddl import create_bronze_tables, verify_tables

    create_bronze_tables()
    repair_bronze_partitions()
    verify_tables()


def cmd_quality_gates(args: argparse.Namespace) -> None:
    """Ejecuta quality gates por bloque o todos."""
    from scripts.quality.run_quality_gates import run_all_gates

    run_all_gates(blocks=args.blocks or None, run_date=args.run_date)


def cmd_export_bi(_args: argparse.Namespace) -> None:
    """Exporta el snapshot BI a CSV."""
    from scripts.bi.export_powerbi_snapshot import main as export_powerbi_snapshot

    export_powerbi_snapshot()


def _add_ingest_commands(subparsers) -> None:
    ingest_local = subparsers.add_parser(
        "ingest-local",
        help="Genera los archivos Bronze locales.",
    )
    ingest_local.add_argument("--run-date", help="Fecha de corrida en formato YYYY-MM-DD.")
    ingest_local.set_defaults(func=cmd_ingest_local)


def _add_athena_commands(subparsers) -> None:
    upload = subparsers.add_parser(
        "upload-bronze",
        help="Sube Bronze local a S3 y valida prefijos.",
    )
    upload.add_argument("--run-date", help="Fecha de corrida en formato YYYY-MM-DD.")
    upload.add_argument(
        "--all",
        action="store_true",
        help="Sube todas las particiones de Bronze en vez de solo la fecha indicada.",
    )
    upload.set_defaults(func=cmd_upload_bronze)

    athena_ddl = subparsers.add_parser(
        "athena-ddl",
        help="Prepara Athena para consultar Bronze.",
    )
    athena_ddl.set_defaults(func=cmd_athena_ddl)


def _add_quality_and_bi_commands(subparsers) -> None:
    quality = subparsers.add_parser(
        "quality-gates",
        help="Ejecuta quality gates por bloque o todos.",
    )
    quality.add_argument(
        "blocks",
        nargs="*",
        help="Bloques a ejecutar: completeness, integrity, consistency, parity, all.",
    )
    quality.add_argument("--run-date", help="Fecha lógica del run para auditoría.")
    quality.set_defaults(func=cmd_quality_gates)

    export_bi = subparsers.add_parser(
        "export-bi",
        help="Exporta el snapshot BI a CSV desde Athena.",
    )
    export_bi.set_defaults(func=cmd_export_bi)


def build_parser() -> argparse.ArgumentParser:
    """Construye el parser principal."""
    parser = argparse.ArgumentParser(
        description="Entry point único del proyecto Coffee Chain Analytics.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_ingest_commands(subparsers)
    _add_athena_commands(subparsers)
    _add_quality_and_bi_commands(subparsers)

    return parser


def main() -> None:
    """Punto de entrada principal."""
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
