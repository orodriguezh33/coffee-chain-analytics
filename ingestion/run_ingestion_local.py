"""Orquestador local para el flujo de ingesta M1."""

from ingestion.generate_synthetic_data import run_generate_synthetic
from ingestion.ingest_pos import run_ingest_pos


if __name__ == "__main__":
    print("\nIniciando pipeline local de ingesta...")

    run_ingest_pos()
    run_generate_synthetic()

    print("\n" + "=" * 50)
    print("✓ Ingesta local completada")
    print("  Archivos listos en data/bronze/")
    print("=" * 50)
