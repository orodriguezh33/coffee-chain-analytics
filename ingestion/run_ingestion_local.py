"""Local orchestrator for M1 ingestion flow."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from generate_synthetic_data import run_generate_synthetic
from ingest_pos import run_ingest_pos


if __name__ == "__main__":
    print("\nStarting local ingestion pipeline...")

    run_ingest_pos()
    run_generate_synthetic()

    print("\n" + "=" * 50)
    print("✓ Local ingestion complete")
    print("  All files ready in data/bronze/")
    print("=" * 50)
