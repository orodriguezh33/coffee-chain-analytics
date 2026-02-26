"""Process raw POS CSV into local Bronze layer."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

# Configuration
RAW_PATH = Path(os.getenv("DATA_RAW", "data/raw"))
BRONZE_PATH = Path(os.getenv("DATA_BRONZE", "data/bronze"))
RUN_DATE = datetime.today().strftime("%Y-%m-%d")


def load_raw_pos(filepath: Path) -> pd.DataFrame:
    """
    Load the raw POS CSV with conservative typing.
    Bronze keeps data close to source; typing cleanup happens later.
    """
    print(f"  Loading raw POS from {filepath}...")

    df = pd.read_csv(
        filepath,
        dtype={
            "transaction_id": str,
            "store_id": str,
            "product_id": str,
            "transaction_qty": str,
            "unit_price": str,
        },
        encoding="utf-8",
        skipinitialspace=True,
    )

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
    )

    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


def validate_raw_pos(df: pd.DataFrame) -> dict:
    """Basic schema/quality checks before writing Bronze."""
    issues: list[str] = []

    expected_cols = {
        "transaction_id",
        "transaction_date",
        "transaction_time",
        "store_id",
        "store_location",
        "product_id",
        "transaction_qty",
        "unit_price",
        "product_category",
        "product_type",
        "product_detail",
    }

    missing = expected_cols - set(df.columns)
    if missing:
        issues.append(f"Missing columns: {sorted(missing)}")

    if len(df) < 100:
        issues.append(f"Too few rows: {len(df)}")

    if "transaction_id" in df.columns and df["transaction_id"].isna().sum() > 0:
        issues.append("Null transaction_ids found")

    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "rows": len(df),
        "cols": list(df.columns),
    }


def save_to_bronze(df: pd.DataFrame, run_date: str) -> Path:
    """
    Save POS CSV using Bronze partition pattern:
    data/bronze/pos/transactions/ingestion_date=YYYY-MM-DD/
    """
    output_dir = BRONZE_PATH / "pos" / "transactions" / f"ingestion_date={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["ingestion_date"] = run_date
    df["ingestion_timestamp"] = datetime.now().isoformat()
    df["source_file"] = "coffee_shop_sales.csv"

    output_path = output_dir / f"coffee_shop_sales_{run_date}.csv"
    df.to_csv(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Saved to {output_path} ({size_mb:.2f} MB)")
    return output_path


def run_ingest_pos() -> str:
    """Run the POS-to-Bronze ingestion process."""
    print("\n" + "=" * 50)
    print("INGESTION: POS Transactions -> Bronze")
    print("=" * 50)

    csv_files = sorted(RAW_PATH.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {RAW_PATH}. "
            "Please copy coffee_shop_sales.csv to data/raw/"
        )

    csv_path = csv_files[0]
    print(f"  Found: {csv_path.name}")

    df = load_raw_pos(csv_path)

    print("  Validating...")
    result = validate_raw_pos(df)
    if not result["valid"]:
        raise ValueError(f"Validation failed: {result['issues']}")
    print(f"  ✓ Validation passed ({result['rows']:,} rows)")
    print(f"  ✓ Columns: {result['cols']}")

    print(f"  Saving to Bronze (date: {RUN_DATE})...")
    output_path = save_to_bronze(df, RUN_DATE)

    print("\n✓ POS ingestion complete")
    print(f"  Output: {output_path}")
    print(f"  Rows:   {len(df):,}")
    return str(output_path)


if __name__ == "__main__":
    run_ingest_pos()
