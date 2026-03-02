"""Procesa el CSV crudo del POS hacia la capa Bronze local."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Configuration
RAW_PATH = Path(os.getenv("DATA_RAW", "data/raw"))
BRONZE_PATH = Path(os.getenv("DATA_BRONZE", "data/bronze"))


def resolve_run_date(run_date: str | None = None) -> str:
    """Resuelve la fecha de ejecución de forma determinística."""
    return run_date or os.getenv("RUN_DATE") or datetime.today().strftime("%Y-%m-%d")


def load_raw_pos(filepath: Path) -> pd.DataFrame:
    """Carga el CSV del POS con tipado conservador."""
    print(f"  Cargando POS crudo desde {filepath}...")

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

    print(f"  Cargadas {len(df):,} filas, {len(df.columns)} columnas")
    return df


def validate_raw_pos(df: pd.DataFrame) -> dict:
    """Valida esquema y calidad básica antes de escribir Bronze."""
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
        issues.append(f"Columnas faltantes: {sorted(missing)}")

    if len(df) < 100:
        issues.append(f"Muy pocas filas: {len(df)}")

    if "transaction_id" in df.columns and df["transaction_id"].isna().sum() > 0:
        issues.append("Se encontraron transaction_id nulos")

    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "rows": len(df),
        "cols": list(df.columns),
    }


def save_to_bronze(df: pd.DataFrame, run_date: str, source_file: str) -> Path:
    """Guarda el CSV del POS en Bronze con partición por fecha de ingesta."""
    output_dir = BRONZE_PATH / "pos" / "transactions" / f"ingestion_date={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["ingestion_date"] = run_date
    df["ingestion_timestamp"] = datetime.now().isoformat()
    df["source_file"] = source_file

    output_path = output_dir / f"coffee_shop_sales_{run_date}.csv"
    df.to_csv(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Guardado en {output_path} ({size_mb:.2f} MB)")
    return output_path


def run_ingest_pos(run_date: str | None = None) -> str:
    """Ejecuta la ingesta POS hacia Bronze."""
    resolved_run_date = resolve_run_date(run_date)

    print("\n" + "=" * 50)
    print("INGESTA: Transacciones POS -> Bronze")
    print("=" * 50)

    preferred_csv = RAW_PATH / "coffee_shop_sales.csv"
    if preferred_csv.exists():
        csv_path = preferred_csv
    else:
        csv_files = sorted(RAW_PATH.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No se encontraron CSV en {RAW_PATH}. "
                "Copia coffee_shop_sales.csv en data/raw/"
            )
        if len(csv_files) > 1:
            raise FileExistsError(
                "Se encontraron múltiples CSV en data/raw y "
                "falta coffee_shop_sales.csv. "
                f"Deja solo un CSV o renombra el archivo objetivo. Encontrados: "
                f"{[p.name for p in csv_files]}"
            )
        csv_path = csv_files[0]

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV no encontrado: {csv_path}. "
            "Copia coffee_shop_sales.csv en data/raw/"
        )
    print(f"  Archivo encontrado: {csv_path.name}")

    df = load_raw_pos(csv_path)

    print("  Validando...")
    result = validate_raw_pos(df)
    if not result["valid"]:
        raise ValueError(f"Validación fallida: {result['issues']}")
    print(f"  ✓ Validación aprobada ({result['rows']:,} filas)")
    print(f"  ✓ Columnas: {result['cols']}")

    print(f"  Guardando en Bronze (fecha: {resolved_run_date})...")
    output_path = save_to_bronze(df, resolved_run_date, csv_path.name)

    print("\n✓ Ingesta POS completada")
    print(f"  Salida: {output_path}")
    print(f"  Filas:  {len(df):,}")
    return str(output_path)


if __name__ == "__main__":
    run_ingest_pos(run_date=sys.argv[1] if len(sys.argv) > 1 else None)
