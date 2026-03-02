"""Registra particiones Bronze en Athena."""

from __future__ import annotations

from scripts.shared.athena_runner import run_query


TABLES = [
    "bronze_pos_transactions",
    "bronze_product_costs",
    "bronze_recipes_bom",
    "bronze_daily_inventory",
    "bronze_staff_shifts",
]


def repair_bronze_partitions() -> None:
    """Ejecuta MSCK REPAIR sobre las tablas particionadas de Bronze."""
    for table_name in TABLES:
        run_query(
            f"MSCK REPAIR TABLE coffee_chain.{table_name}",
            description=f"repair {table_name}",
        )
        print(f"OK repaired {table_name}")


if __name__ == "__main__":
    repair_bronze_partitions()
