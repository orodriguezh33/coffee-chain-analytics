"""Generate 5 synthetic datasets aligned to the coffee POS dataset."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Configuration
SEED = 42
np.random.seed(SEED)
BRONZE_PATH = Path(os.getenv("DATA_BRONZE", "data/bronze"))
RUN_DATE = datetime.today().strftime("%Y-%m-%d")

DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2023, 6, 30)
DATES = pd.date_range(DATE_START, DATE_END, freq="D")

STORES = {5: "Lower Manhattan", 3: "Astoria", 8: "Hell's Kitchen"}

PRODUCTS = {
    "Drip coffee": {"category": "Coffee", "cogs_pct": 0.22},
    "Gourmet brewed coffee": {"category": "Coffee", "cogs_pct": 0.22},
    "Barista Espresso": {"category": "Coffee", "cogs_pct": 0.20},
    "Latte": {"category": "Coffee", "cogs_pct": 0.32},
    "Cappuccino": {"category": "Coffee", "cogs_pct": 0.30},
    "Cortado": {"category": "Coffee", "cogs_pct": 0.28},
    "Americano": {"category": "Coffee", "cogs_pct": 0.18},
    "Espresso shot": {"category": "Coffee", "cogs_pct": 0.18},
    "Brewed Chai tea": {"category": "Tea", "cogs_pct": 0.28},
    "Brewed Black tea": {"category": "Tea", "cogs_pct": 0.25},
    "Brewed herbal tea": {"category": "Tea", "cogs_pct": 0.30},
    "Green tea": {"category": "Tea", "cogs_pct": 0.26},
    "Morning Sunrise Chai": {"category": "Tea", "cogs_pct": 0.29},
    "Traditional Blend Chai": {"category": "Tea", "cogs_pct": 0.29},
    "Spicy Eye Opener Chai": {"category": "Tea", "cogs_pct": 0.30},
    "Serenity Green Tea": {"category": "Tea", "cogs_pct": 0.26},
    "English Breakfast": {"category": "Tea", "cogs_pct": 0.25},
    "Lemon Grass": {"category": "Tea", "cogs_pct": 0.27},
    "Peppermint": {"category": "Tea", "cogs_pct": 0.26},
    "Croissant": {"category": "Bakery", "cogs_pct": 0.45},
    "Almond Croissant": {"category": "Bakery", "cogs_pct": 0.47},
    "Scone": {"category": "Bakery", "cogs_pct": 0.42},
    "Ginger Scone": {"category": "Bakery", "cogs_pct": 0.43},
    "Cranberry Scone": {"category": "Bakery", "cogs_pct": 0.44},
    "Jumbo Savory Scone": {"category": "Bakery", "cogs_pct": 0.46},
    "Scottish Cream Scone": {"category": "Bakery", "cogs_pct": 0.43},
    "Oatmeal Scone": {"category": "Bakery", "cogs_pct": 0.44},
    "Muffin": {"category": "Bakery", "cogs_pct": 0.48},
    "Hazelnut Biscotti": {"category": "Bakery", "cogs_pct": 0.41},
    "Chocolate Chip Biscotti": {"category": "Bakery", "cogs_pct": 0.43},
    "Ginger Biscotti": {"category": "Bakery", "cogs_pct": 0.41},
    "Hot chocolate": {"category": "Drinking Chocolate", "cogs_pct": 0.35},
    "Dark chocolate": {"category": "Drinking Chocolate", "cogs_pct": 0.36},
    "Sugar Free Vanilla syrup": {"category": "Flavours", "cogs_pct": 0.14},
    "Chocolate syrup": {"category": "Flavours", "cogs_pct": 0.16},
    "Carmel syrup": {"category": "Flavours", "cogs_pct": 0.14},
    "Hazelnut syrup": {"category": "Flavours", "cogs_pct": 0.14},
    "Our Old Time Diner Blend": {"category": "Coffee", "cogs_pct": 0.22},
    "Columbian Medium Roast": {"category": "Coffee", "cogs_pct": 0.22},
    "Sustainably Grown Organic": {"category": "Coffee", "cogs_pct": 0.24},
    "Jamaican Coffee River": {"category": "Coffee", "cogs_pct": 0.24},
    "Brazilian": {"category": "Coffee", "cogs_pct": 0.23},
    "Ethiopia": {"category": "Coffee", "cogs_pct": 0.23},
    "Brazilian - Organic": {"category": "Coffee", "cogs_pct": 0.25},
    "Organic Decaf Blend": {"category": "Coffee", "cogs_pct": 0.23},
    "Ouro Brasileiro shot": {"category": "Coffee", "cogs_pct": 0.19},
    "Civet Cat": {"category": "Coffee beans", "cogs_pct": 0.26},
    "Espresso Roast": {"category": "Coffee beans", "cogs_pct": 0.24},
    "Primo Espresso Roast": {"category": "Coffee beans", "cogs_pct": 0.24},
    "Guatemalan Sustainably Grown": {"category": "Coffee beans", "cogs_pct": 0.25},
    "Chili Mayan": {"category": "Packaged Chocolate", "cogs_pct": 0.28},
    "Earl Grey": {"category": "Loose Tea", "cogs_pct": 0.25},
}

INGREDIENTS = {
    "espresso_shot_ml": {"unit": "ml", "cost": 0.008},
    "whole_milk_ml": {"unit": "ml", "cost": 0.002},
    "oat_milk_ml": {"unit": "ml", "cost": 0.004},
    "coffee_grounds_g": {"unit": "g", "cost": 0.015},
    "tea_leaves_g": {"unit": "g", "cost": 0.010},
    "chocolate_syrup_ml": {"unit": "ml", "cost": 0.006},
    "flavor_syrup_ml": {"unit": "ml", "cost": 0.005},
    "sugar_g": {"unit": "g", "cost": 0.001},
    "flour_g": {"unit": "g", "cost": 0.002},
    "butter_g": {"unit": "g", "cost": 0.005},
}

RECIPES = {
    "Latte": [("espresso_shot_ml", 60), ("whole_milk_ml", 180)],
    "Cappuccino": [("espresso_shot_ml", 60), ("whole_milk_ml", 120)],
    "Cortado": [("espresso_shot_ml", 60), ("whole_milk_ml", 60)],
    "Americano": [("espresso_shot_ml", 60)],
    "Espresso shot": [("espresso_shot_ml", 30)],
    "Barista Espresso": [("espresso_shot_ml", 30)],
    "Drip coffee": [("coffee_grounds_g", 18)],
    "Gourmet brewed coffee": [("coffee_grounds_g", 18)],
    "Brewed Chai tea": [("tea_leaves_g", 5), ("whole_milk_ml", 120)],
    "Brewed Black tea": [("tea_leaves_g", 3)],
    "Brewed herbal tea": [("tea_leaves_g", 4)],
    "Green tea": [("tea_leaves_g", 3)],
    "Morning Sunrise Chai": [("tea_leaves_g", 5), ("whole_milk_ml", 120)],
    "Traditional Blend Chai": [("tea_leaves_g", 5), ("whole_milk_ml", 120)],
    "Spicy Eye Opener Chai": [("tea_leaves_g", 5), ("whole_milk_ml", 120)],
    "Serenity Green Tea": [("tea_leaves_g", 3)],
    "English Breakfast": [("tea_leaves_g", 3)],
    "Lemon Grass": [("tea_leaves_g", 4)],
    "Peppermint": [("tea_leaves_g", 4)],
    "Hot chocolate": [("chocolate_syrup_ml", 45), ("whole_milk_ml", 180)],
    "Dark chocolate": [("chocolate_syrup_ml", 45), ("whole_milk_ml", 180)],
    "Sugar Free Vanilla syrup": [("flavor_syrup_ml", 30)],
    "Chocolate syrup": [("chocolate_syrup_ml", 30)],
    "Carmel syrup": [("flavor_syrup_ml", 30)],
    "Hazelnut syrup": [("flavor_syrup_ml", 30)],
    "Our Old Time Diner Blend": [("coffee_grounds_g", 18)],
    "Columbian Medium Roast": [("coffee_grounds_g", 18)],
    "Sustainably Grown Organic": [("coffee_grounds_g", 18)],
    "Jamaican Coffee River": [("coffee_grounds_g", 18)],
    "Brazilian": [("coffee_grounds_g", 18)],
    "Ethiopia": [("coffee_grounds_g", 18)],
    "Brazilian - Organic": [("coffee_grounds_g", 18)],
    "Organic Decaf Blend": [("coffee_grounds_g", 18)],
    "Ouro Brasileiro shot": [("espresso_shot_ml", 30)],
    "Croissant": [("flour_g", 80), ("butter_g", 30)],
    "Almond Croissant": [("flour_g", 85), ("butter_g", 35), ("sugar_g", 10)],
    "Scone": [("flour_g", 60), ("butter_g", 20), ("sugar_g", 15)],
    "Ginger Scone": [("flour_g", 60), ("butter_g", 20), ("sugar_g", 18)],
    "Cranberry Scone": [("flour_g", 62), ("butter_g", 20), ("sugar_g", 18)],
    "Jumbo Savory Scone": [("flour_g", 85), ("butter_g", 25), ("sugar_g", 8)],
    "Scottish Cream Scone": [("flour_g", 65), ("butter_g", 22), ("sugar_g", 15)],
    "Oatmeal Scone": [("flour_g", 60), ("butter_g", 20), ("sugar_g", 15)],
    "Muffin": [("flour_g", 70), ("butter_g", 25), ("sugar_g", 20)],
    "Hazelnut Biscotti": [("flour_g", 45), ("butter_g", 10), ("sugar_g", 12)],
    "Chocolate Chip Biscotti": [
        ("flour_g", 45),
        ("butter_g", 10),
        ("sugar_g", 12),
        ("chocolate_syrup_ml", 10),
    ],
    "Ginger Biscotti": [("flour_g", 45), ("butter_g", 10), ("sugar_g", 12)],
}


def generate_product_costs() -> pd.DataFrame:
    """Simulate ERP product costs with yearly validity period."""
    print("  Generating product_costs...")
    rows = []
    for product_name, attrs in PRODUCTS.items():
        unit_price = round(np.random.uniform(3.0, 8.5), 2)
        unit_cost = round(
            unit_price * attrs["cogs_pct"] * np.random.uniform(0.92, 1.08), 2
        )
        rows.append(
            {
                "product_name": product_name,
                "product_category": attrs["category"],
                "unit_price_std": unit_price,
                "unit_cost": unit_cost,
                "cogs_pct": round(unit_cost / unit_price, 4),
                "valid_from": "2023-01-01",
                "valid_to": "2023-12-31",
                "source_system": "ERP_SAP",
            }
        )
    df = pd.DataFrame(rows)
    print(f"  ✓ {len(df)} products with costs")
    return df


def generate_recipes_bom() -> pd.DataFrame:
    """Generate bill of materials by product."""
    print("  Generating recipes_bom...")
    rows = []
    for product_name, ingredients in RECIPES.items():
        for ing_name, qty in ingredients:
            rows.append(
                {
                    "product_name": product_name,
                    "ingredient_name": ing_name,
                    "qty_required_per_unit": qty,
                    "unit_of_measure": INGREDIENTS[ing_name]["unit"],
                    "ingredient_cost_per_unit": INGREDIENTS[ing_name]["cost"],
                    "last_updated": "2023-01-01",
                }
            )
    df = pd.DataFrame(rows)
    print(f"  ✓ {len(df)} recipe rows ({len(RECIPES)} products)")
    return df


def generate_daily_inventory() -> pd.DataFrame:
    """Generate daily store inventory snapshots (WMS-like)."""
    print("  Generating daily_inventory...")
    rows = []

    for store_id, store_name in STORES.items():
        stock = {ing: round(np.random.uniform(600, 1400), 1) for ing in INGREDIENTS}

        for date in DATES:
            is_weekend = date.weekday() >= 5
            is_monday = date.weekday() == 0

            for ing_name in INGREDIENTS:
                opening = stock[ing_name]
                base = np.random.uniform(40, 120)
                dow_factor = 1.35 if is_weekend else 1.0
                waste_factor = np.random.uniform(1.05, 1.15)
                consumed = round(base * dow_factor * waste_factor, 1)

                received = 0.0
                if is_monday and opening < 350:
                    received = round(np.random.uniform(600, 900), 1)

                closing = max(0.0, round(opening + received - consumed, 1))
                stock[ing_name] = closing

                rows.append(
                    {
                        "inventory_date": date.strftime("%Y-%m-%d"),
                        "store_id": store_id,
                        "store_name": store_name,
                        "ingredient_name": ing_name,
                        "opening_stock": opening,
                        "units_received": received,
                        "actual_consumed": consumed,
                        "closing_stock": closing,
                    }
                )

    df = pd.DataFrame(rows)
    print(
        f"  ✓ {len(df):,} inventory rows "
        f"({len(STORES)} stores × {len(DATES)} days × {len(INGREDIENTS)} ingredients)"
    )
    return df


def generate_staff_shifts() -> pd.DataFrame:
    """Generate staffing shifts and labor cost by store/day."""
    print("  Generating staff_shifts...")
    rows = []

    shifts = {
        "morning": {"h": 4, "start": "06:00", "end": "10:00", "emp": 3, "rate": 15.0},
        "midday": {"h": 4, "start": "10:00", "end": "14:00", "emp": 4, "rate": 15.0},
        "afternoon": {
            "h": 4,
            "start": "14:00",
            "end": "18:00",
            "emp": 2,
            "rate": 15.5,
        },
        "closing": {"h": 3, "start": "18:00", "end": "21:00", "emp": 2, "rate": 15.5},
    }

    for store_id, store_name in STORES.items():
        for date in DATES:
            is_weekend = date.weekday() >= 5
            for shift_name, s in shifts.items():
                emp = s["emp"]
                if is_weekend and shift_name in ["morning", "midday"]:
                    emp += 1
                labor = round(
                    emp * s["h"] * s["rate"] * np.random.uniform(0.98, 1.05), 2
                )
                rows.append(
                    {
                        "shift_date": date.strftime("%Y-%m-%d"),
                        "store_id": store_id,
                        "store_name": store_name,
                        "shift_type": shift_name,
                        "shift_start": s["start"],
                        "shift_end": s["end"],
                        "employees_on_shift": emp,
                        "hours_per_employee": s["h"],
                        "total_hours": emp * s["h"],
                        "hourly_rate_usd": s["rate"],
                        "labor_cost_usd": labor,
                    }
                )

    df = pd.DataFrame(rows)
    print(f"  ✓ {len(df):,} shift rows")
    return df


def generate_promotions() -> pd.DataFrame:
    """Generate CRM/loyalty promotions dataset."""
    print("  Generating promotions...")
    promos = [
        {
            "promotion_id": "PROMO_001",
            "promotion_name": "Happy Hour Coffee",
            "discount_type": "percentage",
            "discount_value": 0.20,
            "applicable_category": "Coffee",
            "start_date": "2023-02-01",
            "end_date": "2023-02-28",
            "applicable_days": "Mon,Tue,Wed,Thu,Fri",
            "applicable_hours": "14:00-17:00",
            "source_system": "CRM_LOYALTY",
        },
        {
            "promotion_id": "PROMO_002",
            "promotion_name": "Weekend Bakery Special",
            "discount_type": "percentage",
            "discount_value": 0.15,
            "applicable_category": "Bakery",
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "applicable_days": "Sat,Sun",
            "applicable_hours": "08:00-12:00",
            "source_system": "CRM_LOYALTY",
        },
        {
            "promotion_id": "PROMO_003",
            "promotion_name": "Tea Tuesday",
            "discount_type": "fixed",
            "discount_value": 1.00,
            "applicable_category": "Tea",
            "start_date": "2023-04-01",
            "end_date": "2023-06-30",
            "applicable_days": "Tue",
            "applicable_hours": "all_day",
            "source_system": "CRM_LOYALTY",
        },
    ]
    df = pd.DataFrame(promos)
    print(f"  ✓ {len(df)} promotions")
    return df


def save_synthetic_to_bronze(df: pd.DataFrame, dataset_name: str, run_date: str) -> str:
    """Save synthetic dataset under Bronze synthetic partitioned layout."""
    output_dir = BRONZE_PATH / "synthetic" / dataset_name / f"ingestion_date={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{dataset_name}_{run_date}.csv"
    df.to_csv(output_path, index=False)

    size_kb = output_path.stat().st_size / 1024
    print(f"  → Saved {output_path.name} ({size_kb:.1f} KB)")
    return str(output_path)


def run_generate_synthetic() -> dict:
    """Generate all synthetic datasets and persist to Bronze."""
    print("\n" + "=" * 50)
    print("SYNTHETIC DATA GENERATION -> Bronze")
    print("=" * 50)

    datasets = {
        "product_costs": generate_product_costs,
        "recipes_bom": generate_recipes_bom,
        "daily_inventory": generate_daily_inventory,
        "staff_shifts": generate_staff_shifts,
        "promotions": generate_promotions,
    }

    results = {}
    for name, generator_fn in datasets.items():
        df = generator_fn()
        path = save_synthetic_to_bronze(df, name, RUN_DATE)
        results[name] = {"rows": len(df), "path": path}

    print("\n✓ All synthetic datasets generated")
    print("\nSummary:")
    for name, info in results.items():
        print(f"  {name:<20} {info['rows']:>8,} rows -> {info['path']}")

    return results


if __name__ == "__main__":
    run_generate_synthetic()
