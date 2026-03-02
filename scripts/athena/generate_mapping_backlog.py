"""Generate a product-name mapping backlog from Athena into a local CSV.

Outputs a review file to dbt/seeds/product_name_mapping_backlog.csv with:
- exact POS product name
- suggested canonical name (size suffix removed)
- suggested size code
- suggested cost/recipe flags by category
- whether the suggested canonical name exists in synthetic cost/recipe catalogs
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

from scripts.shared.athena_runner import iter_result_rows, run_query

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "coffee_chain")
ATHENA_RESULTS = os.getenv("ATHENA_RESULTS")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
OUTPUT_PATH = Path(
    os.getenv("MAPPING_BACKLOG_OUTPUT", "/app/dbt/seeds/product_name_mapping_backlog.csv")
)


QUERY = """
with pos_products as (
    select
        trim(product_detail) as pos_product_detail,
        trim(product_category) as product_category,
        count(*) as transactions
    from coffee_chain.bronze_pos_transactions
    where ingestion_date is not null
    group by 1, 2
),
mapping as (
    select distinct trim(pos_product_detail) as pos_product_detail
    from silver.product_name_mapping
),
backlog as (
    select
        p.pos_product_detail,
        p.product_category,
        p.transactions,
        trim(regexp_replace(p.pos_product_detail, ' +(Sm|Rg|Lg)$', '')) as suggested_canonical_product_name,
        regexp_extract(p.pos_product_detail, ' +(Sm|Rg|Lg)$', 1) as suggested_size_code,
        case
            when p.product_category in ('Branded', 'Coffee beans', 'Packaged Chocolate', 'Loose Tea')
                then 'false'
            else 'true'
        end as suggested_cost_required,
        case
            when p.product_category in ('Branded', 'Coffee beans', 'Packaged Chocolate', 'Loose Tea')
                then 'false'
            else 'true'
        end as suggested_recipe_required,
        case when m.pos_product_detail is not null then 'true' else 'false' end as has_mapping
    from pos_products p
    left join mapping m
      on p.pos_product_detail = m.pos_product_detail
)
select
    b.pos_product_detail,
    b.product_category,
    cast(b.transactions as varchar) as transactions,
    b.suggested_canonical_product_name,
    nullif(b.suggested_size_code, '') as suggested_size_code,
    b.suggested_cost_required,
    b.suggested_recipe_required,
    b.has_mapping,
    case when c.product_name is not null then 'true' else 'false' end as has_cost_for_suggested,
    case when r.product_name is not null then 'true' else 'false' end as has_recipe_for_suggested,
    case
        when b.has_mapping = 'true' then 'Already mapped'
        when c.product_name is not null and r.product_name is not null then 'Add mapping only'
        when c.product_name is null and r.product_name is not null then 'Add mapping + cost'
        when c.product_name is not null and r.product_name is null then 'Add mapping + recipe'
        else 'Add mapping + cost + recipe'
    end as suggested_action
from backlog b
left join coffee_chain.bronze_product_costs c
  on b.suggested_canonical_product_name = c.product_name
left join (
    select distinct product_name
    from coffee_chain.bronze_recipes_bom
) r
  on b.suggested_canonical_product_name = r.product_name
where b.has_mapping = 'false'
order by b.transactions desc, b.pos_product_detail
"""

def fetch_all_rows(qid: str) -> tuple[list[str], list[list[str]]]:
    rows = list(iter_result_rows(qid, include_header=True))
    if not rows:
        return [], []
    headers = rows[0]
    data_rows: list[list[str]] = []
    for row in rows[1:]:
        padded = row + [""] * max(0, len(headers) - len(row))
        data_rows.append(padded[: len(headers)])
    return headers, data_rows


def write_csv(headers: list[str], rows: list[list[str]]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def summarize(rows: list[list[str]], headers: list[str]) -> None:
    idx = {name: i for i, name in enumerate(headers)}
    action_i = idx.get("suggested_action")
    if action_i is None:
        return
    counts: dict[str, int] = {}
    for row in rows:
        action = row[action_i]
        counts[action] = counts.get(action, 0) + 1
    print("\nSummary by suggested_action:")
    for action, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {action:<28} {n:>5}")


def main() -> int:
    print("=" * 60)
    print("GENERATE PRODUCT MAPPING BACKLOG (ATHENA -> CSV)")
    print("=" * 60)
    print(f"Region: {REGION}")
    print(f"Athena DB: {ATHENA_DB}")
    print(f"Workgroup: {ATHENA_WORKGROUP}")
    print(f"Output CSV: {OUTPUT_PATH}")

    qid = run_query(QUERY, description="generate mapping backlog", database=ATHENA_DB, timeout_seconds=180)
    print(f"QueryExecutionId: {qid}")
    headers, rows = fetch_all_rows(qid)
    write_csv(headers, rows)

    print(f"\n✓ Backlog generated: {len(rows)} rows")
    summarize(rows, headers)
    if rows:
        print("\nTop 10 rows:")
        for row in rows[:10]:
            print(f"  - {row[0]} | canonical={row[3]} | action={row[10]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
