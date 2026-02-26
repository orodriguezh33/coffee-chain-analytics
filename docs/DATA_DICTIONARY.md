# DATA_DICTIONARY

## Gold facts

### gold.fct_sales
- Grain: one product line per transaction
- Key columns:
  - `sales_key` (surrogate)
  - `transaction_id`
  - `date_key`, `store_key`, `product_key`
  - `transaction_qty`, `unit_price`, `gross_revenue`
  - `unit_cost`, `cogs_amount`, `gross_profit`, `gross_margin_pct`

### gold.fct_waste
- Grain: ingredient x store x day
- Key columns:
  - `waste_key` (surrogate)
  - `date_key`, `store_key`
  - `ingredient_name`, `unit_of_measure`
  - `theoretical_consumption`, `actual_consumed`
  - `waste_qty`, `waste_amount_usd`, `waste_rate_pct`

### gold.fct_labor
- Grain: store x day
- Key columns:
  - `labor_key` (surrogate)
  - `date_key`, `store_key`
  - `total_labor_cost`, `total_hours_worked`, `daily_revenue`
  - `labor_cost_pct`, `labor_status`

### gold.fct_inventory_snapshot
- Grain: ingredient x store x day
- Key columns:
  - `snapshot_key` (surrogate)
  - `date_key`, `store_key`
  - `ingredient_name`, `opening_stock`, `closing_stock`, `units_received`
  - `days_of_inventory_remaining`, `stockout_risk_flag`

## Gold dimensions

### gold.dim_date
- Grain: one row per calendar date
- Columns: `date_key`, `full_date`, calendar attributes (`year`, `month_num`, `week_num`, etc.)

### gold.dim_store
- Grain: one row per store
- Columns: `store_key`, `store_id`, `store_location`, `store_name`, `city`, `borough`

### gold.dim_product
- Grain: one row per canonical product
- Columns: `product_key`, `product_name`, `product_category`, `unit_price_std`, `unit_cost`, `cogs_pct`, `margin_tier`

### gold.dim_ingredient
- Grain: one row per ingredient
- Columns: `ingredient_key`, `ingredient_name`, `unit_of_measure`, `cost_per_unit`

## Mapping seed

### silver.product_name_mapping (dbt seed)
- Purpose: canonicalize POS product names and define business flags
- Core fields:
  - `pos_product_detail`
  - `canonical_product_name`
  - `cost_required`
  - `recipe_required`
  - `product_group`
