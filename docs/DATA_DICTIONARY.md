# Diccionario de Datos

## Tablas de hechos (Gold)

### gold.fct_sales
- Grano: una línea de producto por transacción
- Columnas clave:
  - `sales_key` (surrogate key)
  - `transaction_id`
  - `date_key`, `store_key`, `product_key`
  - `transaction_qty`, `unit_price`, `gross_revenue`
  - `unit_cost`, `cogs_amount`, `gross_profit`, `gross_margin_pct`

### gold.fct_waste
- Grano: ingrediente x tienda x día
- Columnas clave:
  - `waste_key` (surrogate key)
  - `date_key`, `store_key`
  - `ingredient_name`, `unit_of_measure`
  - `theoretical_consumption`, `actual_consumed`
  - `waste_qty`, `waste_amount_usd`, `waste_rate_pct`

### gold.fct_labor
- Grano: tienda x día
- Columnas clave:
  - `labor_key` (surrogate key)
  - `date_key`, `store_key`
  - `total_labor_cost`, `total_hours_worked`, `daily_revenue`
  - `labor_cost_pct`, `labor_status`

### gold.fct_inventory_snapshot
- Grano: ingrediente x tienda x día
- Columnas clave:
  - `snapshot_key` (surrogate key)
  - `date_key`, `store_key`
  - `ingredient_name`, `opening_stock`, `closing_stock`, `units_received`
  - `days_of_inventory_remaining`, `stockout_risk_flag`

## Dimensiones (Gold)

### gold.dim_date
- Grano: una fila por fecha de calendario
- Columnas: `date_key`, `full_date`, atributos de calendario (`year`, `month_num`, `week_num`, etc.)

### gold.dim_store
- Grano: una fila por tienda
- Columnas: `store_key`, `store_id`, `store_location`, `store_name`, `city`, `borough`

### gold.dim_product
- Grano: una fila por producto canónico
- Columnas: `product_key`, `product_name`, `product_category`, `unit_price_std`, `unit_cost`, `cogs_pct`, `margin_tier`

### gold.dim_ingredient
- Grano: una fila por ingrediente
- Columnas: `ingredient_key`, `ingredient_name`, `unit_of_measure`, `cost_per_unit`

## Seed de mapeo

### silver.product_name_mapping (dbt seed)
- Propósito: estandarizar nombres de producto del POS y definir banderas de negocio
- Campos principales:
  - `pos_product_detail`
  - `canonical_product_name`
  - `cost_required`
  - `recipe_required`
  - `product_group`
