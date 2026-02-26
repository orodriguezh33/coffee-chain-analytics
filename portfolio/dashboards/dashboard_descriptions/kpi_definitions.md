# KPI Definitions (Athena + dbt Gold)

This project serves BI directly from Athena tables in schema `gold`.

## Core KPIs

### Revenue
- Definition: `SUM(gold.fct_sales.gross_revenue)`
- Business meaning: total sales value for selected filters.

### COGS
- Definition: `SUM(gold.fct_sales.cogs_amount)`
- Business meaning: direct cost of products sold.

### Gross Margin %
- Definition: `SUM(gross_profit) / SUM(gross_revenue)`
- Business meaning: gross profitability after COGS.

### Labor Cost %
- Definition: `SUM(gold.fct_labor.total_labor_cost) / SUM(gold.fct_labor.daily_revenue)`
- Business meaning: labor pressure on daily sales.

### Waste Rate %
- Definition: `SUM(gold.fct_waste.waste_qty) / SUM(gold.fct_waste.theoretical_consumption)`
- Business meaning: operational waste against recipe-based expected consumption.

### Revenue per Labor Hour
- Definition: `SUM(gold.fct_labor.daily_revenue) / SUM(gold.fct_labor.total_hours_worked)`
- Business meaning: staff productivity proxy.

### Stockout HIGH Count
- Definition: count of rows in `gold.fct_inventory_snapshot` where `stockout_risk_flag = 'HIGH'`.
- Business meaning: urgent replenishment risk.

## Data model source
- Facts: `gold.fct_sales`, `gold.fct_waste`, `gold.fct_labor`, `gold.fct_inventory_snapshot`
- Dimensions: `gold.dim_date`, `gold.dim_store`, `gold.dim_product`, `gold.dim_ingredient`
