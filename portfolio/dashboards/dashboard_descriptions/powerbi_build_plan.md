# Plan de construcción Power BI (Athena Snapshot)

## Objetivo
Construir el modelo y los dashboards en Power BI con carga manual (Import Mode), usando snapshots CSV exportados desde Athena.

## Alcance de tablas
Tablas de hechos:
- `fct_sales`
- `fct_waste`
- `fct_labor`
- `fct_inventory_snapshot`

Dimensiones:
- `dim_date`
- `dim_store`
- `dim_product`
- `dim_ingredient`

## Flujo recomendado
1. Exportar snapshot desde Athena a CSV.
2. Cargar los 8 CSV en Power BI Desktop (VM Windows).
3. Configurar relaciones tipo estrella.
4. Crear tabla de medidas DAX (`_measures`).
5. Construir las 5 páginas de dashboard.
6. Ejecutar QA cruzando métricas contra Athena.

## Relaciones del modelo
Relaciones 1 a muchos (single direction):
- `dim_date[date_key]` -> `fct_sales[date_key]`
- `dim_date[date_key]` -> `fct_waste[date_key]`
- `dim_date[date_key]` -> `fct_labor[date_key]`
- `dim_date[date_key]` -> `fct_inventory_snapshot[date_key]`
- `dim_store[store_key]` -> `fct_sales[store_key]`
- `dim_store[store_key]` -> `fct_waste[store_key]`
- `dim_store[store_key]` -> `fct_labor[store_key]`
- `dim_store[store_key]` -> `fct_inventory_snapshot[store_key]`
- `dim_product[product_key]` -> `fct_sales[product_key]`
- `dim_ingredient[ingredient_key]` -> `fct_waste[ingredient_key]`

No conectar facts entre sí.

## Medidas DAX base
```dax
Revenue = SUM(fct_sales[gross_revenue])
COGS = SUM(fct_sales[cogs_amount])
Gross Profit = SUM(fct_sales[gross_profit])
Gross Margin % = DIVIDE([Gross Profit], [Revenue], 0)

Labor Cost = SUM(fct_labor[total_labor_cost])
Labor Revenue = SUM(fct_labor[daily_revenue])
Labor Cost % = DIVIDE([Labor Cost], [Labor Revenue], 0)

Waste Qty = SUM(fct_waste[waste_qty])
Waste Amount = SUM(fct_waste[waste_amount_usd])
Theoretical Consumption = SUM(fct_waste[theoretical_consumption])
Waste Rate % = DIVIDE([Waste Qty], [Theoretical Consumption], 0)

Revenue per Labor Hour =
DIVIDE(SUM(fct_labor[daily_revenue]), SUM(fct_labor[total_hours_worked]), 0)

Stockout HIGH Count =
CALCULATE(
    COUNTROWS(fct_inventory_snapshot),
    fct_inventory_snapshot[stockout_risk_flag] = "HIGH"
)
```

## Diseño de páginas (versión portafolio)
1. `Executive Summary`
- KPIs: Revenue, Gross Margin %, Labor Cost %, Waste Amount.
- Tendencia mensual y comparativo por tienda.

2. `Financial Deep Dive`
- Tabla Top productos por margen.
- Barras por categoría (Revenue vs Gross Profit).

3. `Waste & Inventory`
- Waste rate por ingrediente.
- Tabla de riesgo de stockout.

4. `Labor & Operational`
- Labor % por día.
- Revenue per labor hour.
- Heatmap hora x día.

5. `Branch & Product Performance`
- Scorecard por tienda.
- Top productos por tienda.

## QA mínimo antes de publicar
Checklist:
- Totales de Revenue en Power BI = totales en Athena.
- Totales de Gross Profit en Power BI = totales en Athena.
- Totales de Waste Amount en Power BI = totales en Athena.
- Totales de Labor Cost en Power BI = totales en Athena.
- Filtros de fecha y tienda impactan todas las visuales relevantes.

## Export para portafolio
- Guardar `.pbix` en VM.
- Exportar imágenes por página a:
  - `portfolio/dashboards/screenshots/01_executive_summary.png`
  - `portfolio/dashboards/screenshots/02_financial_deep_dive.png`
  - `portfolio/dashboards/screenshots/03_waste_inventory.png`
  - `portfolio/dashboards/screenshots/04_labor_efficiency.png`
  - `portfolio/dashboards/screenshots/05_branch_product.png`
