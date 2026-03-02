# BI y Dashboards

## Enfoque del proyecto
La capa de consumo del proyecto usa `Athena + dbt Gold + Power BI`.
Power BI no se conecta en tiempo real a Athena en este repositorio; consume un snapshot CSV exportado desde Athena para mantener el flujo simple, reproducible y estable en entorno local.

## Modelo que consume Power BI

### Hechos
- `fct_sales`
- `fct_waste`
- `fct_labor`
- `fct_inventory_snapshot`

### Dimensiones
- `dim_date`
- `dim_store`
- `dim_product`
- `dim_ingredient`

## KPIs principales
- `Revenue`: suma de `gross_revenue`
- `COGS`: suma de `cogs_amount`
- `Gross Profit`: suma de `gross_profit`
- `Gross Margin %`: `Gross Profit / Revenue`
- `Labor Cost %`: `total_labor_cost / daily_revenue`
- `Waste Amount`: suma de `waste_amount_usd`
- `Waste Rate %`: `waste_qty / theoretical_consumption`
- `Revenue per Labor Hour`: `daily_revenue / total_hours_worked`
- `Stockout HIGH Count Último Día`: ingredientes en riesgo `HIGH` en la última fecha de snapshot

## Dashboards

### 1. Executive Summary
- Audiencia: dirección y operaciones
- Preguntas: ¿cómo va el negocio?, ¿qué tienda presiona margen?, ¿cómo está labor y merma?
- Visuales:
  - cards de `Revenue`, `Gross Profit`, `Gross Margin %`, `Labor Cost %`, `Waste Amount`
  - tendencia mensual
  - revenue por tienda
  - revenue por categoría
  - tabla resumen mensual

### 2. Financial Deep Dive
- Audiencia: finanzas / análisis de rentabilidad
- Preguntas: ¿qué productos y categorías tienen mejor margen?
- Visuales:
  - tabla por producto con `Units Sold`, `Revenue`, `COGS`, `Gross Profit`, `Gross Margin %`
  - barra por categoría
  - scatter `Revenue vs Gross Margin %` por categoría

### 3. Waste & Inventory
- Audiencia: gerente de tienda / operaciones
- Preguntas: ¿dónde está la merma?, ¿qué ingrediente está en riesgo?
- Visuales:
  - `Waste Rate %` por ingrediente
  - card de `Stockout HIGH Count Último Día`
  - tabla de ingredientes con días restantes
  - tendencia de `Waste Amount`

### 4. Labor & Operational
- Audiencia: operaciones / tienda
- Preguntas: ¿labor está en objetivo?, ¿cuándo ocurre la demanda?
- Visuales:
  - `Labor Cost %` por mes
  - `Revenue per Labor Hour` por tienda
  - matriz por hora y día de semana con formato condicional

### 5. Branch & Product Performance
- Audiencia: liderazgo de operaciones
- Preguntas: ¿qué sucursal necesita atención?, ¿qué productos empujan el resultado?
- Visuales:
  - scorecard por tienda
  - top productos por tienda
  - scatter o barra por categoría

## Diferenciador del proyecto
El dashboard de `Waste & Inventory` es la pieza más fuerte porque cruza tres señales distintas:
- ventas POS
- consumo teórico por receta
- inventario físico

Eso permite pasar de un dashboard de ventas a una herramienta de decisión operativa real.

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
```

## Uso real
- Gerente de tienda:
  - revisa `Waste & Inventory` antes de abrir
  - confirma riesgo de quiebre y merma anormal
- Dirección de operaciones:
  - usa `Executive Summary` y `Branch & Product Performance`
  - compara tiendas y define acciones semanales
- Finanzas:
  - usa `Financial Deep Dive`
  - revisa mezcla de margen por producto y categoría
