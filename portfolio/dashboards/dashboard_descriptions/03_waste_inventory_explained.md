# Dashboard 03: Waste & Inventory Control

## Why this dashboard matters
This is the strongest differentiator of the project because it combines three independent signals:
- sales demand (`gold.fct_sales`)
- theoretical ingredient consumption from recipes (`gold.fct_waste` inputs)
- physical inventory movement (`gold.fct_inventory_snapshot`)

Without this view, store teams react late to waste and stockout risk.

## Primary audience
- Store manager (daily, before opening)
- Operations lead (weekly)

## Main visuals
- Waste rate by ingredient (severity color)
- Stockout risk table (`HIGH`, `MEDIUM`, `LOW`)
- 30-day waste trend (rate + USD)
- Theoretical vs actual consumption comparison

## Decisions enabled
- Which ingredient needs immediate intervention?
- Should a purchase order be placed before opening?
- Is training reducing waste after process changes?

## Operational thresholds
- Waste rate target reference: `8%`
- Stockout risk is derived from `days_of_inventory_remaining`:
  - `< 2`: `HIGH`
  - `< 4`: `MEDIUM`
  - `>= 4`: `LOW`

## Suggested screenshot for portfolio
Save the final Power BI screenshot as:
- `portfolio/dashboards/screenshots/03_waste_inventory.png`
