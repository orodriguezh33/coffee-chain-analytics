# Interview Narrative (3-5 minutes)

## Hook (30s)
I worked in coffee retail operations and saw teams making daily staffing and inventory decisions with weak data visibility. This project builds the system that should exist in that environment.

## Problem (45s)
POS data alone cannot explain operational performance. Critical KPIs such as waste rate and labor efficiency require combining sales, costs, recipe standards, and physical inventory.

## Architecture decisions (90s)
- Data lake on S3 with Bronze/Silver/Gold.
- Bronze remains immutable to preserve source truth.
- dbt on Athena for typed transformations, tests, lineage, and model governance.
- Explicit fact grains (e.g., product line per transaction in `fct_sales`) to preserve KPI correctness.
- Product mapping seed to canonicalize naming across sources.
- Quality gates in Athena with severity levels so only critical failures stop the pipeline.
- Airflow orchestration for retries, ordering, and auditability.

## Business impact (45s)
- Managers get daily visibility on waste and stockout risk before opening.
- Operations can compare branch performance and labor efficiency weekly.
- Finance can analyze product margin with consistent cost attribution.

## Close (30s)
The differentiator is waste tracking: it depends on joining POS demand, recipe BOM, and physical inventory. Many public projects stop at sales dashboards; this one reaches operational decisioning.
