# CV Bullets

## Data Engineer
- Built an end-to-end coffee retail analytics platform on S3 + Athena + dbt + Airflow, processing 150K+ POS rows and integrating synthetic ERP/WMS/Payroll/CRM sources into Bronze/Silver/Gold with automated quality gates.
- Implemented orchestration and failure controls in Airflow (pre/post dbt validation, retry policy, critical-stop logic), enabling reliable daily KPI delivery for operations.

## Analytics Engineer
- Designed dimensional marts and dbt transformations for core retail KPIs (gross margin, waste rate, labor cost %, stockout risk), with tests, lineage, and reproducible model documentation.
- Built product canonicalization workflow (mapping seed + coverage diagnostics) to resolve cross-source naming mismatches and stabilize margin/waste calculations.

## Hybrid DE/AE
- Architected an Athena-first decision layer (S3 -> dbt -> Athena -> Power BI) that links POS sales, recipe BOM, and inventory movement to operational decisions used by store and operations leadership.
