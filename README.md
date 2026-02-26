# Coffee Chain Analytics Platform

> End-to-end data engineering project for coffee retail operations, focused on margin control, waste tracking, inventory optimization, and labor efficiency.

## The Problem
Coffee retail teams make daily decisions (staffing, ordering, promotions) with incomplete data. POS sales alone cannot explain true margin, operational waste, or staffing efficiency.

This project builds a full analytics platform that joins sales, costs, recipes, inventory, and labor into decision-ready KPIs.

## What This System Enables

| Decision | KPI | Frequency |
|---|---|---|
| Which products to promote | Gross Margin % by product | Daily |
| When to reorder ingredients | Stockout Risk Score | Daily |
| How to schedule staff | Labor Cost % by shift | Daily |
| Whether a promotion worked | Promotion impact metrics | Per event |
| Where waste is happening | Waste Rate % by ingredient | Daily |

## Architecture (Current Scope)

```
Data sources (POS + synthetic ERP/WMS/Payroll/CRM)
        -> Python ingestion
S3 Bronze (raw CSV)
        -> Athena external tables
        -> dbt (Athena adapter)
           staging -> intermediate -> marts
S3 Gold + Athena (serving layer)
        -> Power BI dashboards
Orchestration: Airflow
Data quality: Athena quality gates (CRITICAL/WARNING/INFO)
```

Note: Redshift was intentionally excluded from final scope for cost/control reasons. The project remains fully operational on Athena + dbt Gold.

## Stack

| Tool | Role | Why this choice |
|---|---|---|
| S3 | Data lake | Low cost, scalable Bronze/Silver/Gold storage |
| Athena | Query engine + quality checks | Serverless SQL, no infra management |
| dbt (Athena) | Transformations, tests, lineage | SQL-first modeling with testing and docs |
| Airflow | Orchestration | Retries, dependency control, observability |
| Python | Ingestion + synthetic source simulation | Flexible multi-source integration |
| Power BI | BI and decision dashboards | Familiar business-facing consumption layer |

## Data Model
Core marts implemented in dbt Gold schema:

- Facts: `fct_sales`, `fct_waste`, `fct_labor`, `fct_inventory_snapshot`
- Dimensions: `dim_date`, `dim_store`, `dim_product`, `dim_ingredient`

## Dashboards

| Dashboard | Audience | Decision |
|---|---|---|
| Executive Summary | C-level / operations leadership | Are we on track this month? |
| Financial Deep Dive | Finance / analyst | Which products have unsustainable margin? |
| Waste & Inventory | Store manager | What must be reordered before opening? |
| Labor Efficiency | Operations / HR | Is staffing aligned with demand? |
| Branch & Product | Operations director | Which store needs intervention this week? |

## Repository Structure

```
coffee-chain-analytics/
├── ingestion/
├── dbt/
├── airflow/
├── sql/
├── data/
├── docs/
└── portfolio/
```

## Documentation
- Architecture: `docs/ARCHITECTURE.md`
- Data dictionary: `docs/DATA_DICTIONARY.md`
- Design decisions: `docs/DECISIONS.md`
- BI decision layer: `portfolio/dashboards/dashboard_descriptions/bi_decision_layer.md`
- Interview storytelling package: `portfolio/interview/`

## Local Run (Airflow)

```bash
docker compose --profile airflow run --rm airflow-init
docker compose --profile airflow up -d airflow-db airflow-webserver airflow-scheduler
```

Airflow UI: [http://localhost:8080](http://localhost:8080)
DAG: `coffee_chain_daily`

## About
Built to demonstrate practical data engineering grounded in retail operations: turning multi-source data into decisions store teams can use daily.
