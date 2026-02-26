# ARCHITECTURE

## End-to-end flow

```
POS CSV + synthetic sources
        |
        v
Python ingestion scripts
        |
        v
S3 Bronze (raw CSV, ingestion_date partitions)
        |
        v
Athena external tables (coffee_chain.*)
        |
        v
dbt (Athena adapter)
  - staging (silver views)
  - intermediate (silver views)
  - marts (gold tables)
        |
        v
Athena Gold schema (gold.*)
        |
        v
Power BI dashboards
```

## Orchestration
Airflow DAG (`coffee_chain_daily`) executes:
1. POS ingestion
2. Synthetic ingestion
3. Bronze upload to S3
4. Athena partition repair
5. Quality gates pre-dbt
6. dbt run
7. dbt test
8. Quality gates post-dbt
9. Success/failure audit marker in S3

## Data quality
Athena quality gates run with severity levels:
- CRITICAL: stop pipeline
- WARNING: continue with alert context
- INFO: diagnostic only

## Serving decision
Current production-like portfolio scope uses Athena + dbt Gold as serving layer.
Redshift is not required for this repository to deliver decision-ready BI.
