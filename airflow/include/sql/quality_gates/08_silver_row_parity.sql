-- 08_silver_row_parity.sql
-- LEVEL: CRITICAL
-- Silver should retain >=99% of latest Bronze POS rows.

WITH latest_ingest AS (
    SELECT MAX(ingestion_date) AS ingestion_date
    FROM coffee_chain.bronze_pos_transactions
),
bronze_count AS (
    SELECT COUNT(*) AS cnt
    FROM coffee_chain.bronze_pos_transactions
    CROSS JOIN latest_ingest
    WHERE bronze_pos_transactions.ingestion_date = latest_ingest.ingestion_date
),
silver_count AS (
    SELECT COUNT(*) AS cnt
    FROM silver.stg_pos_transactions
    CROSS JOIN latest_ingest
    WHERE stg_pos_transactions.ingestion_date = latest_ingest.ingestion_date
),
summary AS (
    SELECT
        b.cnt AS bronze_rows,
        s.cnt AS silver_rows,
        ROUND(CAST(s.cnt AS DOUBLE) / NULLIF(CAST(b.cnt AS DOUBLE), 0) * 100, 2) AS pct_retained,
        CASE
            WHEN CAST(s.cnt AS DOUBLE) / NULLIF(CAST(b.cnt AS DOUBLE), 0) < 0.99
                THEN 'CRITICAL: silver retained less than 99% of bronze rows'
            ELSE 'PASS'
        END AS status
    FROM bronze_count b
    CROSS JOIN silver_count s
)
SELECT *
FROM summary
WHERE status <> 'PASS';
