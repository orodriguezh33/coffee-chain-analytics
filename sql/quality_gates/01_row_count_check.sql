-- 01_row_count_check.sql
-- LEVEL: CRITICAL
-- Validate reasonable volume on the latest Bronze POS ingestion.

WITH latest_pos AS (
    SELECT *
    FROM coffee_chain.bronze_pos_transactions
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM coffee_chain.bronze_pos_transactions
    )
),
summary AS (
    SELECT
        ingestion_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT store_id) AS store_count,
        CASE
            WHEN COUNT(*) < 300
                THEN 'CRITICAL: row count below threshold (min 300)'
            WHEN COUNT(DISTINCT store_id) < 3
                THEN 'CRITICAL: missing data for one or more stores'
            ELSE 'PASS'
        END AS status
    FROM latest_pos
    GROUP BY ingestion_date
)
SELECT *
FROM summary
WHERE status <> 'PASS';
