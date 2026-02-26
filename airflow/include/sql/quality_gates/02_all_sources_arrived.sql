-- 02_all_sources_arrived.sql
-- LEVEL: CRITICAL
-- Validate critical Bronze sources exist for latest ingestion.

WITH checks AS (
    SELECT
        'bronze_pos_transactions' AS source_name,
        COUNT(*) AS row_count,
        MAX(ingestion_date) AS last_ingestion,
        CASE
            WHEN COUNT(*) = 0 THEN 'CRITICAL: POS data not found'
            ELSE 'PASS'
        END AS status
    FROM coffee_chain.bronze_pos_transactions
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM coffee_chain.bronze_pos_transactions
    )

    UNION ALL

    SELECT
        'bronze_daily_inventory' AS source_name,
        COUNT(*) AS row_count,
        MAX(ingestion_date) AS last_ingestion,
        CASE
            WHEN COUNT(*) = 0 THEN 'CRITICAL: inventory data not found'
            ELSE 'PASS'
        END AS status
    FROM coffee_chain.bronze_daily_inventory
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM coffee_chain.bronze_daily_inventory
    )

    UNION ALL

    SELECT
        'bronze_staff_shifts' AS source_name,
        COUNT(*) AS row_count,
        MAX(ingestion_date) AS last_ingestion,
        CASE
            WHEN COUNT(*) = 0 THEN 'CRITICAL: staff shifts data not found'
            ELSE 'PASS'
        END AS status
    FROM coffee_chain.bronze_staff_shifts
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM coffee_chain.bronze_staff_shifts
    )
)
SELECT *
FROM checks
WHERE status <> 'PASS';
