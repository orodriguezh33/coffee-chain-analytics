-- 03_negative_values.sql
-- LEVEL: CRITICAL
-- Negative/zero prices and quantities are always invalid.

SELECT
    'unit_price' AS field_name,
    COUNT(*) AS invalid_count,
    MIN(CAST(unit_price AS DOUBLE)) AS min_value,
    'CRITICAL: negative or zero unit_price found' AS status
FROM coffee_chain.bronze_pos_transactions
WHERE ingestion_date = (
    SELECT MAX(ingestion_date)
    FROM coffee_chain.bronze_pos_transactions
)
AND CAST(unit_price AS DOUBLE) <= 0
HAVING COUNT(*) > 0

UNION ALL

SELECT
    'transaction_qty' AS field_name,
    COUNT(*) AS invalid_count,
    MIN(CAST(transaction_qty AS DOUBLE)) AS min_value,
    'CRITICAL: negative or zero transaction_qty found' AS status
FROM coffee_chain.bronze_pos_transactions
WHERE ingestion_date = (
    SELECT MAX(ingestion_date)
    FROM coffee_chain.bronze_pos_transactions
)
AND CAST(transaction_qty AS INTEGER) <= 0
HAVING COUNT(*) > 0;
