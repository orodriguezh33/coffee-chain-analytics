-- 04_referential_integrity.sql
-- LEVEL: WARNING
-- Unexpected store_ids may indicate new store or coding issue.

SELECT
    store_id,
    store_location,
    COUNT(*) AS transaction_count,
    'WARNING: unknown store_id' AS status
FROM coffee_chain.bronze_pos_transactions
WHERE ingestion_date = (
    SELECT MAX(ingestion_date)
    FROM coffee_chain.bronze_pos_transactions
)
AND CAST(store_id AS INTEGER) NOT IN (3, 5, 8)
GROUP BY 1, 2
HAVING COUNT(*) > 0;
