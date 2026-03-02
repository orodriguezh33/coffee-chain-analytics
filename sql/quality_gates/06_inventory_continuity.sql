-- 06_inventory_continuity.sql
-- LEVEL: WARNING
-- Prior day closing should match current day opening within tolerance.

WITH max_dates AS (
    SELECT
        MAX(CAST(inventory_date AS DATE)) AS today_date,
        MAX(CAST(inventory_date AS DATE)) FILTER (
            WHERE CAST(inventory_date AS DATE) < (
                SELECT MAX(CAST(inventory_date AS DATE))
                FROM coffee_chain.bronze_daily_inventory
            )
        ) AS yesterday_date
    FROM coffee_chain.bronze_daily_inventory
),
today AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        ingredient_name,
        CAST(opening_stock AS DOUBLE) AS opening_stock,
        CAST(inventory_date AS DATE) AS inventory_date
    FROM coffee_chain.bronze_daily_inventory
    CROSS JOIN max_dates
    WHERE CAST(inventory_date AS DATE) = max_dates.today_date
),
yesterday AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        ingredient_name,
        CAST(closing_stock AS DOUBLE) AS closing_stock,
        CAST(inventory_date AS DATE) AS inventory_date
    FROM coffee_chain.bronze_daily_inventory
    CROSS JOIN max_dates
    WHERE CAST(inventory_date AS DATE) = max_dates.yesterday_date
),
joined AS (
    SELECT
        t.store_id,
        t.ingredient_name,
        y.closing_stock AS yesterday_closing,
        t.opening_stock AS today_opening,
        ABS(t.opening_stock - COALESCE(y.closing_stock, 0)) AS discrepancy,
        CASE
            WHEN y.closing_stock IS NULL
                THEN 'WARNING: no inventory record for yesterday'
            WHEN ABS(t.opening_stock - y.closing_stock) > 5
                THEN 'WARNING: inventory gap > 5 units between days'
            ELSE 'PASS'
        END AS status
    FROM today t
    LEFT JOIN yesterday y
        ON t.store_id = y.store_id
       AND t.ingredient_name = y.ingredient_name
)
SELECT *
FROM joined
WHERE status <> 'PASS'
ORDER BY discrepancy DESC;
