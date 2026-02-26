-- 07_labor_revenue_ratio.sql
-- LEVEL: INFO
-- Extreme labor ratios can indicate data issues or unusual sales days.

WITH labor AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        SUM(CAST(labor_cost_usd AS DOUBLE)) AS total_labor
    FROM coffee_chain.bronze_staff_shifts
    WHERE CAST(shift_date AS DATE) = (
        SELECT MAX(CAST(shift_date AS DATE))
        FROM coffee_chain.bronze_staff_shifts
    )
    GROUP BY 1
),
revenue AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        SUM(CAST(transaction_qty AS INTEGER) * CAST(unit_price AS DOUBLE)) AS total_revenue
    FROM coffee_chain.bronze_pos_transactions
    WHERE CAST(transaction_date AS DATE) = (
        SELECT MAX(CAST(transaction_date AS DATE))
        FROM coffee_chain.bronze_pos_transactions
    )
    GROUP BY 1
),
joined AS (
    SELECT
        l.store_id,
        ROUND(l.total_labor, 2) AS labor_cost,
        ROUND(r.total_revenue, 2) AS revenue,
        ROUND(l.total_labor / NULLIF(r.total_revenue, 0) * 100, 2) AS labor_pct,
        CASE
            WHEN r.total_revenue IS NULL
                THEN 'WARNING: no revenue data for this store'
            WHEN l.total_labor / NULLIF(r.total_revenue, 0) * 100 > 60
                THEN 'INFO: labor % above 60 - verify data'
            ELSE 'PASS'
        END AS status
    FROM labor l
    LEFT JOIN revenue r
        ON l.store_id = r.store_id
)
SELECT *
FROM joined
WHERE status <> 'PASS';
