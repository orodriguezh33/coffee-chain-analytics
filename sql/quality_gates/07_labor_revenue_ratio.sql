-- 07_labor_revenue_ratio.sql
-- LEVEL: INFO
-- Extreme labor ratios can indicate data issues or unusual sales days.
-- Uses the same latest ingestion_date across both datasets to avoid date drift.

WITH latest_ingestion AS (
    SELECT LEAST(
        (SELECT MAX(ingestion_date) FROM coffee_chain.bronze_staff_shifts),
        (SELECT MAX(ingestion_date) FROM coffee_chain.bronze_pos_transactions)
    ) AS ingestion_date
),
target_date AS (
    SELECT
        latest_ingestion.ingestion_date AS ingestion_date,
        MAX(CAST(shift_date AS DATE)) AS business_date
    FROM coffee_chain.bronze_staff_shifts
    CROSS JOIN latest_ingestion
    WHERE bronze_staff_shifts.ingestion_date = latest_ingestion.ingestion_date
    GROUP BY latest_ingestion.ingestion_date
),

labor AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        SUM(CAST(labor_cost_usd AS DOUBLE)) AS total_labor
    FROM coffee_chain.bronze_staff_shifts
    CROSS JOIN target_date
    WHERE bronze_staff_shifts.ingestion_date = target_date.ingestion_date
      AND CAST(shift_date AS DATE) = target_date.business_date
    GROUP BY 1
),
revenue AS (
    SELECT
        CAST(store_id AS INTEGER) AS store_id,
        SUM(CAST(transaction_qty AS INTEGER) * CAST(unit_price AS DOUBLE)) AS total_revenue
    FROM coffee_chain.bronze_pos_transactions
    CROSS JOIN target_date
    WHERE bronze_pos_transactions.ingestion_date = target_date.ingestion_date
      AND CAST(transaction_date AS DATE) = target_date.business_date
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
