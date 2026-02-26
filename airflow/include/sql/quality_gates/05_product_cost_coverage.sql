-- 05_product_cost_coverage.sql
-- LEVEL: WARNING
-- Uses the dbt mapping seed to avoid false positives from POS naming variants.

WITH latest_pos AS (
    SELECT
        product_detail,
        COUNT(*) AS transactions_today
    FROM coffee_chain.bronze_pos_transactions
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM coffee_chain.bronze_pos_transactions
    )
    GROUP BY product_detail
),
mapped AS (
    SELECT
        p.product_detail,
        p.transactions_today,
        m.canonical_product_name,
        COALESCE(LOWER(CAST(m.cost_required AS VARCHAR)), 'true') AS cost_required
    FROM latest_pos p
    LEFT JOIN silver.product_name_mapping m
        ON TRIM(p.product_detail) = TRIM(m.pos_product_detail)
),
missing_cost AS (
    SELECT
        mapped.product_detail AS product_name,
        mapped.transactions_today,
        CASE
            WHEN mapped.canonical_product_name IS NULL
                THEN 'WARNING: product missing mapping — gross margin may be NULL'
            ELSE 'WARNING: mapped product has no cost — gross margin will be NULL'
        END AS status
    FROM mapped
    LEFT JOIN coffee_chain.bronze_product_costs costs
        ON TRIM(mapped.canonical_product_name) = TRIM(costs.product_name)
    WHERE mapped.cost_required = 'true'
      AND (
          mapped.canonical_product_name IS NULL
          OR costs.product_name IS NULL
      )
)
SELECT *
FROM missing_cost
ORDER BY transactions_today DESC;
