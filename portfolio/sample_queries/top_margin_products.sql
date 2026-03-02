-- top_margin_products.sql
-- Query en Athena sobre esquema Gold de dbt.
-- Pregunta: ¿Qué productos son los más rentables?

SELECT
    dp.product_name,
    dp.product_category,
    dp.margin_tier,
    SUM(fs.transaction_qty) AS units_sold,
    ROUND(SUM(fs.gross_revenue), 2) AS total_revenue,
    ROUND(AVG(fs.gross_margin_pct), 2) AS avg_margin_pct,
    ROUND(SUM(fs.gross_profit), 2) AS total_gross_profit
FROM gold.fct_sales fs
JOIN gold.dim_product dp
    ON fs.product_key = dp.product_key
GROUP BY 1, 2, 3
ORDER BY avg_margin_pct DESC
LIMIT 10;
