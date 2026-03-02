-- weekly_waste_by_store.sql
-- Query en Athena sobre esquema Gold de dbt.
-- Pregunta: ¿Qué tienda está teniendo más merma por ingrediente esta semana?

SELECT
    ds.store_location,
    fw.ingredient_name,
    ROUND(SUM(fw.waste_amount_usd), 2) AS waste_usd,
    ROUND(AVG(fw.waste_rate_pct), 2) AS avg_waste_rate_pct,
    MAX(fw.waste_rate_pct) AS max_waste_rate_pct
FROM gold.fct_waste fw
JOIN gold.dim_store ds
    ON fw.store_key = ds.store_key
JOIN gold.dim_date dd
    ON fw.date_key = dd.date_key
WHERE dd.full_date >= date_add('day', -7, current_date)
GROUP BY 1, 2
ORDER BY waste_usd DESC;
