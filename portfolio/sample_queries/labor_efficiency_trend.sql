-- labor_efficiency_trend.sql
-- Query en Athena sobre esquema Gold de dbt.
-- Pregunta: ¿La eficiencia laboral mejora semana a semana?

SELECT
    dd.year_month,
    dd.week_num,
    ds.store_location,
    ROUND(AVG(fl.labor_cost_pct), 2) AS avg_labor_pct,
    ROUND(SUM(fl.total_labor_cost), 2) AS total_labor_cost,
    ROUND(SUM(fl.daily_revenue), 2) AS total_revenue,
    COUNT(CASE WHEN fl.labor_status = 'OVER TARGET' THEN 1 END) AS days_over_target
FROM gold.fct_labor fl
JOIN gold.dim_date dd
    ON fl.date_key = dd.date_key
JOIN gold.dim_store ds
    ON fl.store_key = ds.store_key
GROUP BY 1, 2, 3
ORDER BY dd.year_month, dd.week_num, ds.store_location;
