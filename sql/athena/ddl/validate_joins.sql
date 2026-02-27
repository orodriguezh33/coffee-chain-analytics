-- validate_joins.sql
-- Verifica cobertura de joins de Bronze (diagnóstico puro).
-- Importante:
-- - Este archivo NO normaliza nombres ni aplica reglas de negocio.
-- - La solución real se implementará en M4 con mapping explícito
--   (seed dbt: dbt/seeds/product_name_mapping.csv).

-- Check 1: productos del POS sin costo en catálogo sintético
SELECT
    pos.product_detail AS product_name,
    COUNT(*) AS transactions,
    costs.unit_cost AS unit_cost,
    CASE
        WHEN costs.unit_cost IS NULL THEN 'NO COST - map or extend synthetic catalog'
        ELSE 'OK'
    END AS status
FROM coffee_chain.bronze_pos_transactions pos
LEFT JOIN coffee_chain.bronze_product_costs costs
    ON TRIM(pos.product_detail) = TRIM(costs.product_name)
WHERE pos.ingestion_date IS NOT NULL
GROUP BY 1, 3
HAVING costs.unit_cost IS NULL
ORDER BY transactions DESC;

-- Check 2: cobertura de recetas por producto del POS
SELECT
    pos.product_detail AS product_name,
    COUNT(DISTINCT pos.transaction_id) AS transactions,
    COUNT(DISTINCT bom.ingredient_name) AS ingredients_in_recipe,
    CASE
        WHEN COUNT(DISTINCT bom.ingredient_name) = 0
            THEN 'NO RECIPE - map or extend synthetic catalog'
        ELSE 'OK'
    END AS status
FROM coffee_chain.bronze_pos_transactions pos
LEFT JOIN coffee_chain.bronze_recipes_bom bom
    ON TRIM(pos.product_detail) = TRIM(bom.product_name)
WHERE pos.ingestion_date IS NOT NULL
GROUP BY 1
ORDER BY transactions DESC;
