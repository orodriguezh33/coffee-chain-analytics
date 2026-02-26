-- stg_recipes_bom.sql
-- Recetas estandar por producto.

with source as (
    select * from {{ source('bronze_synthetic', 'bronze_recipes_bom') }}
),

cleaned as (
    select
        trim(product_name) as product_name,
        trim(ingredient_name) as ingredient_name,
        cast(qty_required_per_unit as decimal(10,4)) as qty_required_per_unit,
        trim(unit_of_measure) as unit_of_measure,
        cast(ingredient_cost_per_unit as decimal(10,6)) as ingredient_cost_per_unit,
        cast(last_updated as date) as last_updated,
        cast(ingestion_date as varchar) as ingestion_date
    from source
    where cast(qty_required_per_unit as decimal(10,4)) > 0
)

select * from cleaned
