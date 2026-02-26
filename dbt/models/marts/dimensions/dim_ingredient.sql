-- dim_ingredient.sql
-- Dimension de ingredientes derivada del BOM.

with bom as (
    select distinct
        ingredient_name,
        unit_of_measure,
        ingredient_cost_per_unit
    from {{ ref('stg_recipes_bom') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ingredient_name']) }} as ingredient_key,
        ingredient_name,
        unit_of_measure,
        ingredient_cost_per_unit as cost_per_unit
    from bom
)

select * from final
