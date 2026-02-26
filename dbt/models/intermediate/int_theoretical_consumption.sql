-- int_theoretical_consumption.sql
-- Consumo teorico = ventas x receta.
-- Grain: ingrediente x tienda x dia.

with sales as (
    select
        transaction_date,
        store_id,
        product_name,
        sum(transaction_qty) as total_qty_sold
    from {{ ref('stg_pos_transactions') }}
    where recipe_required = true
    group by 1, 2, 3
),

bom as (
    select * from {{ ref('stg_recipes_bom') }}
),

theoretical as (
    select
        s.transaction_date,
        s.store_id,
        b.ingredient_name,
        b.unit_of_measure,
        b.ingredient_cost_per_unit,
        round(sum(s.total_qty_sold * b.qty_required_per_unit), 4) as theoretical_consumption
    from sales s
    inner join bom b
      on s.product_name = b.product_name
    group by 1, 2, 3, 4, 5
)

select * from theoretical
