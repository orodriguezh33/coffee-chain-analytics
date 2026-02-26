-- fct_waste.sql
-- Grain: ingrediente x tienda x dia.

with theoretical as (
    select * from {{ ref('int_theoretical_consumption') }}
),

actual as (
    select
        inventory_date,
        store_id,
        ingredient_name,
        actual_consumed
    from {{ ref('stg_daily_inventory') }}
),

dim_d as (select date_key, full_date from {{ ref('dim_date') }}),
dim_s as (select store_key, store_id from {{ ref('dim_store') }}),
dim_i as (select ingredient_key, ingredient_name from {{ ref('dim_ingredient') }}),

waste as (
    select
        t.transaction_date,
        t.store_id,
        t.ingredient_name,
        t.unit_of_measure,
        t.ingredient_cost_per_unit,
        t.theoretical_consumption,
        a.actual_consumed,
        round(a.actual_consumed - t.theoretical_consumption, 4) as waste_qty,
        round((a.actual_consumed - t.theoretical_consumption) * t.ingredient_cost_per_unit, 4) as waste_amount_usd,
        case
            when t.theoretical_consumption <> 0
                then round((a.actual_consumed - t.theoretical_consumption) / t.theoretical_consumption * 100, 2)
            else null
        end as waste_rate_pct
    from theoretical t
    left join actual a
      on t.transaction_date = a.inventory_date
     and t.store_id = a.store_id
     and t.ingredient_name = a.ingredient_name
)

select
    {{ dbt_utils.generate_surrogate_key([
        'cast(w.transaction_date as varchar)',
        'cast(w.store_id as varchar)',
        'w.ingredient_name'
    ]) }} as waste_key,
    dd.date_key,
    ds.store_key,
    di.ingredient_key,
    w.ingredient_name,
    w.unit_of_measure,
    w.theoretical_consumption,
    w.actual_consumed,
    w.waste_qty,
    w.waste_amount_usd,
    w.waste_rate_pct,
    w.transaction_date,
    w.store_id
from waste w
left join dim_d dd on w.transaction_date = dd.full_date
left join dim_s ds on w.store_id = ds.store_id
left join dim_i di on w.ingredient_name = di.ingredient_name
