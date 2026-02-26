-- fct_inventory_snapshot.sql
-- Grain: ingrediente x tienda x dia.

with inventory as (
    select * from {{ ref('stg_daily_inventory') }}
),

avg_consumption as (
    select
        store_id,
        ingredient_name,
        inventory_date,
        avg(actual_consumed) over (
            partition by store_id, ingredient_name
            order by inventory_date
            rows between 6 preceding and current row
        ) as avg_daily_consumption_7d
    from inventory
),

dim_d as (select date_key, full_date from {{ ref('dim_date') }}),
dim_s as (select store_key, store_id from {{ ref('dim_store') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(i.inventory_date as varchar)',
            'cast(i.store_id as varchar)',
            'i.ingredient_name'
        ]) }} as snapshot_key,
        dd.date_key,
        ds.store_key,
        i.ingredient_name,
        i.opening_stock,
        i.closing_stock,
        i.units_received,
        round(i.closing_stock / nullif(ac.avg_daily_consumption_7d, 0), 1) as days_of_inventory_remaining,
        case
            when i.closing_stock / nullif(ac.avg_daily_consumption_7d, 0) < 2 then 'HIGH'
            when i.closing_stock / nullif(ac.avg_daily_consumption_7d, 0) < 4 then 'MEDIUM'
            else 'LOW'
        end as stockout_risk_flag,
        i.inventory_date as snapshot_date,
        i.store_id
    from inventory i
    left join avg_consumption ac
      on i.inventory_date = ac.inventory_date
     and i.store_id = ac.store_id
     and i.ingredient_name = ac.ingredient_name
    left join dim_d dd on i.inventory_date = dd.full_date
    left join dim_s ds on i.store_id = ds.store_id
)

select * from final
