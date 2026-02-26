-- fct_labor.sql
-- Grain: tienda x dia.

with labor as (
    select * from {{ ref('int_labor_by_day') }}
),

dim_d as (select date_key, full_date from {{ ref('dim_date') }}),
dim_s as (select store_key, store_id from {{ ref('dim_store') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(labor.shift_date as varchar)',
            'cast(labor.store_id as varchar)'
        ]) }} as labor_key,
        dd.date_key,
        ds.store_key,
        labor.total_labor_cost,
        labor.total_hours_worked,
        labor.total_employee_shifts,
        labor.daily_revenue,
        labor.labor_cost_pct,
        case
            when labor.labor_cost_pct > 35 then 'OVER TARGET'
            when labor.labor_cost_pct > 30 then 'WATCH'
            else 'ON TARGET'
        end as labor_status,
        labor.shift_date,
        labor.store_id
    from labor
    left join dim_d dd on labor.shift_date = dd.full_date
    left join dim_s ds on labor.store_id = ds.store_id
)

select * from final
