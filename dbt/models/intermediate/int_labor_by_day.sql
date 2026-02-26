-- int_labor_by_day.sql
-- Agrega turnos por tienda y dia y cruza con revenue diario.

with shifts as (
    select
        shift_date,
        store_id,
        sum(labor_cost_usd) as total_labor_cost,
        sum(total_hours) as total_hours_worked,
        sum(employees_on_shift) as total_employee_shifts
    from {{ ref('stg_staff_shifts') }}
    group by 1, 2
),

daily_revenue as (
    select
        transaction_date,
        store_id,
        sum(gross_revenue) as daily_revenue
    from {{ ref('stg_pos_transactions') }}
    group by 1, 2
),

joined as (
    select
        s.shift_date,
        s.store_id,
        s.total_labor_cost,
        s.total_hours_worked,
        s.total_employee_shifts,
        r.daily_revenue,
        case
            when r.daily_revenue is not null and r.daily_revenue <> 0
                then round(s.total_labor_cost / r.daily_revenue * 100, 2)
            else null
        end as labor_cost_pct
    from shifts s
    left join daily_revenue r
      on s.shift_date = r.transaction_date
     and s.store_id = r.store_id
)

select * from joined
