-- stg_staff_shifts.sql
-- Turnos de personal por tienda y dia.

with source as (
    select * from {{ source('bronze_synthetic', 'bronze_staff_shifts') }}
),

latest_snapshot as (
    select cast(max(ingestion_date) as varchar) as ingestion_date
    from source
),

cleaned as (
    select
        cast(shift_date as date) as shift_date,
        cast(store_id as integer) as store_id,
        trim(store_name) as store_name,
        trim(shift_type) as shift_type,
        trim(shift_start) as shift_start,
        trim(shift_end) as shift_end,
        cast(employees_on_shift as integer) as employees_on_shift,
        cast(hours_per_employee as decimal(4,1)) as hours_per_employee,
        cast(total_hours as decimal(6,1)) as total_hours,
        cast(hourly_rate_usd as decimal(6,2)) as hourly_rate_usd,
        cast(labor_cost_usd as decimal(10,2)) as labor_cost_usd,
        cast(ingestion_date as varchar) as ingestion_date
    from source
    where cast(ingestion_date as varchar) = (select ingestion_date from latest_snapshot)
      and cast(labor_cost_usd as decimal(10,2)) > 0
      and cast(employees_on_shift as integer) > 0
)

select * from cleaned
