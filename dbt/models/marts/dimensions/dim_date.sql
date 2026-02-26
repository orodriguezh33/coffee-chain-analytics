-- dim_date.sql
-- Dimension de fechas.

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2023-07-01' as date)"
    ) }}
),

final as (
    select
        cast(date_format(cast(date_day as timestamp), '%Y%m%d') as integer) as date_key,
        cast(date_day as date) as full_date,
        year(cast(date_day as date)) as year,
        month(cast(date_day as date)) as month_num,
        date_format(cast(date_day as timestamp), '%M') as month_name,
        quarter(cast(date_day as date)) as quarter,
        week(cast(date_day as date)) as week_num,
        day(cast(date_day as date)) as day_of_month,
        day_of_week(cast(date_day as date)) as day_of_week_num,
        date_format(cast(date_day as timestamp), '%W') as day_of_week_name,
        case when day_of_week(cast(date_day as date)) in (6, 7) then true else false end as is_weekend,
        date_format(cast(date_day as timestamp), '%Y-%m') as year_month
    from date_spine
)

select * from final
