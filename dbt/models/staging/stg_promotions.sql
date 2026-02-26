-- stg_promotions.sql
-- Promociones del CRM/loyalty.

with source as (
    select * from {{ source('bronze_synthetic', 'bronze_promotions') }}
),

cleaned as (
    select
        trim(promotion_id) as promotion_id,
        trim(promotion_name) as promotion_name,
        trim(discount_type) as discount_type,
        cast(discount_value as decimal(10,2)) as discount_value,
        trim(applicable_category) as applicable_category,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        trim(applicable_days) as applicable_days,
        trim(applicable_hours) as applicable_hours,
        trim(source_system) as source_system
    from source
)

select * from cleaned
