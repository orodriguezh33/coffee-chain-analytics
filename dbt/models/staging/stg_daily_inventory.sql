-- stg_daily_inventory.sql
-- Conteo fisico por ingrediente x tienda x dia.

with source as (
    select * from {{ source('bronze_synthetic', 'bronze_daily_inventory') }}
),

cleaned as (
    select
        cast(inventory_date as date) as inventory_date,
        cast(store_id as integer) as store_id,
        trim(store_name) as store_name,
        trim(ingredient_name) as ingredient_name,
        cast(opening_stock as decimal(12,2)) as opening_stock,
        cast(units_received as decimal(12,2)) as units_received,
        cast(actual_consumed as decimal(12,2)) as actual_consumed,
        cast(closing_stock as decimal(12,2)) as closing_stock,
        cast(ingestion_date as varchar) as ingestion_date
    from source
    where inventory_date is not null
      and cast(closing_stock as decimal(12,2)) >= 0
)

select * from cleaned
