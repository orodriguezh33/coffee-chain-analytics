-- stg_product_costs.sql
-- Limpieza de tabla de costos del ERP.

with source as (
    select * from {{ source('bronze_synthetic', 'bronze_product_costs') }}
),

latest_snapshot as (
    select cast(max(ingestion_date) as varchar) as ingestion_date
    from source
),

cleaned as (
    select
        trim(product_name) as product_name,
        trim(product_category) as product_category,
        cast(unit_price_std as decimal(10,2)) as unit_price_std,
        cast(unit_cost as decimal(10,4)) as unit_cost,
        cast(cogs_pct as decimal(6,4)) as cogs_pct,
        cast(valid_from as date) as valid_from,
        cast(valid_to as date) as valid_to,
        trim(source_system) as source_system,
        cast(ingestion_date as varchar) as ingestion_date
    from source
    where cast(ingestion_date as varchar) = (select ingestion_date from latest_snapshot)
      and cast(unit_cost as decimal(10,4)) > 0
      and cast(unit_price_std as decimal(10,2)) > 0
      and cast(unit_cost as decimal(10,4)) <= cast(unit_price_std as decimal(10,2))
)

select * from cleaned
