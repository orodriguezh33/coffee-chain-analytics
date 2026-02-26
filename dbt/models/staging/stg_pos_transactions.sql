-- stg_pos_transactions.sql
-- Limpieza y casteo del POS + mapping a nombre canonico.
-- Grain: linea de producto por transaccion.

with source as (
    select * from {{ source('bronze_pos', 'bronze_pos_transactions') }}
),

mapping as (
    select
        trim(pos_product_detail) as pos_product_detail,
        trim(canonical_product_name) as canonical_product_name,
        nullif(trim(size_code), '') as size_code,
        cast(cost_required as boolean) as cost_required,
        cast(recipe_required as boolean) as recipe_required,
        trim(product_group) as product_group
    from {{ ref('product_name_mapping') }}
),

typed as (
    select
        cast(transaction_id as varchar) as transaction_id,
        cast(store_id as integer) as store_id,
        cast(product_id as integer) as product_id,
        cast(transaction_date as date) as transaction_date,
        cast(transaction_time as varchar) as transaction_time,
        cast(substr(transaction_time, 1, 2) as integer) as transaction_hour,
        cast(transaction_qty as integer) as transaction_qty,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(transaction_qty as integer) * cast(unit_price as decimal(10,2)) as gross_revenue,
        trim(store_location) as store_location,
        trim(product_category) as product_category,
        trim(product_type) as product_type,
        trim(product_detail) as product_name_raw,
        cast(ingestion_date as varchar) as ingestion_date
    from source
    where transaction_id is not null
      and transaction_date is not null
      and transaction_qty is not null
      and unit_price is not null
),

cleaned as (
    select
        t.*,
        row_number() over (
            partition by t.transaction_id
            order by
                t.product_id,
                t.product_name_raw,
                t.transaction_time,
                t.unit_price,
                t.transaction_qty
        ) as transaction_line_number,
        coalesce(m.canonical_product_name, t.product_name_raw) as product_name,
        m.size_code,
        coalesce(m.cost_required, true) as cost_required,
        coalesce(m.recipe_required, true) as recipe_required,
        coalesce(m.product_group, 'unclassified') as product_group
    from typed t
    left join mapping m
      on t.product_name_raw = m.pos_product_detail
    where t.transaction_qty > 0
      and t.unit_price > cast(0 as decimal(10,2))
)

select
    transaction_id,
    transaction_line_number,
    store_id,
    product_id,
    transaction_date,
    transaction_time,
    transaction_hour,
    transaction_qty,
    unit_price,
    gross_revenue,
    store_location,
    product_category,
    product_type,
    product_name_raw,
    product_name,
    size_code,
    cost_required,
    recipe_required,
    product_group,
    ingestion_date
from cleaned
