-- dim_store.sql
-- Dimension de tiendas derivada del POS.

with stores as (
    select distinct store_id, store_location
    from {{ ref('stg_pos_transactions') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(store_id as varchar)']) }} as store_key,
        store_id,
        store_location,
        case store_id
            when 3 then 'Astoria'
            when 5 then 'Lower Manhattan'
            when 8 then 'Hell''s Kitchen'
            else store_location
        end as store_name,
        case store_id
            when 3 then 'Queens'
            when 5 then 'Manhattan'
            when 8 then 'Manhattan'
            else null
        end as borough,
        'New York' as city,
        cast('2019-01-01' as date) as open_date
    from stores
)

select * from final
