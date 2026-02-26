-- dim_product.sql
-- Dimension de productos basada en costos + productos POS mapeados.

with pos_products as (
    select
        product_name,
        min(product_category) as product_category,
        min(product_type) as product_type,
        min(product_group) as product_group
    from {{ ref('stg_pos_transactions') }}
    group by 1
),

costs as (
    select * from {{ ref('stg_product_costs') }}
),

joined as (
    select
        p.product_name,
        coalesce(c.product_category, p.product_category) as product_category,
        p.product_type,
        p.product_group,
        c.unit_price_std,
        c.unit_cost,
        c.cogs_pct,
        c.valid_from,
        c.valid_to,
        c.source_system
    from pos_products p
    left join costs c
      on p.product_name = c.product_name
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_name']) }} as product_key,
        product_name,
        product_category,
        product_type,
        product_group,
        unit_price_std,
        unit_cost,
        cogs_pct,
        valid_from,
        valid_to,
        source_system,
        case
            when cogs_pct is null then 'Unknown'
            when (1 - cogs_pct) >= 0.70 then 'High Margin'
            when (1 - cogs_pct) >= 0.55 then 'Medium Margin'
            else 'Low Margin'
        end as margin_tier
    from joined
)

select * from final
