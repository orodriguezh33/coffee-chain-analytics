-- int_sales_with_costs.sql
-- Une ventas con costos para calcular margen bruto.
-- Grain: linea de producto por transaccion.

with sales as (
    select * from {{ ref('stg_pos_transactions') }}
),

costs as (
    select * from {{ ref('stg_product_costs') }}
),

joined as (
    select
        s.transaction_id,
        s.transaction_line_number,
        s.transaction_date,
        s.transaction_hour,
        s.store_id,
        s.store_location,
        s.product_id,
        s.product_name_raw,
        s.product_name,
        s.product_category,
        s.product_type,
        s.product_group,
        s.cost_required,
        s.recipe_required,
        s.transaction_qty,
        s.unit_price,
        s.gross_revenue,
        c.unit_cost,
        c.cogs_pct,
        case
            when c.unit_cost is not null then round(s.transaction_qty * c.unit_cost, 2)
            else null
        end as cogs_amount,
        case
            when c.unit_cost is not null then round(s.gross_revenue - (s.transaction_qty * c.unit_cost), 2)
            else null
        end as gross_profit,
        case
            when c.unit_cost is not null and s.gross_revenue <> 0
                then round((s.gross_revenue - (s.transaction_qty * c.unit_cost)) / s.gross_revenue * 100, 2)
            else null
        end as gross_margin_pct
    from sales s
    left join costs c
      on s.product_name = c.product_name
     and s.transaction_date between c.valid_from and c.valid_to
)

select * from joined
