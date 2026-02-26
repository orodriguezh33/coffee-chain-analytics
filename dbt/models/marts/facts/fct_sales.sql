-- fct_sales.sql
-- Grain: linea de producto por transaccion.

with sales as (
    select * from {{ ref('int_sales_with_costs') }}
),

dim_d as (select date_key, full_date from {{ ref('dim_date') }}),
dim_s as (select store_key, store_id from {{ ref('dim_store') }}),
dim_p as (select product_key, product_name from {{ ref('dim_product') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'sales.transaction_id',
            'cast(sales.transaction_line_number as varchar)'
        ]) }} as sales_key,
        sales.transaction_id,
        sales.transaction_line_number,
        dd.date_key,
        ds.store_key,
        dp.product_key,
        sales.transaction_qty,
        sales.unit_price,
        sales.gross_revenue,
        sales.unit_cost,
        sales.cogs_amount,
        sales.gross_profit,
        sales.gross_margin_pct,
        sales.transaction_date,
        sales.transaction_hour,
        sales.cost_required,
        sales.recipe_required,
        sales.product_name_raw,
        sales.product_name
    from sales
    left join dim_d dd on sales.transaction_date = dd.full_date
    left join dim_s ds on sales.store_id = ds.store_id
    left join dim_p dp on sales.product_name = dp.product_name
)

select * from final
