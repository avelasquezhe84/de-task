-- IMPORTS
with orders as (
    select * from {{ ref('stg_snowflake__orders') }}
)

, payments as (
    select * from {{ ref('stg_snowflake__order_payments') }}
)

, items as (
    select * from {{ ref('stg_snowflake__order_items') }}
)

, products as (
    select * from {{ ref('stg_snowflake__products') }}
)

-- FINAL SELECT
, final as (
    select
        o.order_purchase_date as purchase_date,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as total_customers_making_orders,
        sum(p.payment_value) as total_revenue,
        round((sum(p.payment_value) / count(distinct o.order_id)), 2) as average_revenue_per_order
    from orders o
    join payments p on o.order_id = p.order_id
    group by purchase_date
    order by purchase_date desc
)

select * from final
