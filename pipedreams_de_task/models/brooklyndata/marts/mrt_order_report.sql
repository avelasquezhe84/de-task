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

, revenue_by_category as (
    select * from {{ ref('int_revenue_by_product_category') }}
)

-- TRANSFORMATION
, total_revenue as (
    select
        o.order_purchase_date as purchase_date,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as total_customers_making_orders,
        sum(p.payment_value) as total_revenue,
        round((sum(p.payment_value) / count(distinct o.order_id)), 2) as average_revenue_per_order
    from orders o
    join payments p on o.order_id = p.order_id
    where lower(trim(o.order_status)) <> 'canceled'
    group by purchase_date
)

-- FINAL SELECT
, final as (
    select
        t.purchase_date,
        t.total_orders,
        t.total_customers_making_orders,
        t.total_revenue,
        t.average_revenue_per_order,
        r.top_1_category,
        (ifnull(r.top_1_category_revenue, 0) / total_revenue) * 100 as top_1_category_revenue_share,
        r.top_2_category,
        (ifnull(r.top_2_category_revenue, 0) / total_revenue) * 100 as top_2_category_revenue_share,
        r.top_3_category,
        (ifnull(r.top_3_category_revenue, 0) / total_revenue) * 100 as top_3_category_revenue_share        
    from total_revenue t
    join revenue_by_category r on t.purchase_date = r.purchase_date
    order by purchase_date desc
)

select * from final
