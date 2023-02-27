-- IMPORTS
with orders as (
    select * from {{ ref('stg_snowflake__orders') }}
)

, payments as (
    select * from {{ ref('stg_snowflake__order_payments') }}
)

, revenue_by_category as (
    select * from {{ ref('int_revenue_by_product_category') }}
)

-- TRANSFORMATIONS
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

, top_categories as (
    select
        t.purchase_date as order_purchase_date,
        t.total_orders as orders_count,
        round(t.total_customers_making_orders, 2) as customers_making_orders_count,
        t.total_revenue as revenue_usd,
        round(t.average_revenue_per_order, 2) as average_revenue_per_order_usd,
        ifnull(r.top_1_category, '') as top_1_category,
        round((ifnull(r.top_1_category_revenue, 0) / total_revenue) * 100, 2) as top_1_category_revenue_share,
        ifnull(r.top_2_category, '') as top_2_category,
        round((ifnull(r.top_2_category_revenue, 0) / total_revenue) * 100, 2) as top_2_category_revenue_share,
        ifnull(r.top_3_category, '') as top_3_category,
        round((ifnull(r.top_3_category_revenue, 0) / total_revenue) * 100, 2) as top_3_category_revenue_share        
    from total_revenue t
    join revenue_by_category r on t.purchase_date = r.purchase_date
)

-- FINAL SELECT
, final as (
    select
        order_purchase_date,
        orders_count,
        customers_making_orders_count,
        revenue_usd,
        average_revenue_per_order_usd,
        concat_ws(', ', top_1_category, top_2_category, top_3_category) as top_3_product_categories_by_revenue,
        concat_ws(', ', top_1_category_revenue_share, top_2_category_revenue_share, top_3_category_revenue_share) as top_3_product_categories_revenue_percentage
    from top_categories
    order by order_purchase_date desc
)

select * from final
