with orders as (
    select * from {{ ref('stg_snowflake__orders') }}
)

, items as (
    select * from {{ ref('stg_snowflake__order_items') }}
)

, products as (
    select * from {{ ref('stg_snowflake__products') }}
)

, final as (
    select 
        o.order_purchase_date as purchase_date,
        p.product_category_name as category,
        (sum(i.price) + sum(i.freight_value)) as revenue,
        rank() over(partition by purchase_date order by purchase_date desc, revenue desc) as rnk
    from orders o
    join items i on o.order_id = i.order_id 
    join products p on i.product_id = p.product_id 
    group by purchase_date, category
)

select * from final
