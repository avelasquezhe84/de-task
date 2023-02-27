-- IMPORTS
with orders as (
    select * from {{ ref('stg_snowflake__orders') }}
)

, items as (
    select * from {{ ref('stg_snowflake__order_items') }}
)

, products as (
    select * from {{ ref('stg_snowflake__products') }}
)

-- TRANSFORMATION
, revenue_by_category as (
    select 
        o.order_purchase_date as purchase_date,
        p.product_category_name as category,
        (sum(i.price) + sum(i.freight_value)) as revenue,
        rank() over(partition by purchase_date order by purchase_date desc, revenue desc) as rnk
    from orders o
    join items i on o.order_id = i.order_id 
    join products p on i.product_id = p.product_id 
    group by purchase_date, category
    order by purchase_date desc, rnk
)

-- FINAL SELECT
, final as (
    select
        purchase_date,
        max(case when rnk = 1 then category else null end) AS top_1_category,
        max(case when rnk = 1 then revenue else null end) AS top_1_category_revenue,
        max(case when rnk = 2 then category else null end) AS top_2_category,
        max(case when rnk = 2 then revenue else null end) AS top_2_category_revenue,
        max(case when rnk = 3 then category else null end) AS top_3_category,
        max(case when rnk = 3 then revenue else null end) AS top_3_category_revenue
    from revenue_by_category
    group by purchase_date 
    order by purchase_date desc
)

select * from final
