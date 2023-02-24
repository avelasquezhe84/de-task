with revenue_by_category as (
    select * from {{ ref('int_revenue_by_product_category') }}
)

, final as (
    select
        purchase_date ,
        array_agg(category) within group (order by rnk) as top_categories
    from revenue_by_category
    where rnk < 4 
    group by purchase_date 
    order by purchase_date desc
)

select * from final