with order_items as (
    select * from {{ source('brooklyndata', 'olist_order_items_dataset') }}
)

, final as (
    select
        trim(order_id) as order_id,
        trim(order_item_id) as order_item_id,
        trim(product_id) as product_id,
        trim(seller_id) as seller_id,
        try_to_date(shipping_limit_date, 'YYYY-MM-DD HH24:MI:SS') as shipping_limit_date,
        try_to_number(price, 38, 2) as price,
        try_to_number(freight_value, 38, 2) as freight_value 
    from
        order_items
)

select * from final