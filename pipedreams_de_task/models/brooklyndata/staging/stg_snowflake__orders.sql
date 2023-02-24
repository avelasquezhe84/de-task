with orders as (
    select * from {{ source('brooklyndata', 'olist_orders_dataset') }}
)

, final as (
    select
        trim(order_id) as order_id,
        trim(customer_id) as customer_id,
        trim(order_status) as order_status,
        try_to_date(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') as order_purchase_date,
        try_to_date(order_approved_at, 'YYYY-MM-DD HH24:MI:SS') as order_approved_at,
        try_to_date(order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS') as order_delivered_carrier_date,
        try_to_date(order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') as order_delivered_customer_date,
        try_to_date(order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS') as order_estimated_delivery_date
    from
        orders
)

select * from final
