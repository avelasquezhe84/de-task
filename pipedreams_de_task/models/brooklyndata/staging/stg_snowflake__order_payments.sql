with order_payments as (
    select * from {{ source('brooklyndata', 'olist_order_payments_dataset') }}
)

, final as (
    select
        trim(order_id) as order_id,
        try_to_number(payment_sequential, 4, 0) as payment_sequential,
        trim(payment_type) as payment_type,
        try_to_number(payment_installments, 4, 0) as payment_installments,
        try_to_number(payment_value, 38, 2) as payment_value
    from
        order_payments
)

select * from final
