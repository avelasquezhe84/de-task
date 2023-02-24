with order_customer as (
    select * from {{ source('brooklyndata', 'olist_order_customer_dataset') }}
)

, final as (
    select
        trim(customer_id) as customer_id,
        trim(customer_unique_id) as customer_unique_id,
        {{ parse_zipcode('customer_zip_code_prefix') }},
        trim(customer_city) as customer_city,
        trim(customer_state) as customer_state 
    from
        order_customer
)

select * from final