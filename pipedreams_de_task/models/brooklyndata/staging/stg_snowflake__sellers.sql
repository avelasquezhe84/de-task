with sellers as (
    select * from {{ source('brooklyndata', 'olist_sellers_dataset') }}
)

, final as (
    select
        trim(seller_id) as seller_id,
        {{ parse_zipcode('seller_zip_code_prefix') }},
        trim(seller_city) as seller_city,
        trim(seller_state) as seller_state
    from
        sellers
)

select * from final
