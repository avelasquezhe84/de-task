with products as (
    select * from {{ source('brooklyndata', 'olist_products_dataset') }}
)

, final as (
    select
        trim(product_id) as product_id,
        trim(product_category_name) as product_category_name,
        try_to_number(product_name_length, 10, 0) as product_name_length,
        try_to_number(product_description_length, 10, 0) as product_description_length,
        try_to_number(product_photos_qty, 10, 0) as product_photos_qty,
        try_to_number(product_weight_g, 10, 0) as product_weight_g,
        try_to_number(product_length_cm, 10, 0) as product_length_cm,
        try_to_number(product_height_cm, 10, 0) as product_height_cm,
        try_to_number(product_width_cm, 10, 0) as product_width_cm
    from
        products
)

select * from final
