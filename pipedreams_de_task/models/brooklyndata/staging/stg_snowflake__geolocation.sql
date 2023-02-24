with geolocation as (
    select * from {{ source('brooklyndata', 'olist_geolocation_dataset') }}
)

, final as (
    select
        {{ parse_zipcode('geolocation_zip_code_prefix') }},
        try_to_double(geolocation_lat) as geolocation_lat,
        try_to_double(geolocation_lng) as geolocation_lng,
        trim(geolocation_city) as geolocation_city,
        trim(geolocation_state) as geolocation_state
    from
        geolocation
)

select * from final