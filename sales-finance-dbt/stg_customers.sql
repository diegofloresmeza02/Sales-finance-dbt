-- Catálogo de clientes limpio y estandarizado.

with source as (
    select * from {{ source('raw_sales', 'customers') }}
),

renamed as (
    select
        customer_id::varchar                        as customer_id,

        -- Normalizamos el nombre para búsquedas consistentes
        initcap(trim(first_name))                   as first_name,
        initcap(trim(last_name))                    as last_name,
        trim(first_name) || ' ' || trim(last_name)  as full_name,

        lower(trim(email))                          as email,
        lower(trim(segment))                        as customer_segment,  -- 'consumer', 'corporate', 'home_office'
        lower(trim(country))                        as country,
        lower(trim(city))                           as city,

        registration_date::date                     as registration_date,
        _loaded_at

    from source
    where customer_id is not null
)

select * from renamed
