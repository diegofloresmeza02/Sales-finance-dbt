-- Catálogo de productos limpio.

with source as (
    select * from {{ source('raw_sales', 'products') }}
),

renamed as (
    select
        product_id::varchar                         as product_id,
        sku::varchar                                as sku,
        trim(product_name)                          as product_name,
        lower(trim(category))                       as category,
        lower(trim(subcategory))                    as subcategory,
        lower(trim(brand))                          as brand,

        round(cost_price::numeric, 2)               as cost_price,
        round(list_price::numeric, 2)               as list_price,

        -- Margen base del producto (sin descuentos de venta)
        round(
            (list_price - cost_price) / nullif(list_price, 0),
        4)                                          as base_margin_pct,

        is_active::boolean                          as is_active,
        _loaded_at

    from source
    where product_id is not null
)

select * from renamed
