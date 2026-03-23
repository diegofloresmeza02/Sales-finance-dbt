-- Limpiamos el detalle de líneas por orden.
-- Cada fila es un producto dentro de una orden.

with source as (
    select * from {{ source('raw_sales', 'order_items') }}
),

renamed as (
    select
        order_item_id::varchar                      as order_item_id,
        order_id::varchar                           as order_id,
        product_id::varchar                         as product_id,

        quantity::int                               as quantity,
        round(unit_price::numeric, 2)               as unit_price,
        round(discount_pct::numeric, 4)             as discount_pct,

        -- Calculamos el monto neto directamente en staging
        round(
            quantity * unit_price * (1 - coalesce(discount_pct, 0)),
        2)                                          as net_amount,

        _loaded_at

    from source
    where order_item_id is not null
      and quantity > 0  -- ignoramos líneas con cantidad inválida
)

select * from renamed
