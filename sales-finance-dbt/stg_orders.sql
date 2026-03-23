/*
  stg_orders
  ──────────
  Cleans and deduplicates raw orders from the ERP Fivetran feed.

  Source behavior: the ERP emits a full row on every status change.
  Fivetran appends every emission. This means one order_id can have
  3–5 rows in the raw table (placed, confirmed, processing, shipped, delivered).
  We keep the latest row per order_id based on _loaded_at.

  Edge case: ~0.2% of orders arrive with two rows sharing the exact same
  _loaded_at within a single Fivetran batch. In that case, row_number()
  picks one arbitrarily (no tiebreaker available without a sequence column
  in the source). Known issue, tracked as DATA-189.

  What we do NOT do here: apply revenue recognition logic. That belongs in
  int_order_line_economics. Staging is purely cast + rename + deduplicate.

  Null handling:
  - order_id null → excluded (can't be keyed)
  - shipped_date / delivered_date null → valid for in-progress orders, left as null
  - total_amount null → set to 0.0 rather than null to prevent silent
    aggregation errors downstream. A zero-amount order is a data quality issue
    but not a pipeline-breaking one.
*/

with source as (
    select * from {{ source('raw_sales', 'orders') }}
),

-- Deduplicate: keep latest row per order_id.
-- This is where the ERP's status-update-as-insert pattern gets resolved.
deduped as (
    select *,
        row_number() over (
            partition by order_id
            order by _loaded_at desc
        ) as rn
    from source
    where order_id is not null
),

latest as (
    select * from deduped where rn = 1
),

renamed as (
    select
        order_id::varchar                           as order_id,
        customer_id::varchar                        as customer_id,

        order_date::date                            as order_date,
        shipped_date::date                          as shipped_date,
        delivered_date::date                        as delivered_date,

        -- Normalize status: source has mixed casing ('Shipped', 'SHIPPED', 'shipped')
        lower(trim(status))                         as order_status,
        lower(trim(channel))                        as sales_channel,
        lower(trim(region))                         as region,

        round(subtotal_amount::numeric, 2)          as subtotal_amount,
        round(discount_amount::numeric, 2)          as discount_amount,
        round(tax_amount::numeric, 2)               as tax_amount,
        round(shipping_amount::numeric, 2)          as shipping_amount,
        -- Null-safe total: downstream aggregations fail silently with null amounts
        round(coalesce(total_amount::numeric, 0), 2) as total_amount,

        _loaded_at

    from latest
)

select * from renamed
