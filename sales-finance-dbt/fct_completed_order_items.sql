/*
  fct_completed_order_items
  ──────────────────────────
  Grain: one row per order line item. Shipped and delivered orders only.
  ~18M rows in production. Full refresh: ~14 min on TRANSFORMING_WH SMALL.

  This is the revenue source of truth for commercial reporting.
  Finance uses it to reconcile against the GL monthly (see assert_revenue_matches_gl test).

  ──────────────────────────────────────────────────────────────────────────────
  Why this is not incremental yet (and why it should be)
  ──────────────────────────────────────────────────────────────────────────────
  Orders transition from processing → shipped up to 72 hours after placement.
  An incremental model on order_date needs a lookback window long enough to
  catch late status transitions. 3-day lookback covers ~97% of cases.
  The 3% edge cases are carrier delay scenarios (holiday peaks) where status
  updates arrive 4–5 days late. Business hasn't decided whether to accept
  that gap or extend the lookback (which increases cost). Full refresh for now.

  ──────────────────────────────────────────────────────────────────────────────
  Surrogate key note
  ──────────────────────────────────────────────────────────────────────────────
  order_item_id is a varchar from the ERP with environment-specific prefixes.
  A Fivetran incident in Oct 2023 ingested STG- prefixed IDs into prod.
  The surrogate key is computed from the cleaned order_item_id after staging.
  If the uniqueness test on order_item_sk passes, the data is clean.
  If it fails, something upstream is wrong — don't patch it here.

  ──────────────────────────────────────────────────────────────────────────────
  Window functions at line level (order_total_revenue etc.)
  ──────────────────────────────────────────────────────────────────────────────
  These are here so BI tools can answer "what was the total order value for
  orders containing this product" without a self-join.
  They add ~90s to the model run time on SMALL warehouse.
  If performance becomes a problem, move them to a separate agg model.
*/

with completed as (
    select *
    from {{ ref('int_order_line_economics') }}
    where is_revenue_recognized = true
    -- Paranoia filter: int_order_line_economics should only return recognized rows
    -- when is_revenue_recognized = true, but we're explicit here because this model
    -- is the revenue source of truth and silent filter failures would be catastrophic.
    and order_status in ({{ "'" + var('completed_order_statuses') | join("','") + "'" }})
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} as order_item_sk,

        order_item_id,
        order_id,
        product_id,
        customer_id,

        order_date,
        shipped_date,
        delivered_date,
        order_month,
        order_year,
        dayofweek(order_date)                                   as day_of_week,
        quarter(order_date)                                     as fiscal_quarter,
        weekofyear(order_date)                                  as iso_week,

        sales_channel,
        region,
        category,
        subcategory,
        brand,
        sku,

        quantity,

        -- Preserve list_price for discount analytics — not used in revenue calcs
        list_price,
        unit_price,
        discount_pct,
        effective_discount_pct,

        revenue,
        cogs,
        gross_profit,
        gross_margin_pct,

        -- Expose the COGS data quality flag so BI can filter or highlight affected rows.
        -- Finance tracks ~120 affected SKUs manually until the ERP provides archived cost_price.
        has_missing_cost,

        -- Order-level window aggregates. Allows product-level questions like
        -- "average basket size for orders containing SKU X" without a self-join.
        sum(revenue)      over (partition by order_id)          as order_total_revenue,
        sum(cogs)         over (partition by order_id)          as order_total_cogs,
        sum(gross_profit) over (partition by order_id)          as order_total_gross_profit,
        count(*)          over (partition by order_id)          as order_line_count

    from completed
)

select * from final
