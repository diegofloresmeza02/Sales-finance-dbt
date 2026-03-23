/*
  int_order_line_economics
  ─────────────────────────
  Grain: one row per order_item.
  Materialized: ephemeral (compiled inline — never stored in Snowflake).

  Why ephemeral: this join (order_items × orders × products) touches ~50M rows.
  Both fct_completed_order_items and dim_customer_360 need the result.
  Materializing as a table would add ~14 min to the run and double storage cost
  for a dataset that's never queried directly by anything outside dbt.
  Tradeoff: compile time is slightly longer. Acceptable.

  ──────────────────────────────────────────
  Business rules encoded in this model
  ──────────────────────────────────────────

  Revenue recognition gate (is_revenue_recognized):
    Only 'shipped' and 'delivered' orders count as recognized revenue.
    Defined via var('completed_order_statuses') in dbt_project.yml so the
    definition is consistent across all models. If finance changes the policy,
    update the var — not this model.

  Refunds excluded:
    Status = 'refunded' is excluded from revenue recognition intentionally.
    The GL books refunds as contra-revenue entries (account_subtype = 'refund').
    Including refunds in the sales fact would cause double-counting when
    analysts join both layers. Known tradeoff: ~$40K/month of refunds that
    exist in the ERP but have no GL entry (ZenDesk refunds, see README) are
    excluded from both layers. This overstates net revenue slightly.

  COGS uses standard cost, not actual purchase cost:
    Actual COGS requires a purchase_orders feed from the ERP that doesn't
    exist yet. Standard cost is in the product catalog and is updated quarterly.
    For ~15% of SKUs, standard cost is stale by 1–3 months at any given time.
    Gross margin figures should be treated as directional for those SKUs.

  ──────────────────────────────────────────
  Join behavior — read before changing
  ──────────────────────────────────────────

  order_items → orders: LEFT JOIN.
    Orphaned order_items (items with no parent order) exist in the source —
    approximately 300/month, cause unknown (suspected ERP race condition on
    order creation). They arrive with null order fields and get filtered out
    downstream by is_revenue_recognized = true. Switching to INNER JOIN would
    silently drop them. LEFT JOIN makes them visible for investigation.

  order_items → products: LEFT JOIN.
    Products are soft-deleted in the ERP (is_active = false, cost_price = null).
    An INNER JOIN would silently drop historical orders against archived products.
    We use COALESCE(cost_price, 0) and expose has_missing_cost as a flag.
    As of last check: ~120 products in this state, affecting ~8K order lines/month.
*/

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

economics as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,

        o.order_date,
        o.shipped_date,
        o.delivered_date,
        date_trunc('month', o.order_date)                       as order_month,
        date_trunc('year',  o.order_date)                       as order_year,

        o.order_status,
        o.sales_channel,
        o.region,

        -- Product attributes reflect current catalog state, not state at time of sale.
        -- If a product was recategorized after purchase, historical orders will
        -- reflect the new category. This is a known limitation — no SCD on products.
        -- Impact: reclassifications affect historical category mix reports.
        p.sku,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price                                            as standard_cost,
        p.list_price,

        oi.quantity,
        oi.unit_price,
        oi.discount_pct,

        -- Effective discount: how far unit_price deviates from list_price.
        -- Useful for detecting channel-specific pricing pressure or unauthorized discounting.
        -- Returns null if list_price is null (product archived without a price).
        round((p.list_price - oi.unit_price) / nullif(p.list_price, 0), 4) as effective_discount_pct,

        oi.net_amount                                           as revenue,

        -- COGS: quantity × standard cost.
        -- COALESCE(cost_price, 0): when product is archived, COGS defaults to zero.
        -- This overstates margin for those lines. has_missing_cost flag = true for these.
        round(oi.quantity * coalesce(p.cost_price, 0), 2)      as cogs,
        (p.cost_price is null)                                  as has_missing_cost,

        round(oi.net_amount - (oi.quantity * coalesce(p.cost_price, 0)), 2) as gross_profit,
        round(
            (oi.net_amount - (oi.quantity * coalesce(p.cost_price, 0)))
            / nullif(oi.net_amount, 0),
        4)                                                      as gross_margin_pct,

        -- Revenue recognition gate. Centralizing this expression avoids
        -- having the status list in multiple models.
        o.order_status in ({{ "'" + var('completed_order_statuses') | join("','") + "'" }})
                                                                as is_revenue_recognized

    from order_items oi
    left join orders  o on oi.order_id   = o.order_id
    left join products p on oi.product_id = p.product_id
)

select * from economics
