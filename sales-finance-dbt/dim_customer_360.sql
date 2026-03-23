/*
  dim_customer_360
  ─────────────────
  Grain: one row per customer. ~2.1M rows including prospects.

  "360" = you can answer any customer question here without joining back to fct.
  The tradeoff: lifetime_revenue is stored here AND computable from fct.
  They can drift if the fact table logic changes and this model isn't rebuilt.
  Mitigated by running both in the same dbt invocation (fct runs first via ref()).

  ──────────────────────────────────────────────────────────────────────────────
  Why this model is a full table refresh (not incremental)
  ──────────────────────────────────────────────────────────────────────────────
  days_since_last_order changes every calendar day for every customer with a
  historical purchase. An incremental model would need to update every such row
  daily regardless, eliminating the benefit. The right fix is to move
  days_since_last_order to the BI layer. It's here because the Tableau workbook
  that reads this table has a 200ms query budget that couldn't accommodate the
  computation. Tech debt accepted, documented here.

  ──────────────────────────────────────────────────────────────────────────────
  Customer tier thresholds
  ──────────────────────────────────────────────────────────────────────────────
  Set by the commercial team in Q1 2024. Reviewed quarterly.
  Based on lifetime_revenue, not order count. A single $15K B2B order
  outranks 30 × $200 consumer orders. This is intentional — the company
  prioritizes enterprise revenue retention over consumer frequency.

  Edge case: customers who placed orders that were later fully refunded may show
  lifetime_revenue = 0 (since refunds reduce the GL, not the sales fact) or
  lifetime_revenue > 0 (if only some orders were refunded). This means a customer
  could be classified as 'bronze' despite never having kept a purchase.
  The commercial team is aware. Fixing it properly requires linking refunds to
  customer IDs, which requires the ZenDesk refund feed (see README: DATA-214).

  ──────────────────────────────────────────────────────────────────────────────
  is_churn_risk definition
  ──────────────────────────────────────────────────────────────────────────────
  Purchased in last 12 months but not in last 90 days.
  This is not a predictive model — it's a heuristic flag set by the commercial
  team based on their sales cycle. Average B2B repurchase cycle is ~75 days,
  so 90 days = ~1 missed cycle. Not validated with a churn model.
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

lifetime_metrics as (
    select
        customer_id,
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,
        count(distinct order_id)                            as total_completed_orders,
        sum(quantity)                                       as total_units_purchased,
        round(sum(revenue), 2)                              as lifetime_revenue,
        round(sum(gross_profit), 2)                         as lifetime_gross_profit,
        -- AOV at order level to avoid inflation from multi-line orders
        round(avg(order_total_revenue), 2)                  as avg_order_value,
        round(
            (count(distinct order_id) - 1.0) / nullif(count(distinct order_id), 0),
        4)                                                  as repurchase_rate

    from {{ ref('fct_completed_order_items') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.customer_segment,
        c.country,
        c.city,
        c.registration_date,

        m.first_order_date,
        m.last_order_date,
        m.total_completed_orders,
        m.total_units_purchased,
        m.lifetime_revenue,
        m.lifetime_gross_profit,
        m.avg_order_value,
        m.repurchase_rate,

        -- Recalculated nightly. Null for prospects (no completed orders).
        -- Should move to BI layer — see model header for context.
        datediff('day', m.last_order_date, current_date())  as days_since_last_order,

        case
            when m.lifetime_revenue >= 10000 then 'platinum'
            when m.lifetime_revenue >= 5000  then 'gold'
            when m.lifetime_revenue >= 1000  then 'silver'
            when m.lifetime_revenue > 0      then 'bronze'
            else 'prospect'
        end                                                 as customer_tier,

        -- Heuristic churn flag. Not a predictive model. See header note.
        (
            m.last_order_date >= dateadd('month', -12, current_date())
            and m.last_order_date <  dateadd('day',   -90, current_date())
        )                                                   as is_churn_risk

    from customers c
    left join lifetime_metrics m on c.customer_id = m.customer_id
)

select * from final
