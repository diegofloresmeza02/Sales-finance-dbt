/*
  agg_revenue_performance_monthly
  ────────────────────────────────
  Grain: one row per month × sales_channel × region × category.
  Primary source for the commercial dashboard.

  ──────────────────────────────────────────────────────────────────────────────
  Known gap: no date spine → YoY comparisons break on zero-revenue months
  ──────────────────────────────────────────────────────────────────────────────
  LAG(12) returns NULL when the prior year month has no row for that
  channel/region/category combination. This is wrong and we know it —
  a NULL YoY figure is indistinguishable from missing data in most BI tools.
  The fix is a cross-join with a date spine to fill zero-revenue months.
  Blocked on defining the canonical list of active channel/region/category
  combinations (new channels are introduced mid-year without announcement).
  Current workaround: dashboard filters out NULL YoY values with a tooltip.
  This should be fixed, not worked around.

  ──────────────────────────────────────────────────────────────────────────────
  new_customers definition
  ──────────────────────────────────────────────────────────────────────────────
  A customer is "new" in a given month if that month = their first_order_date month
  in dim_customer_360. Definition is consistent with how the dim model defines
  first purchase. If you change the definition in dim_customer_360, it changes here too.
  Do not add a separate first_purchase flag to the fact table.

  ──────────────────────────────────────────────────────────────────────────────
  avg_order_value calculation
  ──────────────────────────────────────────────────────────────────────────────
  Uses sum(order_total_revenue) / count(distinct order_id), not avg(revenue).
  avg(revenue) would give the average LINE value, inflated for multi-item orders.
  A $500 order with 5 × $100 items should count as one $500 order, not five $100 items.
*/

with monthly_base as (
    select
        order_month,
        sales_channel,
        region,
        category,

        count(distinct order_id)                            as total_orders,
        count(distinct customer_id)                         as total_customers,
        sum(quantity)                                       as units_sold,
        round(sum(revenue), 2)                              as net_revenue,
        round(sum(cogs), 2)                                 as total_cogs,
        round(sum(gross_profit), 2)                         as gross_profit,
        round(avg(gross_margin_pct), 4)                     as avg_gross_margin_pct,
        round(
            sum(order_total_revenue) / nullif(count(distinct order_id), 0),
        2)                                                  as avg_order_value,
        round(avg(effective_discount_pct), 4)               as avg_effective_discount_pct

    from {{ ref('fct_completed_order_items') }}
    group by 1, 2, 3, 4
),

new_customers as (
    -- Joining to dim avoids duplicating the "first order" definition here
    select
        date_trunc('month', first_order_date)               as cohort_month,
        count(distinct customer_id)                         as new_customers
    from {{ ref('dim_customer_360') }}
    where first_order_date is not null
    group by 1
),

with_yoy as (
    select
        m.*,
        coalesce(nc.new_customers, 0)                       as new_customers,
        m.total_customers - coalesce(nc.new_customers, 0)   as returning_customers,

        lag(m.net_revenue) over (
            partition by m.sales_channel, m.region, m.category
            order by m.order_month
        )                                                   as prev_month_revenue,

        -- LAG(12) is null if prior year month has no row. See known gap above.
        lag(m.net_revenue, 12) over (
            partition by m.sales_channel, m.region, m.category
            order by m.order_month
        )                                                   as same_month_prior_year_revenue

    from monthly_base m
    left join new_customers nc on m.order_month = nc.cohort_month
),

final as (
    select
        order_month,
        sales_channel,
        region,
        category,
        total_orders,
        total_customers,
        new_customers,
        returning_customers,
        units_sold,
        net_revenue,
        total_cogs,
        gross_profit,
        avg_gross_margin_pct,
        avg_order_value,
        avg_effective_discount_pct,

        round(
            (net_revenue - prev_month_revenue) / nullif(prev_month_revenue, 0),
        4)                                                  as mom_revenue_growth_pct,

        round(
            (net_revenue - same_month_prior_year_revenue) / nullif(same_month_prior_year_revenue, 0),
        4)                                                  as yoy_revenue_growth_pct,  -- null if no prior year row

        sum(net_revenue) over (
            partition by year(order_month), sales_channel, region, category
            order by order_month
            rows between unbounded preceding and current row
        )                                                   as ytd_revenue

    from with_yoy
)

select * from final
