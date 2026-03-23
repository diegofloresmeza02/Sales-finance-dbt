/*
  assert_revenue_matches_gl
  ──────────────────────────
  Singular test. Returns rows (failures) when transactional revenue in
  fct_completed_order_items deviates from GL operating revenue by more than 1%.

  Run manually after month-end close (around the 5th business day).
  NOT included in standard CI — the GL is never fully posted mid-month.
  Tag: monthly

  Tolerance of 1%:
    Accounts for same-day shipment timing differences (orders shipped on the 31st
    may arrive in the ERP after the GL has been posted for the period).
    This is the source of the ~$12K/month remaining variance documented in the README.
    Anything above 1% = investigate. Likely causes:
      a) A batch of refunds processed through ZenDesk without a GL entry (DATA-214)
      b) Manual journal entry posted to wrong period
      c) Fivetran sync failure that truncated a day of orders

  Scope:
    2020-01-01 onward — pre-2020 GL data has encoding issues from the legacy ERP migration.
    is_order_revenue = true — excludes accruals, adjustments, intercompany entries.
*/

with sales_revenue as (
    select
        order_month                                             as month,
        round(sum(revenue), 2)                                  as sales_fact_revenue
    from {{ ref('fct_completed_order_items') }}
    where order_month >= '2020-01-01'
    group by 1
),

gl_revenue as (
    select
        entry_month                                             as month,
        -- Revenue accounts have credit normal balance → negate signed_amount
        round(-sum(net_amount), 2)                              as gl_revenue
    from {{ ref('fct_gl_activity_monthly') }}
    where account_subtype = 'operating_revenue'
      and is_order_revenue = true
      and entry_month >= '2020-01-01'
    group by 1
),

reconciliation as (
    select
        s.month,
        s.sales_fact_revenue,
        g.gl_revenue,
        round(s.sales_fact_revenue - coalesce(g.gl_revenue, 0), 2) as variance_usd,
        round(
            abs(s.sales_fact_revenue - coalesce(g.gl_revenue, 0))
            / nullif(s.sales_fact_revenue, 0),
        4)                                                      as variance_pct
    from sales_revenue s
    left join gl_revenue g on s.month = g.month
)

-- Return failures. Each row is a month that needs investigation.
select
    month,
    sales_fact_revenue,
    gl_revenue,
    variance_usd,
    variance_pct,
    case
        when gl_revenue is null then 'GL has no revenue entries for this month — sync issue?'
        when variance_pct > 0.05 then 'Variance >5% — likely a batch issue or ZenDesk refunds'
        else 'Variance between 1-5% — check for timing differences on month-end shipments'
    end as investigation_hint
from reconciliation
where variance_pct > 0.01
order by month desc
