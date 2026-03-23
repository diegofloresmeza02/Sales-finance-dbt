/*
  agg_income_statement_monthly
  ─────────────────────────────
  Grain: one row per calendar month.

  The P&L in the format finance uses for the monthly business review:
  Revenue → Gross Profit → EBITDA → EBIT → EBT → Net Income

  Budget source: stg_budgets joined on entry_month + account_code = 'REV_TOTAL'.
  The budget table is loaded monthly by the finance team via a Fivetran Google Sheets
  connector. Any month without a budget row will show null budget fields — this is
  intentional (no zero-fill) because a zero budget is meaningfully different from
  a missing budget.

  Known limitation: budget currently only loaded at the total revenue level.
  Line-item budget (COGS, OPEX) is in the roadmap for Q3 2024.

  Revenue sign:
  ─────────────
  Revenue accounts have a credit normal balance in double-entry bookkeeping.
  In int_gl_classified, signed_amount flips credits to negative for revenue accounts.
  We reverse that here with -sum() to present revenue as a positive number in the P&L.
  Expense accounts retain their positive signed_amount (debits are positive).
  If this seems confusing, see the sign convention note in int_gl_classified.sql.
*/

with gl as (
    select * from {{ ref('fct_gl_activity_monthly') }}
),

budgets as (
    select * from {{ ref('stg_budgets') }}
),

-- Pivot account subtypes into P&L line items
pnl_lines as (
    select
        entry_month,

        -- Revenue accounts carry negative signed_amount — negate to show positive
        round(-sum(case when account_subtype = 'operating_revenue' then net_amount else 0 end), 2) as revenue,
        round( sum(case when account_subtype = 'cogs'              then net_amount else 0 end), 2) as cogs,
        round( sum(case when account_subtype = 'opex'              then net_amount else 0 end), 2) as opex,
        round( sum(case when account_subtype = 'depreciation'      then net_amount else 0 end), 2) as depreciation_amortization,
        round( sum(case when account_subtype = 'interest_expense'  then net_amount else 0 end), 2) as interest_expense,
        round( sum(case when account_subtype = 'income_tax'        then net_amount else 0 end), 2) as income_tax_expense

    from gl
    group by 1
),

-- Chain the P&L from top to bottom
pnl_cascaded as (
    select
        entry_month,
        revenue,
        cogs,
        revenue - cogs                                              as gross_profit,
        round((revenue - cogs) / nullif(revenue, 0), 4)            as gross_margin_pct,
        opex,
        revenue - cogs - opex                                       as ebitda,
        round((revenue - cogs - opex) / nullif(revenue, 0), 4)     as ebitda_margin_pct,
        depreciation_amortization,
        -- EBIT = EBITDA - D&A
        revenue - cogs - opex - depreciation_amortization           as ebit,
        round(
            (revenue - cogs - opex - depreciation_amortization) / nullif(revenue, 0),
        4)                                                          as ebit_margin_pct,
        interest_expense,
        revenue - cogs - opex - depreciation_amortization
            - interest_expense                                      as ebt,
        income_tax_expense,
        revenue - cogs - opex - depreciation_amortization
            - interest_expense - income_tax_expense                 as net_income,
        round(
            (revenue - cogs - opex - depreciation_amortization
                - interest_expense - income_tax_expense)
            / nullif(revenue, 0),
        4)                                                          as net_margin_pct

    from pnl_lines
),

-- Join budget at total revenue level only (COGS/OPEX budget not yet available)
with_budget as (
    select
        p.*,
        b.budgeted_amount                                           as budgeted_revenue,
        round(
            (p.revenue - b.budgeted_amount) / nullif(b.budgeted_amount, 0),
        4)                                                          as revenue_vs_budget_pct,
        -- Absolute variance in dollars — finance prefers this over % for monthly review
        round(p.revenue - coalesce(b.budgeted_amount, 0), 2)       as revenue_vs_budget_usd

    from pnl_cascaded p
    left join budgets b
        on  p.entry_month      = b.budget_month
        and b.account_code     = 'REV_TOTAL'
)

select * from with_budget
order by entry_month
