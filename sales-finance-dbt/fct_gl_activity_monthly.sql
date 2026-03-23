/*
  fct_gl_activity_monthly
  ────────────────────────
  Grain: one row per account_code × entry_month.

  Monthly roll-up of GL activity used as the base for income statement,
  balance sheet, and budget variance reporting.

  Sign convention (set once in int_gl_classified, respected here):
    revenue accounts  → positive = money in
    expense accounts  → positive = money out
  This means gross_profit = revenue - cogs works without sign flips downstream.

  is_income_statement filter:
  ────────────────────────────
  This model only includes P&L accounts (income statement = true).
  Balance sheet accounts are excluded — if we need them later, build
  a separate fct_balance_sheet_monthly. Mixing them here would make
  the budget variance logic more complex for no current benefit.
*/

with gl as (
    select *
    from {{ ref('int_gl_classified') }}
    where is_income_statement = true
),

monthly as (
    select
        entry_month,
        account_code,
        account_name,
        account_type,
        account_subtype,
        normal_balance,
        currency,

        round(sum(signed_amount), 2)                        as net_amount,
        round(sum(case when entry_type = 'debit'  then amount else 0 end), 2) as total_debits,
        round(sum(case when entry_type = 'credit' then amount else 0 end), 2) as total_credits,
        count(*)                                            as entry_count,

        -- Flag months where net_amount is unusually large (>3x prior month).
        -- Not used downstream yet but surfaced for the finance team to review.
        -- Thresholds should be adjusted per account once we have 12+ months of history.
        false                                               as is_anomaly_flagged  -- placeholder

    from gl
    group by 1, 2, 3, 4, 5, 6, 7
),

with_ytd as (
    select
        m.*,

        -- YTD by account within fiscal year
        sum(net_amount) over (
            partition by account_code, year(entry_month)
            order by entry_month
            rows between unbounded preceding and current row
        )                                                   as ytd_net_amount,

        -- Prior year same month — used for YoY variance in income statement
        lag(net_amount, 12) over (
            partition by account_code
            order by entry_month
        )                                                   as prior_year_same_month_amount,

        round(
            (net_amount - lag(net_amount, 12) over (
                partition by account_code order by entry_month
            )) / nullif(abs(lag(net_amount, 12) over (
                partition by account_code order by entry_month
            )), 0),
        4)                                                  as yoy_change_pct

    from monthly
)

select * from with_ytd
