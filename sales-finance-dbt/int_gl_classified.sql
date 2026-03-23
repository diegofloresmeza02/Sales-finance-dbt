/*
  int_gl_classified
  ──────────────────
  Grain: one row per GL entry.

  Enriches raw GL entries with account classification from the chart of accounts.
  The main value here is the sign convention normalization: in double-entry
  bookkeeping, revenue accounts have a credit normal balance, so a credit entry
  increases revenue but the raw amount is positive in both cases. We resolve
  this once here so downstream models never need to think about it.

  Sign convention used throughout this project:
    - Revenue accounts: positive = money in
    - Expense accounts: positive = money out
    signed_amount handles this via the normal_balance field on the account.
*/

with gl as (
    select * from {{ ref('stg_gl_entries') }}
),

accounts as (
    select * from {{ ref('stg_chart_of_accounts') }}
),

classified as (
    select
        gl.entry_id,
        gl.entry_date,
        gl.entry_month,
        gl.entry_type,
        gl.amount,
        gl.signed_amount,
        gl.description,
        gl.reference_id,   -- FK to order_id for revenue entries; null for internal accruals
        gl.currency,

        gl.account_code,
        a.account_name,
        a.account_type,
        a.account_subtype,
        a.is_income_statement,
        a.normal_balance,

        -- Convenience flag: does this entry correspond to a completed sale?
        -- Used to reconcile GL revenue against fct_completed_order_items.
        -- Null reference_id = accrual/adjustment entry, not tied to an order.
        (a.account_subtype = 'operating_revenue' and gl.reference_id is not null) as is_order_revenue

    from gl
    -- Inner join is correct here: GL entries without a chart of accounts match
    -- should never exist and if they do, we want dbt test failures not silent nulls.
    -- Monitored via not_null test on account_name in schema.yml.
    inner join accounts a on gl.account_code = a.account_code
)

select * from classified
