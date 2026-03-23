/*
  stg_gl_entries
  ───────────────
  Standardizes GL entries from the accounting system.

  Two things worth knowing about this source before you touch it:

  1. entry_date is the ACCOUNTING date (the period the entry belongs to),
     not the posting date. An entry posted on November 8th for October
     will have entry_date = October 31st. This is correct accounting behavior
     but means "entries loaded this week" and "entries for this period" are
     different filters. Don't confuse them.

  2. Manual journal entries (adjustments, accruals, reclassifications) arrive
     with a 5–7 day lag. If you run this model on November 3rd, the October
     GL will be ~15% incomplete. The `_loaded_at` column lets you see when
     each entry actually arrived in the warehouse. The `entry_date` tells
     you which period it belongs to. Both matter.

  Sign convention:
  ─────────────────
  Raw source: amount is always positive. entry_type is 'debit' or 'credit'.
  This model computes signed_amount using the raw entry_type.
  Downstream models use signed_amount — don't reconstruct the sign logic there.

  Revenue accounts have a CREDIT normal balance in double-entry bookkeeping.
  A credit to a revenue account increases revenue.
  signed_amount for a revenue credit = negative (credit).
  int_gl_classified and agg_income_statement_monthly negate revenue accounts
  to present them as positive numbers. Do not negate here.

  Excluded rows:
  ───────────────
  - amount <= 0: shouldn't exist per accounting rules, but occasionally
    appears in the source as a data entry error. Excluded rather than
    attempting to fix — these require manual review by accounting.
  - entry_id null: should never happen; if it does, the GL integration is broken.
*/

with source as (
    select * from {{ source('raw_finance', 'gl_entries') }}
),

cleaned as (
    select
        entry_id::varchar                           as entry_id,
        account_code::varchar                       as account_code,
        entry_date::date                            as entry_date,
        date_trunc('month', entry_date::date)       as entry_month,

        lower(trim(entry_type))                     as entry_type,
        round(amount::numeric, 2)                   as amount,

        case
            when lower(trim(entry_type)) = 'debit'  then  round(amount::numeric, 2)
            when lower(trim(entry_type)) = 'credit' then -round(amount::numeric, 2)
            -- Unexpected entry_type: surface as null rather than silently zeroing.
            -- Will fail not_null test on signed_amount and alert the team.
            else null
        end                                         as signed_amount,

        trim(description)                           as description,

        -- reference_id links GL entries back to source transactions.
        -- For revenue entries: contains the order_id from the ERP.
        -- For adjustments: contains the journal entry reference (e.g., 'JE-2024-1182').
        -- For accruals: often null. Don't assume null = no order.
        reference_id::varchar                       as reference_id,

        lower(trim(currency))                       as currency,
        _loaded_at

    from source
    where entry_id is not null
      and amount > 0  -- see exclusion note above
)

select * from cleaned
