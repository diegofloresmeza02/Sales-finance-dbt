-- Chart of accounts — static reference table, updated quarterly by finance.
-- No deduplication needed: account_code is a natural unique key enforced in the source.

with source as (
    select * from {{ source('raw_finance', 'chart_of_accounts') }}
),

renamed as (
    select
        account_code::varchar                       as account_code,
        trim(account_name)                          as account_name,
        lower(trim(account_type))                   as account_type,
        lower(trim(account_subtype))                as account_subtype,
        lower(trim(normal_balance))                 as normal_balance,
        account_subtype not in ('current_asset', 'fixed_asset', 'liability', 'equity')
                                                    as is_income_statement,
        _loaded_at

    from source
    where account_code is not null
)

select * from renamed
