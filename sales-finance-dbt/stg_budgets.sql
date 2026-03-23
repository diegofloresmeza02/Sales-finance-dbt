-- Presupuestos mensuales por cuenta contable.

with source as (
    select * from {{ source('raw_finance', 'budgets') }}
),

renamed as (
    select
        budget_id::varchar                          as budget_id,
        account_code::varchar                       as account_code,
        fiscal_year::int                            as fiscal_year,
        month_number::int                           as month_number,

        -- Construimos la fecha del mes para joins más fáciles
        {{ make_date('fiscal_year', 'month_number') }}          as budget_month,

        round(budgeted_amount::numeric, 2)          as budgeted_amount,
        lower(trim(currency))                       as currency,
        _loaded_at

    from source
    where budget_id is not null
)

select * from renamed
