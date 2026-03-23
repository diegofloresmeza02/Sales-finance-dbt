-- Macro que genera una espina de fechas entre dos puntos.
-- Útil para asegurarnos de no tener huecos en reportes mensuales
-- cuando no hay transacciones en un mes determinado.

{% macro generate_date_spine(start_date, end_date, datepart='day') %}

    with date_spine as (
        {{ dbt_utils.date_spine(
            datepart=datepart,
            start_date="cast('" ~ start_date ~ "' as date)",
            end_date="cast('" ~ end_date ~ "' as date)"
        ) }}
    )

    select
        date_day                                    as date,
        date_trunc('month', date_day)               as month,
        date_trunc('quarter', date_day)             as quarter,
        date_trunc('year', date_day)                as year,
        dayofweek(date_day)                         as day_of_week,
        weekofyear(date_day)                        as week_of_year,
        dayofyear(date_day)                         as day_of_year,
        case dayofweek(date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end                                         as day_name,
        case
            when dayofweek(date_day) in (0, 6) then false
            else true
        end                                         as is_weekday

    from date_spine

{% endmacro %}
