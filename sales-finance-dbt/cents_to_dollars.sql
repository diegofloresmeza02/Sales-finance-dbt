-- Macro para convertir centavos a dólares.
-- Lo usamos cuando el ERP guarda montos como enteros en centavos.

{% macro cents_to_dollars(column_name, scale=2) %}
    round({{ column_name }} / 100.0, {{ scale }})
{% endmacro %}
