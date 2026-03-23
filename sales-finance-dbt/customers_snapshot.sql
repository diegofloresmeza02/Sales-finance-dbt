-- Snapshot de clientes (SCD Tipo 2).
-- Captura cambios en segmento, país o ciudad a lo largo del tiempo.
-- Así podemos saber el segmento del cliente al momento de cada compra,
-- no solo el segmento actual.

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_segment', 'country', 'city'],
        invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
