/*
  assert_no_cancelled_orders_in_fct
  ───────────────────────────────────
  Verifies the revenue recognition filter in int_order_line_economics is working.
  Any row in fct_completed_order_items with a non-completed status is a critical failure.
*/

select
    order_item_id,
    order_id,
    order_status,
    revenue
from {{ ref('fct_completed_order_items') }}
where order_status not in ('shipped', 'delivered')
