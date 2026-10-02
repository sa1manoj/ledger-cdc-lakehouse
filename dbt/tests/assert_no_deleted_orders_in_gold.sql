-- orders whose latest silver version is a delete marker must not appear in gold
select f.order_id
from {{ ref('fct_orders') }} f
join {{ ref('stg_orders_history') }} h
  on h.order_id = f.order_id and h.is_current and h.is_deleted
