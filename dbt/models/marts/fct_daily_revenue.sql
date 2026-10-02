select
    order_date,
    count(*)                                                        as orders,
    count(*) filter (where status = 'delivered')                    as delivered_orders,
    count(*) filter (where status = 'cancelled')                    as cancelled_orders,
    coalesce(sum(amount) filter (where status <> 'cancelled'), 0)   as gross_revenue,
    coalesce(sum(amount) filter (where status = 'delivered'), 0)    as delivered_revenue
from {{ ref('fct_orders') }}
group by order_date
