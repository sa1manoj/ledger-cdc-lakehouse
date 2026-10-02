-- Orders that exist in the source now, with the customer version that was
-- valid when the order was placed (point-in-time join).
--
-- Amounts are always the latest committed value, and revenue is attributed to
-- order_date, so a backdated correction restates the day the order belongs to.
with current_orders as (
    select *
    from {{ ref('stg_orders_history') }}
    where is_current and not is_deleted
),

first_customer_version as (
    -- fallback for orders placed before the pipeline's initial snapshot of the customer
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
    qualify row_number() over (partition by customer_id order by valid_from) = 1
)

select
    o.order_id,
    o.customer_id,
    coalesce(d.customer_sk, f.customer_sk)  as customer_sk,
    cast(o.order_ts as date)                as order_date,
    o.order_ts,
    o.status,
    o.amount,
    o.effective_at,
    o.valid_from                            as last_changed_at
from current_orders o
left join {{ ref('dim_customers') }} d
    on  d.customer_id = o.customer_id
    and o.order_ts >= d.valid_from
    and o.order_ts <  d.valid_to
left join first_customer_version f
    on f.customer_id = o.customer_id
