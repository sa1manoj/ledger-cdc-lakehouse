-- One row per version of an order, as recorded in silver.
select
    version_id,
    order_id,
    customer_id,
    status,
    cast(amount as decimal(12, 2))      as amount,
    cast(order_ts as timestamptz)       as order_ts,
    cast(updated_at as timestamptz)     as effective_at,   -- business time set by the source app
    cast(valid_from as timestamptz)     as valid_from,     -- system time: when the source committed it
    cast(valid_to as timestamptz)       as valid_to,
    is_current,
    is_deleted,
    lsn
from {{ source('silver', 'orders') }}
