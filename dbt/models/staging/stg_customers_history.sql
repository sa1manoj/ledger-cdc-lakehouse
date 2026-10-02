select
    version_id,
    customer_id,
    name,
    email,
    city,
    segment,
    cast(updated_at as timestamptz)     as effective_at,
    cast(valid_from as timestamptz)     as valid_from,
    cast(valid_to as timestamptz)       as valid_to,
    is_current,
    is_deleted,
    lsn
from {{ source('silver', 'customers') }}
