-- Type 2 customer dimension. Delete markers are dropped; a deleted customer's
-- last live version keeps its closing valid_to.
select
    version_id                                          as customer_sk,
    customer_id,
    name,
    email,
    city,
    segment,
    valid_from,
    coalesce(valid_to, timestamptz '9999-12-31 00:00:00+00') as valid_to,
    is_current
from {{ ref('stg_customers_history') }}
where not is_deleted
