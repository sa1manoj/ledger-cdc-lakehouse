-- Example point-in-time query: what did the orders table look like at this moment?
-- Compile with: dbt compile --select orders_as_of
{{ as_of(ref('stg_orders_history'), "timestamptz '2025-01-01 12:00:00+00'") }}
