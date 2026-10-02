-- Source OLTP schema for the Ledger demo.
-- REPLICA IDENTITY FULL makes Postgres write the full "before" row to the WAL,
-- so Debezium delete events carry every column (needed for delete propagation).

CREATE TABLE IF NOT EXISTS customers (
    customer_id  BIGINT PRIMARY KEY,
    name         TEXT        NOT NULL,
    email        TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    segment      TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT        NOT NULL REFERENCES customers (customer_id),
    status       TEXT          NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    order_ts     TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_ts ON orders (order_ts);
