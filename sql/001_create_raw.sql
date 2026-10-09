-- FinBank Phase 3 raw ingestion schema.
-- Run while connected to the finbank database as the finbank application user.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    date_of_birth DATE NOT NULL,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    registration_date DATE NOT NULL,
    customer_segment TEXT NOT NULL
        CHECK (customer_segment IN ('standard', 'premium', 'private'))
);

CREATE TABLE IF NOT EXISTS raw.accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL
        REFERENCES raw.customers(customer_id),
    account_type TEXT NOT NULL
        CHECK (account_type IN ('checking', 'savings')),
    currency CHAR(3) NOT NULL,
    account_open_date DATE NOT NULL,
    initial_balance NUMERIC(18, 2) NOT NULL
        CHECK (initial_balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_accounts_customer_id
    ON raw.accounts(customer_id);

CREATE TABLE IF NOT EXISTS raw.merchants (
    merchant_id TEXT PRIMARY KEY,
    merchant_name TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    country TEXT NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.transaction_events (
    event_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL
        CHECK (schema_version = 1),
    event_timestamp TIMESTAMPTZ NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL
        REFERENCES raw.customers(customer_id),
    account_id TEXT NOT NULL
        REFERENCES raw.accounts(account_id),
    merchant_id TEXT NOT NULL
        REFERENCES raw.merchants(merchant_id),
    amount NUMERIC(18, 2) NOT NULL
        CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    transaction_type TEXT NOT NULL,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('completed', 'pending', 'failed')),
    payload JSONB NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL
        CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL
        CHECK (kafka_offset >= 0),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_transaction_event_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_transaction_events_customer_id
    ON raw.transaction_events(customer_id);

CREATE INDEX IF NOT EXISTS idx_transaction_events_account_id
    ON raw.transaction_events(account_id);

CREATE INDEX IF NOT EXISTS idx_transaction_events_event_timestamp
    ON raw.transaction_events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_transaction_events_status
    ON raw.transaction_events(status);
