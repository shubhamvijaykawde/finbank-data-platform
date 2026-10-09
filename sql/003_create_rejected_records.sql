-- FinBank Phase 4 rejected-record quarantine table.
-- Run while connected to the finbank database.

CREATE TABLE IF NOT EXISTS raw.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    transaction_id TEXT,
    rejection_code TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL
        CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL
        CHECK (kafka_offset >= 0),
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_rejected_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_rejected_records_code
    ON raw.rejected_records(rejection_code);

CREATE INDEX IF NOT EXISTS idx_rejected_records_event_id
    ON raw.rejected_records(event_id);

CREATE INDEX IF NOT EXISTS idx_rejected_records_rejected_at
    ON raw.rejected_records(rejected_at);
