-- The case store: one row per alert, written exactly once by the case router.
-- alert_id carries no uniqueness constraint on purpose: a second copy of an
-- alert would land as a second row, where the run's check counts it, rather
-- than fail the router's transaction over and over.
--
-- clink writes times as epoch milliseconds; the readable timestamps are
-- derived here.
CREATE TABLE IF NOT EXISTS cases (
    alert_id       text        NOT NULL,
    kind           text        NOT NULL,
    severity       text        NOT NULL,
    at_ms          bigint      NOT NULL,
    subject        text        NOT NULL,
    evidence       text,
    summary        text,
    observed_at_ms bigint,
    at             timestamptz GENERATED ALWAYS AS (to_timestamp(at_ms / 1000.0)) STORED,
    observed_at    timestamptz GENERATED ALWAYS AS (to_timestamp(observed_at_ms / 1000.0)) STORED,
    opened_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cases_alert_id ON cases (alert_id);
