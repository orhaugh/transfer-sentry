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

-- The decision log: one row per transfer, written exactly once by
-- pipeline/audit.sql. Again no uniqueness constraint, so a second copy would
-- show in the run's check rather than fail the job.
CREATE TABLE IF NOT EXISTS decision_log (
    transaction_id text   NOT NULL,
    decision       text   NOT NULL,
    reasons        text,
    evidence       text,
    from_account   text,
    to_account     text,
    amount         double precision,
    ts_ms          bigint
);
CREATE INDEX IF NOT EXISTS decision_log_transaction_id ON decision_log (transaction_id);

-- The inbound log: one row per inbound decision, a credit decided or an
-- onward payment held, written exactly once by pipeline/audit.sql.
CREATE TABLE IF NOT EXISTS inbound_log (
    payment_id  text   NOT NULL,
    account     text,
    direction   text   NOT NULL,
    decision    text   NOT NULL,
    reasons     text,
    evidence    text,
    amount      double precision,
    ts_ms       bigint
);
CREATE INDEX IF NOT EXISTS inbound_log_payment_id ON inbound_log (payment_id);

-- The profile log: one row per transfer decided from its payer's and payee's
-- profiles, written exactly once by pipeline/audit.sql.
CREATE TABLE IF NOT EXISTS profile_log (
    transfer_id  text   NOT NULL,
    decision     text   NOT NULL,
    reasons      text,
    evidence     text,
    from_account text,
    to_account   text,
    amount       double precision,
    ts_ms        bigint
);
CREATE INDEX IF NOT EXISTS profile_log_transfer_id ON profile_log (transfer_id);
