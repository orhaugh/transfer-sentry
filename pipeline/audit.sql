-- The decision log: every decision the decision path published, written
-- exactly once into PostgreSQL.
--
-- The decision path publishes at least once, so a restore can publish a
-- decision again, and a transfer a producer resent is decided again. The log
-- reads each decision once: its columns are what the transfer and the graph
-- decided, the same however often it is published, so SELECT DISTINCT drops
-- a copy (remembered for an hour of processing time). When the transfer was
-- sent and when its context was read are left out, since a second copy has
-- its own. Each row then lands in a PostgreSQL transaction prepared at a
-- checkpoint barrier and committed when the checkpoint completes.

CREATE TABLE decisions (
    transaction_id VARCHAR,
    decision       VARCHAR,
    reasons        VARCHAR,
    evidence       VARCHAR,
    from_account   VARCHAR,
    to_account     VARCHAR,
    amount         DOUBLE,
    ts             BIGINT,
    sent_at        BIGINT,
    context_at     BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'decisions',
    group_id          = 'decision-log',
    auto_offset_reset = 'earliest',
    state_ttl         = '1h',
    state_ttl_domain  = 'processing_time'
);

CREATE TABLE decision_log (
    transaction_id VARCHAR,
    decision       VARCHAR,
    reasons        VARCHAR,
    evidence       VARCHAR,
    from_account   VARCHAR,
    to_account     VARCHAR,
    amount         DOUBLE,
    ts_ms          BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 user=sentry password=sentry dbname=sentry',
    "table"            = 'public.decision_log',
    delivery_guarantee = 'exactly_once'
);

INSERT INTO decision_log
SELECT DISTINCT transaction_id, decision, reasons, evidence, from_account, to_account,
       amount, ts
FROM decisions;
