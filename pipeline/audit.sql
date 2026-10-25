-- The decision logs: every decision the decision path, the inbound screen and
-- the profile decisions published, written exactly once into PostgreSQL.
--
-- Both publish at least once, so a restore can publish a decision again, and
-- a transfer a producer resent is decided again. Each log reads each decision
-- once: its columns are what the payment and the graph decided, the same
-- however often it is published, so SELECT DISTINCT drops a copy (remembered
-- for an hour of processing time). When the payment was sent and when its
-- context was read are left out, since a second copy has its own. Each row
-- then lands in a PostgreSQL transaction prepared at a checkpoint barrier and
-- committed when the checkpoint completes.

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

CREATE TABLE inbound_decisions (
    payment_id   VARCHAR,
    account      VARCHAR,
    direction    VARCHAR,
    decision     VARCHAR,
    reasons      VARCHAR,
    evidence     VARCHAR,
    amount       DOUBLE,
    ts           BIGINT,
    credits_hour BIGINT,
    context_at   BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'inbound-decisions',
    group_id          = 'inbound-log',
    auto_offset_reset = 'earliest',
    state_ttl         = '1h',
    state_ttl_domain  = 'processing_time'
);

CREATE TABLE inbound_log (
    payment_id VARCHAR,
    account    VARCHAR,
    direction  VARCHAR,
    decision   VARCHAR,
    reasons    VARCHAR,
    evidence   VARCHAR,
    amount     DOUBLE,
    ts_ms      BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 user=sentry password=sentry dbname=sentry',
    "table"            = 'public.inbound_log',
    delivery_guarantee = 'exactly_once'
);

CREATE TABLE profile_decisions (
    transfer_id      VARCHAR,
    decision         VARCHAR,
    reasons          VARCHAR,
    evidence         VARCHAR,
    from_account     VARCHAR,
    to_account       VARCHAR,
    amount           DOUBLE,
    payer_history    BIGINT,
    payer_history_total DOUBLE,
    first_paid_at    BIGINT,
    payee_new_payers BIGINT,
    ts               BIGINT,
    sent_at          BIGINT,
    context_at       BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'profile-decisions',
    group_id          = 'profile-log',
    auto_offset_reset = 'earliest',
    state_ttl         = '1h',
    state_ttl_domain  = 'processing_time'
);

CREATE TABLE profile_log (
    transfer_id  VARCHAR,
    decision     VARCHAR,
    reasons      VARCHAR,
    evidence     VARCHAR,
    from_account VARCHAR,
    to_account   VARCHAR,
    amount       DOUBLE,
    ts_ms        BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 user=sentry password=sentry dbname=sentry',
    "table"            = 'public.profile_log',
    delivery_guarantee = 'exactly_once'
);

EXECUTE STATEMENT SET BEGIN
  INSERT INTO decision_log
  SELECT DISTINCT transaction_id, decision, reasons, evidence, from_account, to_account,
         amount, ts
  FROM decisions;
  INSERT INTO inbound_log
  SELECT DISTINCT payment_id, account, direction, decision, reasons, evidence, amount, ts
  FROM inbound_decisions;
  INSERT INTO profile_log
  SELECT DISTINCT transfer_id, decision, reasons, evidence, from_account, to_account,
         amount, ts
  FROM profile_decisions;
END;
