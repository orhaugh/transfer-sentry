-- The case router: every alert, into the case store and onto the graph.
--
-- It reads `alerts` with read_committed (the default), so an alert reaches
-- the case store only after the detectors' checkpoint committed it. Both
-- statements take one reader (shared_reader), so the case store and the graph
-- are cut at the same barriers. The case store is written exactly once, in
-- PostgreSQL transactions prepared at each barrier and committed when the
-- checkpoint completes. The graph is written at least once, as its table
-- says, and keyed by alert id, so a replayed alert finds its node and adds
-- nothing.

CREATE TABLE alerts (
    alert_id    VARCHAR,
    kind        VARCHAR,
    severity    VARCHAR,
    at          BIGINT,
    subject     VARCHAR,
    evidence    VARCHAR,
    summary     VARCHAR,
    observed_at TIMESTAMP(3)
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'alerts',
    group_id          = 'case-router',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'at',
    watermark_lag_ms  = '1000'
);

-- Times as epoch milliseconds; postgres/schema.sql derives the readable
-- timestamps from them.
CREATE TABLE cases (
    alert_id       VARCHAR,
    kind           VARCHAR,
    severity       VARCHAR,
    at_ms          BIGINT,
    subject        VARCHAR,
    evidence       VARCHAR,
    summary        VARCHAR,
    observed_at_ms BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 user=sentry password=sentry dbname=sentry',
    "table"            = 'public.cases',
    delivery_guarantee = 'exactly_once'
);

-- An alert node, linked to what raised it (the transaction that closed the
-- ring, the new customer, or the payment into a high-risk jurisdiction) and
-- to its evidence (the ring's transactions, the earlier customers, or the
-- transfers the trace counted). Created with its relationships in one
-- transaction, once, as the keeper creates transactions.
CREATE TABLE alert_graph (
    alert_id VARCHAR,
    kind     VARCHAR,
    severity VARCHAR,
    at       BIGINT,
    subject  VARCHAR,
    evidence VARCHAR,
    summary  VARCHAR
) WITH (
    connector          = 'neo4j',
    delivery_guarantee = 'at_least_once',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    batch_size         = '200',
    flush_interval_ms  = '200',
    statement = 'UNWIND $rows AS row
                 WITH row.alert_id AS id, collect(row)[0] AS row
                 MERGE (al:Alert {alertID: id})
                   ON CREATE SET al.kind = row.kind, al.severity = row.severity,
                                 al.at = datetime({epochMillis: row.at}),
                                 al.summary = row.summary, al._created = true
                 WITH al, row
                 CALL (al, row) {
                   WITH al, row WHERE al._created AND row.kind IN ["transaction_ring", "high_risk_trace"]
                   MATCH (t:Transaction {transactionID: row.subject})
                   CREATE (al)-[:RAISED_ON]->(t)
                   WITH al, row
                   UNWIND split(coalesce(row.evidence, ""), ",") AS tid
                   MATCH (e:Transaction {transactionID: tid})
                   CREATE (al)-[:EVIDENCE]->(e)
                 }
                 CALL (al, row) {
                   WITH al, row WHERE al._created AND row.kind = "shared_identity"
                   MATCH (c:Customer {customerID: row.subject})
                   CREATE (al)-[:RAISED_ON]->(c)
                   WITH al, row
                   UNWIND split(coalesce(row.evidence, ""), ",") AS cid
                   MATCH (e:Customer {customerID: cid})
                   CREATE (al)-[:EVIDENCE]->(e)
                 }
                 REMOVE al._created'
);

EXECUTE STATEMENT SET BEGIN
  INSERT INTO cases
    SELECT alert_id, kind, severity, at, subject, evidence, summary,
           CAST(observed_at AS BIGINT)
    FROM alerts;
  INSERT INTO alert_graph
    SELECT alert_id, kind, severity, at, subject, evidence, summary FROM alerts;
END;
