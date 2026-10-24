-- The graph keeper: the bank's three event streams, written into the fraud
-- graph, and each onboarding and transaction announced once the graph holds
-- it.
--
-- The graph is the model of Neo4j's fraud demo (see NOTICE), so the demo's
-- own dashboard queries run on it unchanged:
--
--   (:Customer)-[:USES_EMAIL]->(:Email)
--   (:Customer)-[:USES_PHONE]->(:PhoneNumber)
--   (:Customer)-[:RESIDES_AT]->(:Address)-[:LOCATED_IN]->(:Jurisdiction)
--   (:Customer)-[:HAS_ACCOUNT]->(:Account:Internal|External[:HighRiskJurisdiction])
--   (:Account)-[:SENT]->(:Transaction)-[:RECEIVED]->(:Account)
--
-- One job, one statement set, checkpointed as one. Writes to Neo4j are at
-- least once, and each graph table says so: after a restore, batches the
-- graph already holds are written again, so every statement below is safe to
-- repeat (the uniqueness constraints in neo4j/schema.cypher are part of
-- that).
--
-- The announcements are written exactly once, in Kafka transactions that
-- commit only when a checkpoint completes, and a checkpoint completes only
-- after every graph write before its barrier has committed. customers and
-- transactions are each read by two statements, the graph write and the
-- announcement, and declare shared_reader so that both statements take one
-- reader and are cut at the same barriers. A reader of
-- committed-transactions with read_committed (the default) therefore never
-- sees a transaction before the graph holds it, or before the graph holds
-- everything that arrived ahead of it.

CREATE TABLE customers (
    customer_id       VARCHAR,
    name              VARCHAR,
    date_of_birth     VARCHAR,
    email_id          VARCHAR,
    email             VARCHAR,
    phone_id          VARCHAR,
    phone             VARCHAR,
    address_id        VARCHAR,
    street            VARCHAR,
    city              VARCHAR,
    state             VARCHAR,
    country           VARCHAR,
    jurisdiction_id   VARCHAR,
    jurisdiction      VARCHAR,
    jurisdiction_risk VARCHAR,
    fraud_flag        BOOLEAN,
    ts                BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'customers',
    group_id          = 'graph-keeper',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'ts',
    watermark_lag_ms  = '1000'
);

CREATE TABLE accounts (
    account_id             VARCHAR,
    account_number         VARCHAR,
    account_type           VARCHAR,
    status                 VARCHAR,
    internal               BOOLEAN,
    high_risk_jurisdiction BOOLEAN,
    customer_id            VARCHAR
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'accounts',
    group_id          = 'graph-keeper',
    auto_offset_reset = 'earliest'
);

CREATE TABLE transactions (
    transaction_id      VARCHAR,
    from_account_id     VARCHAR,
    from_account_number VARCHAR,
    to_account_id       VARCHAR,
    to_account_number   VARCHAR,
    amount              DOUBLE,
    type                VARCHAR,
    status              VARCHAR,
    ts                  BIGINT,
    -- When the feeder sent the transfer. Declared so every record matches the
    -- table, which keeps the JSON decode on its columnar path; the graph
    -- does not store it.
    sent_at             BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'transactions',
    group_id          = 'graph-keeper',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'ts',
    watermark_lag_ms  = '1000'
);

-- An onboarding. A batch can hold the same event twice (a producer that
-- retried), so it is reduced to one row per customer first. Setting _lock
-- takes the customer's write lock, so a concurrent or replayed write of the
-- same customer waits, then finds the relationships this one created and
-- MERGE adds none. The account stream may have created the customer already,
-- with no properties, so the properties are SET rather than set on create.
-- An address is shared between customers, so its jurisdiction is linked
-- under the address's own lock in the same way.
CREATE TABLE customer_graph (
    customer_id       VARCHAR,
    name              VARCHAR,
    date_of_birth     VARCHAR,
    email_id          VARCHAR,
    email             VARCHAR,
    phone_id          VARCHAR,
    phone             VARCHAR,
    address_id        VARCHAR,
    street            VARCHAR,
    city              VARCHAR,
    state             VARCHAR,
    country           VARCHAR,
    jurisdiction_id   VARCHAR,
    jurisdiction      VARCHAR,
    jurisdiction_risk VARCHAR,
    fraud_flag        BOOLEAN,
    ts                BIGINT
) WITH (
    connector          = 'neo4j',
    delivery_guarantee = 'at_least_once',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    batch_size         = '200',
    flush_interval_ms  = '200',
    statement = 'UNWIND $rows AS row
                 WITH row.customer_id AS id, collect(row)[0] AS row
                 MERGE (c:Customer {customerID: id})
                 SET c._lock = true
                 SET c.name = row.name,
                     c.dateOfBirth = date(row.date_of_birth),
                     c.fraudFlag = row.fraud_flag,
                     c.onboardedAt = datetime({epochMillis: row.ts})
                 MERGE (e:Email {emailID: row.email_id})
                   ON CREATE SET e.emailAddress = row.email
                 MERGE (p:PhoneNumber {phoneNumberID: row.phone_id})
                   ON CREATE SET p.number = row.phone
                 MERGE (ad:Address {addressID: row.address_id})
                   ON CREATE SET ad.street = row.street, ad.city = row.city,
                                 ad.state = row.state, ad.country = row.country
                 MERGE (c)-[:USES_EMAIL]->(e)
                 MERGE (c)-[:USES_PHONE]->(p)
                 MERGE (c)-[:RESIDES_AT]->(ad)
                 REMOVE c._lock
                 WITH ad, row
                 MERGE (j:Jurisdiction {jurisdictionID: row.jurisdiction_id})
                   ON CREATE SET j.name = row.jurisdiction, j.riskLevel = row.jurisdiction_risk
                 SET ad._lock = true
                 MERGE (ad)-[:LOCATED_IN]->(j)
                 REMOVE ad._lock'
);

-- An account, and its owner when it has one. The demo labels each account
-- Internal or External and marks those in a high-risk jurisdiction; the
-- labels follow the same properties here. A transaction can arrive before
-- its accounts' events, and creates the account with its number only.
CREATE TABLE account_graph (
    account_id             VARCHAR,
    account_number         VARCHAR,
    account_type           VARCHAR,
    status                 VARCHAR,
    internal               BOOLEAN,
    high_risk_jurisdiction BOOLEAN,
    customer_id            VARCHAR
) WITH (
    connector          = 'neo4j',
    delivery_guarantee = 'at_least_once',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    batch_size         = '200',
    flush_interval_ms  = '200',
    statement = 'UNWIND $rows AS row
                 WITH row.account_id AS id, collect(row)[0] AS row
                 MERGE (a:Account {accountID: id})
                 SET a._lock = true
                 SET a.accountNumber = row.account_number,
                     a.accountType = row.account_type,
                     a.status = row.status,
                     a.internalExternal = CASE WHEN row.internal THEN "Internal" ELSE "External" END,
                     a.highRiskJurisdiction = row.high_risk_jurisdiction
                 SET a:$(CASE WHEN row.internal THEN "Internal" ELSE "External" END)
                 WITH a, row
                 CALL (a, row) {
                   WITH a, row WHERE row.high_risk_jurisdiction
                   SET a:HighRiskJurisdiction
                 }
                 CALL (a, row) {
                   WITH a, row WHERE row.customer_id IS NOT NULL
                   MERGE (c:Customer {customerID: row.customer_id})
                   MERGE (c)-[:HAS_ACCOUNT]->(a)
                 }
                 REMOVE a._lock'
);

-- A transaction, the demo's event node, keyed by its id. Its relationships
-- are created only by the transaction that creates the node, so a replayed
-- or concurrent copy finds the node and creates nothing; the batch is
-- reduced to one row per transaction first, because two copies in one batch
-- would both see the node as just created.
CREATE TABLE transaction_graph (
    transaction_id      VARCHAR,
    from_account_id     VARCHAR,
    from_account_number VARCHAR,
    to_account_id       VARCHAR,
    to_account_number   VARCHAR,
    amount              DOUBLE,
    type                VARCHAR,
    status              VARCHAR,
    ts                  BIGINT
) WITH (
    connector          = 'neo4j',
    delivery_guarantee = 'at_least_once',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    batch_size         = '200',
    flush_interval_ms  = '200',
    statement = 'UNWIND $rows AS row
                 WITH row.transaction_id AS id, collect(row)[0] AS row
                 MERGE (t:Transaction {transactionID: id})
                   ON CREATE SET t.date = datetime({epochMillis: row.ts}),
                                 t.amount = row.amount, t.type = row.type,
                                 t.status = row.status, t._created = true
                 WITH t, row
                 CALL (t, row) {
                   WITH t, row WHERE t._created
                   MERGE (s:Account {accountID: row.from_account_id})
                     ON CREATE SET s.accountNumber = row.from_account_number
                   MERGE (r:Account {accountID: row.to_account_id})
                     ON CREATE SET r.accountNumber = row.to_account_number
                   CREATE (s)-[:SENT]->(t)
                   CREATE (t)-[:RECEIVED]->(r)
                 }
                 REMOVE t._created'
);

-- The announcements carry every record the keeper reads, a producer's retry
-- included, which arrives behind the stream's event time. Replay suppression
-- classifies records by event time, so it is off here; the detectors drop a
-- repeated announcement by its id.
CREATE TABLE committed_customers (
    customer_id VARCHAR,
    fraud_flag  BOOLEAN,
    ts          BIGINT
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'committed-customers',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'graph-keeper-customers',
    replay_suppression = 'false'
);

CREATE TABLE committed_transactions (
    transaction_id  VARCHAR,
    from_account_id VARCHAR,
    to_account_id   VARCHAR,
    amount          DOUBLE,
    ts              BIGINT
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'committed-transactions',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'graph-keeper-transactions',
    replay_suppression = 'false'
);

EXECUTE STATEMENT SET BEGIN
  INSERT INTO customer_graph SELECT * FROM customers;
  INSERT INTO account_graph SELECT * FROM accounts;
  INSERT INTO transaction_graph
    SELECT transaction_id, from_account_id, from_account_number, to_account_id,
           to_account_number, amount, type, status, ts
    FROM transactions;
  INSERT INTO committed_customers SELECT customer_id, fraud_flag, ts FROM customers;
  INSERT INTO committed_transactions
    SELECT transaction_id, from_account_id, to_account_id, amount, ts FROM transactions;
END;
