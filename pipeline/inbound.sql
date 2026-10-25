-- The inbound screen: the receiving side of a payment, at the two moments a
-- bank can act on money passing through a mule's account. When a credit
-- lands: does the account look like a mule? When the money leaves again: is
-- it going straight back out?
--
-- It reads the payments stream (tools/payment_flows.py): credits into and
-- debits out of the bank's own accounts, minute by minute. Each account's
-- recent behaviour is kept by clink, per account, in event time; who the
-- account's owner is connected to comes from the graph the keeper wrote when
-- the customers and accounts arrived. Decisions are published at least once,
-- the moment they are made; pipeline/audit.sql writes each exactly once into
-- the inbound log.

CREATE TABLE payments (
    payment_id          VARCHAR,
    account_id          VARCHAR,
    account_number      VARCHAR,
    counterparty_id     VARCHAR,
    counterparty_number VARCHAR,
    direction           VARCHAR,
    amount              DOUBLE,
    ts                  BIGINT,
    sent_at             BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'payments',
    group_id          = 'inbound',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'ts',
    -- The stream is in time order, so a payment's own watermark lets both
    -- screens act on it at once: the credit window counts it, and the pattern
    -- takes it in event-time order. A copy a producer resends arrives behind
    -- the watermark, where both screens drop it rather than count or match it
    -- again.
    watermark_lag_ms  = '0'
);

-- Who the receiving account's owner is connected to: flagged as fraud, or
-- sharing an email address, phone number or street address with a flagged
-- customer, directly or through one other customer. The decision path asks
-- the same of a payee.
CREATE TABLE receiver_context (
    account_id       VARCHAR,
    owner_flagged    BOOLEAN,
    flagged_one_hop  BIGINT,
    flagged_two_hops BIGINT,
    evidence         VARCHAR,
    matched          BOOLEAN,
    observed_at      TIMESTAMP(3)
) WITH (
    connector          = 'lookup',
    function           = 'neo4j',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    request_timeout_ms = '10000',
    server_timeout_s   = '5',
    max_in_flight      = '256',
    max_concurrent_requests = '8',
    matched_column     = 'matched',
    observed_at_column = 'observed_at',
    statement = 'MATCH (a:Account {accountID: $key})
                 OPTIONAL MATCH (owner:Customer)-[:HAS_ACCOUNT]->(a)
                 WITH a, owner,
                      COLLECT {
                        MATCH (owner)-[:USES_EMAIL|USES_PHONE|RESIDES_AT]->()
                              <-[:USES_EMAIL|USES_PHONE|RESIDES_AT]-(o:Customer)
                        WHERE o <> owner AND o.fraudFlag
                        RETURN DISTINCT o.customerID AS id ORDER BY id
                      } AS one_hop,
                      COLLECT {
                        MATCH (owner)-[:USES_EMAIL|USES_PHONE|RESIDES_AT]->()
                              <-[:USES_EMAIL|USES_PHONE|RESIDES_AT]-(m:Customer)
                              -[:USES_EMAIL|USES_PHONE|RESIDES_AT]->()
                              <-[:USES_EMAIL|USES_PHONE|RESIDES_AT]-(o:Customer)
                        WHERE m <> owner AND o <> owner AND o <> m AND o.fraudFlag
                        RETURN DISTINCT o.customerID AS id ORDER BY id
                      } AS two_hop
                 WITH owner, one_hop, [x IN two_hop WHERE NOT x IN one_hop] AS two_only
                 RETURN coalesce(owner.fraudFlag, false) AS owner_flagged,
                        size(one_hop) AS flagged_one_hop,
                        size(two_only) AS flagged_two_hops,
                        reduce(s = "", x IN (one_hop + two_only)[0..10] |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + x) AS evidence'
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
    connector = 'kafka',
    format    = 'json',
    brokers   = 'kafka:9092',
    topic     = 'inbound-decisions'
);

EXECUTE STATEMENT SET BEGIN
  -- When a credit lands, the reasons in order of weight:
  --   receiver_flagged  the account's owner is flagged as fraud           hold
  --   receiver_linked   the owner shares an identifier with a flagged     hold
  --                     customer
  --   fan_in            the fourth credit or more into the account        hold
  --                     within the hour
  --   receiver_near     the owner is two shared identifiers from a        review
  --                     flagged customer
  INSERT INTO inbound_decisions
  WITH landed AS (
      SELECT *,
             COUNT(*) OVER (PARTITION BY account_id ORDER BY ts
                            RANGE BETWEEN 3600000 PRECEDING AND CURRENT ROW) AS credits_hour
      FROM payments
      WHERE direction = 'credit'
  )
  SELECT l.payment_id,
         l.account_number,
         'credit',
         CASE WHEN r.owner_flagged OR r.flagged_one_hop > 0 OR l.credits_hour >= 4 THEN 'hold'
              WHEN r.flagged_two_hops > 0 THEN 'review'
              ELSE 'credit' END,
         rtrim(CASE WHEN r.owner_flagged THEN 'receiver_flagged,' ELSE '' END ||
               CASE WHEN r.flagged_one_hop > 0 THEN 'receiver_linked,' ELSE '' END ||
               CASE WHEN l.credits_hour >= 4 THEN 'fan_in,' ELSE '' END ||
               CASE WHEN r.flagged_two_hops > 0 THEN 'receiver_near,' ELSE '' END, ','),
         r.evidence,
         l.amount,
         l.ts,
         l.credits_hour,
         CAST(r.observed_at AS BIGINT)
  FROM landed l
  LEFT JOIN receiver_context r ON l.account_id = r.account_id;

  -- When the money leaves: a debit straight after a credit, of 80% to 100% of
  -- it, within half an hour, holds the onward payment. The credit is the
  -- evidence.
  INSERT INTO inbound_decisions
  SELECT onward_id, account_number, 'debit', 'hold', 'pass_through', credit_id,
         onward_amount, onward_ts, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
  FROM payments MATCH_RECOGNIZE (
      PARTITION BY account_id
      ORDER BY ts
      MEASURES FIRST(c.payment_id) AS credit_id,
               LAST(d.payment_id) AS onward_id,
               LAST(d.account_number) AS account_number,
               LAST(d.amount) AS onward_amount,
               LAST(d.ts) AS onward_ts
      PATTERN (c d)
      DEFINE c AS direction = 'credit',
             d AS direction = 'debit' AND amount <= PREV(amount)
                  AND amount >= PREV(amount) * 0.8 AND ts <= PREV(ts) + 1800000
  );
END;
