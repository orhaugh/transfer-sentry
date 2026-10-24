-- The decision path: every transfer decided as it arrives, hold, review or
-- approve, with the reasons, from what the graph knows about where the money
-- is going.
--
-- It reads the bank's transactions directly, not the keeper's announcements:
-- a decision cannot wait a checkpoint for the graph to hold the transfer it
-- is about, and does not need to. Its context is the payee's place in the
-- customer and identity graph (who owns the account, which identifiers that
-- customer shares, with whom), which the keeper wrote when the customers and
-- accounts arrived, before any transfer.
--
-- Decisions are published at least once, the moment they are made: a
-- restore can decide a transfer again, and a consumer keeps the first
-- decision for each transaction id. pipeline/audit.sql writes each decision
-- exactly once into the decision log.

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
    sent_at             BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'transactions',
    group_id          = 'decisions',
    auto_offset_reset = 'earliest'
);

-- Where is the money going? The payee account's owner, and the customers
-- that owner shares an email address, phone number or street address with,
-- directly or through one other customer, counting those the bank has
-- flagged as fraud: the mule network a payment can be routed into. Plus the
-- account's own jurisdiction. Exactly one row per account; an account the
-- graph does not hold yet (an external account named by its first transfer)
-- is a no-match, decided on what the transfer itself says.
CREATE TABLE payee_context (
    account_id       VARCHAR,
    payee_high_risk  BOOLEAN,
    payee_kind       VARCHAR,
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
                 WITH a, owner, one_hop, [x IN two_hop WHERE NOT x IN one_hop] AS two_only
                 RETURN coalesce(a.highRiskJurisdiction, false) AS payee_high_risk,
                        a.internalExternal AS payee_kind,
                        coalesce(owner.fraudFlag, false) AS owner_flagged,
                        size(one_hop) AS flagged_one_hop,
                        size(two_only) AS flagged_two_hops,
                        reduce(s = "", x IN (one_hop + two_only)[0..10] |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + x) AS evidence'
);

-- One decision per transfer, the reasons in order of weight:
--   payee_flagged     the payee's owner is flagged as fraud            hold
--   payee_linked      the owner shares an identifier with a flagged    hold
--                     customer
--   payee_high_risk   the payee account is in a high-risk jurisdiction review
--   payee_near        the owner is two shared identifiers from a       review
--                     flagged customer
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
    connector = 'kafka',
    format    = 'json',
    brokers   = 'kafka:9092',
    topic     = 'decisions'
);

INSERT INTO decisions
SELECT t.transaction_id,
       CASE WHEN c.owner_flagged OR c.flagged_one_hop > 0 THEN 'hold'
            WHEN c.payee_high_risk OR c.flagged_two_hops > 0 THEN 'review'
            ELSE 'approve' END,
       rtrim(CASE WHEN c.owner_flagged THEN 'payee_flagged,' ELSE '' END ||
             CASE WHEN c.flagged_one_hop > 0 THEN 'payee_linked,' ELSE '' END ||
             CASE WHEN c.payee_high_risk THEN 'payee_high_risk,' ELSE '' END ||
             CASE WHEN c.flagged_two_hops > 0 THEN 'payee_near,' ELSE '' END, ','),
       c.evidence,
       t.from_account_number,
       t.to_account_number,
       t.amount,
       t.ts,
       t.sent_at,
       CAST(c.observed_at AS BIGINT)
FROM transactions t
LEFT JOIN payee_context c ON t.to_account_id = c.account_id;
