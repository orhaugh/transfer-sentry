-- Payer and payee profiles, kept by the stream in event time and read in the
-- same step as the decision on each transfer.
--
-- For every transfer out of the bank, as of the transfer itself:
--   the payer's transfers in the last 28 days and their total, so the
--     average before this one;
--   when the payer first paid this payee, so whether the payee is new
--     (first paid within the last day);
--   how many payers paid this payee for the first time in the last day.
-- Each is an OVER window in event time. A window keeps one window's columns
-- and adds its own, so they read one another: per payer, then per payer and
-- payee, then per payee. Then the payee's place in the graph, as the decision
-- path reads it.
--
-- Decisions are published at least once, the moment they are made; a
-- restore decides again the transfers read after its checkpoint, the same
-- way. pipeline/audit.sql writes each decision exactly once into the profile
-- log.

CREATE TABLE transfers (
    transfer_id         VARCHAR,
    from_account_id     VARCHAR,
    from_account_number VARCHAR,
    to_account_id       VARCHAR,
    to_account_number   VARCHAR,
    amount              DOUBLE,
    ts                  BIGINT,
    sent_at             BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'transfers',
    group_id          = 'profiles',
    auto_offset_reset = 'earliest',
    event_time_column = 'ts',
    -- The stream is in time order, so each transfer's own watermark lets the
    -- windows act on it at once, and a copy a producer resends arrives behind
    -- the watermark, where the windows drop it rather than count it again.
    watermark_lag_ms  = '0'
);

-- The payee's place in the graph: the same question pipeline/decisions.sql
-- asks. An account the graph does not hold is a no-match.
CREATE TABLE payee_context (
    account_id       VARCHAR,
    payee_high_risk  BOOLEAN,
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
                        coalesce(owner.fraudFlag, false) AS owner_flagged,
                        size(one_hop) AS flagged_one_hop,
                        size(two_only) AS flagged_two_hops,
                        reduce(s = "", x IN (one_hop + two_only)[0..10] |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + x) AS evidence'
);

-- One decision per transfer, the reasons in order of weight:
--   payee_flagged   the payee's owner is flagged as fraud             hold
--   payee_linked    the owner shares an identifier with a flagged     hold
--                   customer
--   payee_fan_in    a new payee that three or more payers paid for    hold
--                   the first time in the last day
--   unusual_amount  a new payee, at least three times the payer's     review
--                   average over three or more transfers in 28 days
--   payee_high_risk the payee account is in a high-risk jurisdiction  review
--   payee_near      the owner is two shared identifiers from a        review
--                   flagged customer
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
    connector = 'kafka',
    format    = 'json',
    brokers   = 'kafka:9092',
    topic     = 'profile-decisions'
);

INSERT INTO profile_decisions
WITH payer AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY from_account_id ORDER BY ts
                          RANGE BETWEEN INTERVAL '28' DAY PRECEDING AND CURRENT ROW) AS payer_count,
           SUM(amount) OVER (PARTITION BY from_account_id ORDER BY ts
                             RANGE BETWEEN INTERVAL '28' DAY PRECEDING AND CURRENT ROW) AS payer_total
    FROM transfers
), pair AS (
    SELECT *,
           FIRST_VALUE(ts) OVER (PARTITION BY from_account_id, to_account_id ORDER BY ts)
               AS first_paid_at
    FROM payer
), marked AS (
    -- The payer's history before this transfer, and two marks: whether this
    -- is the payer's first payment to the payee, and whether the payee is new
    -- (first paid within the last day) and the amount at least three times the
    -- payer's average over three or more earlier transfers.
    SELECT transfer_id, from_account_id, from_account_number, to_account_id,
           to_account_number, amount, ts, sent_at, first_paid_at,
           payer_count - 1 AS payer_history,
           payer_total - amount AS payer_history_total,
           CASE WHEN ts = first_paid_at THEN 1 ELSE 0 END AS first_payment,
           CASE WHEN ts - first_paid_at <= 86400000 THEN 1 ELSE 0 END AS new_payee,
           CASE WHEN payer_count - 1 >= 3
                     AND amount * (payer_count - 1) >= 3 * (payer_total - amount)
                THEN 1 ELSE 0 END AS unusual
    FROM pair
), payee AS (
    SELECT *,
           SUM(first_payment) OVER (PARTITION BY to_account_id ORDER BY ts
                                    RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)
               AS payee_new_payers
    FROM marked
)
SELECT p.transfer_id,
       CASE WHEN c.owner_flagged OR c.flagged_one_hop > 0
                 OR (p.new_payee = 1 AND p.payee_new_payers >= 3) THEN 'hold'
            WHEN (p.new_payee = 1 AND p.unusual = 1) OR c.payee_high_risk
                 OR c.flagged_two_hops > 0 THEN 'review'
            ELSE 'approve' END,
       rtrim(CASE WHEN c.owner_flagged THEN 'payee_flagged,' ELSE '' END ||
             CASE WHEN c.flagged_one_hop > 0 THEN 'payee_linked,' ELSE '' END ||
             CASE WHEN p.new_payee = 1 AND p.payee_new_payers >= 3
                  THEN 'payee_fan_in,' ELSE '' END ||
             CASE WHEN p.new_payee = 1 AND p.unusual = 1 THEN 'unusual_amount,' ELSE '' END ||
             CASE WHEN c.payee_high_risk THEN 'payee_high_risk,' ELSE '' END ||
             CASE WHEN c.flagged_two_hops > 0 THEN 'payee_near,' ELSE '' END, ','),
       c.evidence,
       p.from_account_number,
       p.to_account_number,
       p.amount,
       p.payer_history,
       p.payer_history_total,
       p.first_paid_at,
       p.payee_new_payers,
       p.ts,
       p.sent_at,
       CAST(c.observed_at AS BIGINT)
FROM payee p
LEFT JOIN payee_context c ON p.to_account_id = c.account_id;
