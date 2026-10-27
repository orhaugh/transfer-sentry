-- The detectors: every transaction and onboarding the graph holds, checked
-- against the graph around it, and what they find written to `alerts`
-- exactly once.
--
-- They read the graph keeper's announcements, not the bank's streams. An
-- announcement is committed only after the graph holds its event and
-- everything that arrived before it, so each read below sees the event it is
-- about. A read sees the graph as it is when the read runs, which can include
-- later events too; each statement therefore looks only at what came before
-- its event (earlier transfers, customers who onboarded earlier), and the
-- same event finds the same evidence whenever it is read, after a restore
-- included.

-- An event can be announced twice: a producer upstream that retried a batch
-- sends its events again, and the keeper announces each record it reads.
-- The detectors read each event once (SELECT DISTINCT below) and remember
-- it for an hour of processing time, longer than any retry waits.
CREATE TABLE committed_transactions (
    transaction_id  VARCHAR,
    from_account_id VARCHAR,
    to_account_id   VARCHAR,
    amount          DOUBLE,
    ts              BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'committed-transactions',
    group_id          = 'detectors',
    auto_offset_reset = 'earliest',
    event_time_column = 'ts',
    watermark_lag_ms  = '1000',
    state_ttl         = '1h',
    state_ttl_domain  = 'processing_time'
);

-- Onboardings stop long before transactions do. idle_timeout_ms lets this
-- stream's watermark stop holding back the alerts' once it has gone quiet.
CREATE TABLE committed_customers (
    customer_id VARCHAR,
    fraud_flag  BOOLEAN,
    ts          BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'committed-customers',
    group_id          = 'detectors',
    auto_offset_reset = 'earliest',
    event_time_column = 'ts',
    watermark_lag_ms  = '1000',
    idle_timeout_ms   = '5000',
    state_ttl         = '1h',
    state_ttl_domain  = 'processing_time'
);

-- Does this transfer close a ring? The demo's "Rings" query (see NOTICE),
-- anchored on the transfer instead of run over the whole graph: money leaves
-- an account and comes back to it through 3 to 16 distinct accounts, each
-- transfer later than the one before and 80% to 100% of its amount. The
-- transfer that closes a ring is its latest, so every other transfer in the
-- ring is already in the graph when it is read. One row when it closes at
-- least one ring (the shortest, then the one with the lowest transaction
-- ids, and how many there are), none otherwise.
CREATE TABLE ring_context (
    transaction_id    VARCHAR,
    ring_size         BIGINT,
    rings             BIGINT,
    entry_account     VARCHAR,
    ring_accounts     VARCHAR,
    ring_transactions VARCHAR,
    first_at          BIGINT,
    first_amount      DOUBLE,
    last_amount       DOUBLE,
    observed_at       TIMESTAMP(3)
) WITH (
    connector          = 'lookup',
    function           = 'neo4j',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    request_timeout_ms = '10000',
    server_timeout_s   = '5',
    observed_at_column = 'observed_at',
    statement = 'MATCH (last_tx:Transaction {transactionID: $key})-[:RECEIVED]->(a:Account)
                 MATCH (a)-[:SENT]->(first_tx:Transaction)
                   ((tx_i:Transaction)-[:RECEIVED]->(a_i:Account)-[:SENT]->(tx_j:Transaction)
                     WHERE tx_i.date < tx_j.date
                       AND tx_i.amount >= tx_j.amount >= 0.80 * tx_i.amount){2,15}
                   (last_tx)
                 WHERE COUNT { WITH a, a_i UNWIND [a] + a_i AS b RETURN DISTINCT b } = size([a] + a_i)
                 WITH a, [a] + a_i AS ring, [first_tx] + tx_j AS txs
                 ORDER BY size(ring), [t IN txs | t.transactionID]
                 WITH a, collect({ring: ring, txs: txs}) AS found
                 WITH a, found[0] AS r, size(found) AS rings
                 RETURN size(r.ring) AS ring_size,
                        rings,
                        a.accountNumber AS entry_account,
                        reduce(s = "", x IN r.ring |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + x.accountNumber) AS ring_accounts,
                        reduce(s = "", t IN r.txs |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + t.transactionID) AS ring_transactions,
                        r.txs[0].date.epochMillis AS first_at,
                        r.txs[0].amount AS first_amount,
                        last(r.txs).amount AS last_amount'
);

-- Does this new customer share an email address, phone number or street
-- address with customers who were already customers when they onboarded?
-- The demo's "Customers Sharing PII" query (see NOTICE), asked at onboarding
-- about one customer. One row when they share something, none otherwise.
CREATE TABLE identity_context (
    customer_id        VARCHAR,
    shared_customers   BIGINT,
    shared_with_fraud  BIGINT,
    shared_kinds       VARCHAR,
    evidence_customers VARCHAR,
    observed_at        TIMESTAMP(3)
) WITH (
    connector          = 'lookup',
    function           = 'neo4j',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    request_timeout_ms = '10000',
    server_timeout_s   = '5',
    observed_at_column = 'observed_at',
    statement = 'MATCH (c:Customer {customerID: $key})
                 WITH c, COLLECT {
                   MATCH (c)-[r:USES_EMAIL|USES_PHONE|RESIDES_AT]->()<-[:USES_EMAIL|USES_PHONE|RESIDES_AT]-(o:Customer)
                   WHERE o.onboardedAt < c.onboardedAt
                      OR (o.onboardedAt = c.onboardedAt AND o.customerID < c.customerID)
                   RETURN {customer: o, kind: type(r)} AS m
                 } AS matches
                 WHERE size(matches) > 0
                 WITH c, matches,
                      COLLECT { UNWIND matches AS m RETURN DISTINCT m.customer AS o ORDER BY o.customerID } AS earlier,
                      COLLECT { UNWIND matches AS m RETURN DISTINCT m.kind AS k ORDER BY k } AS kinds
                 RETURN size(earlier) AS shared_customers,
                        size([o IN earlier WHERE o.fraudFlag]) AS shared_with_fraud,
                        reduce(s = "", k IN kinds |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + k) AS shared_kinds,
                        reduce(s = "", o IN earlier[0..10] |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + o.customerID) AS evidence_customers'
);

-- Does this payment take money into a high-risk jurisdiction that reached
-- the payer through other accounts first? The demo's "High Risk Accounts"
-- query (see NOTICE), anchored on a payment into an account in a high-risk
-- jurisdiction: what the payer has paid that account since the page's start
-- date, 22 August 2024, traced back through distinct accounts between which
-- 90% to 110% of it moved, to an account no one paid as much. Only transfers
-- up to this payment count (by date, then id), so money that reached an
-- account after it paid is not followed, and the same payment finds the same
-- trace whenever it is read. One row when the trace has at least one hop
-- (the longest, then the lowest by account numbers, and how many there
-- are), none otherwise. The evidence is every transfer the trace counted,
-- hop by hop, then the payer's earlier payments to the same account.
CREATE TABLE trace_context (
    transaction_id    VARCHAR,
    hops              BIGINT,
    traces            BIGINT,
    origin_account    VARCHAR,
    high_risk_account VARCHAR,
    trace_accounts    VARCHAR,
    origin_high_risk  BOOLEAN,
    flagged_on_chain  BIGINT,
    evidence          VARCHAR,
    observed_at       TIMESTAMP(3)
) WITH (
    connector          = 'lookup',
    function           = 'neo4j',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    request_timeout_ms = '10000',
    server_timeout_s   = '5',
    observed_at_column = 'observed_at',
    statement = 'MATCH (l:Account)-[:SENT]->(x:Transaction {transactionID: $key})-[:RECEIVED]->(h:HighRiskJurisdiction)
                 WITH l, h, x, datetime("2024-08-22") AS since
                 WHERE x.date >= since
                 WITH l, h, x, since, COLLECT {
                        MATCH (l)-[:SENT]->(p:Transaction)-[:RECEIVED]->(h)
                        WHERE p.date >= since
                          AND (p.date < x.date OR (p.date = x.date AND p.transactionID <= x.transactionID))
                        RETURN p ORDER BY p.date, p.transactionID
                      } AS paid
                 WITH l, h, x, since, paid, reduce(acc = 0.0, p IN paid | acc + p.amount) AS total
                 MATCH (first:Account)
                       ((a1:Account)-[:SENT]->(t:Transaction)-[:RECEIVED]->(a2:Account)
                         WHERE t.date >= since
                           AND (t.date < x.date OR (t.date = x.date AND t.transactionID <= x.transactionID))
                           AND 0.9 * total <= COLLECT {
                                 MATCH (a1)-[:SENT]->(m:Transaction)-[:RECEIVED]->(a2)
                                 WHERE m.date >= since
                                   AND (m.date < x.date OR (m.date = x.date AND m.transactionID <= x.transactionID))
                                 RETURN sum(m.amount)
                               }[0] <= 1.1 * total)+
                       (l)
                 WHERE COUNT { WITH first, a2 UNWIND [first] + a2 AS b RETURN DISTINCT b } = size([first] + a2)
                   AND NOT EXISTS {
                         MATCH (b:Account)-[:SENT]->(m:Transaction)-[:RECEIVED]->(first)
                         WHERE m.date >= since
                           AND (m.date < x.date OR (m.date = x.date AND m.transactionID <= x.transactionID))
                         WITH b, sum(m.amount) AS moved
                         WHERE 0.9 * total <= moved <= 1.1 * total
                       }
                 WITH DISTINCT h, x, since, paid, [first] + a2 AS chain
                 ORDER BY size(chain) DESC, [a IN chain | a.accountNumber]
                 WITH h, x, since, paid, collect(chain) AS chains
                 WITH h, x, since, paid, chains[0] AS chain, size(chains) AS traces
                 RETURN size(chain) - 1 AS hops,
                        traces,
                        chain[0].accountNumber AS origin_account,
                        h.accountNumber AS high_risk_account,
                        reduce(out = "", a IN chain |
                               out + CASE WHEN out = "" THEN "" ELSE "," END + a.accountNumber) AS trace_accounts,
                        coalesce(chain[0].highRiskJurisdiction, false) AS origin_high_risk,
                        size([a IN chain WHERE EXISTS {
                                MATCH (c:Customer)-[:HAS_ACCOUNT]->(a) WHERE c.fraudFlag }]) AS flagged_on_chain,
                        reduce(out = "", tid IN COLLECT {
                                 UNWIND range(0, size(chain) - 2) AS i
                                 WITH chain[i] AS a1, chain[i + 1] AS a2, i, since, x
                                 MATCH (a1)-[:SENT]->(m:Transaction)-[:RECEIVED]->(a2)
                                 WHERE m.date >= since
                                   AND (m.date < x.date OR (m.date = x.date AND m.transactionID <= x.transactionID))
                                 RETURN m.transactionID ORDER BY i, m.date, m.transactionID
                               } + [p IN paid WHERE p <> x | p.transactionID] |
                               out + CASE WHEN out = "" THEN "" ELSE "," END + tid) AS evidence'
);

-- What the detectors found, exactly once. Each alert is keyed by the event
-- that raised it (ring:<transaction id>, identity:<customer id>,
-- trace:<transaction id>), so whatever stores it downstream can tell a
-- second copy from a second alert.
--   kind      transaction_ring, shared_identity or high_risk_trace
--   severity  high for a ring, or for a customer sharing identifiers with one
--             the bank flags as fraud; medium for a shared email address or
--             phone number; low for a shared street address alone. A trace is
--             high when the money started in a high-risk jurisdiction or
--             passed through an account of a customer the bank flags as
--             fraud, medium otherwise
--   subject   the transaction that closed the ring, the new customer, or the
--             payment into the high-risk jurisdiction
--   evidence  the ring's transactions in order, the earlier customers, or
--             the transfers the trace counted
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
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'alerts',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'detectors-alerts'
);

INSERT INTO alerts
WITH transactions AS (SELECT DISTINCT transaction_id, ts FROM committed_transactions),
     onboardings  AS (SELECT DISTINCT customer_id, ts FROM committed_customers)
SELECT 'ring:' || t.transaction_id AS alert_id,
       'transaction_ring' AS kind,
       'high' AS severity,
       t.ts AS at,
       t.transaction_id AS subject,
       r.ring_transactions AS evidence,
       'closes a ring of ' || CAST(r.ring_size AS VARCHAR) || ' accounts entered at ' ||
         r.entry_account || ': ' || r.ring_accounts AS summary,
       r.observed_at AS observed_at
FROM transactions t
JOIN ring_context r ON t.transaction_id = r.transaction_id
UNION ALL
SELECT 'identity:' || c.customer_id,
       'shared_identity',
       CASE WHEN i.shared_with_fraud > 0 THEN 'high'
            WHEN i.shared_kinds LIKE '%USES_EMAIL%' OR i.shared_kinds LIKE '%USES_PHONE%' THEN 'medium'
            ELSE 'low' END,
       c.ts,
       c.customer_id,
       i.evidence_customers,
       'shares ' || i.shared_kinds || ' with ' || CAST(i.shared_customers AS VARCHAR) ||
         ' earlier customers, ' || CAST(i.shared_with_fraud AS VARCHAR) || ' flagged as fraud',
       i.observed_at
FROM onboardings c
JOIN identity_context i ON c.customer_id = i.customer_id
UNION ALL
SELECT 'trace:' || t.transaction_id,
       'high_risk_trace',
       CASE WHEN h.origin_high_risk OR h.flagged_on_chain > 0 THEN 'high' ELSE 'medium' END,
       t.ts,
       t.transaction_id,
       h.evidence,
       'pays high-risk account ' || h.high_risk_account || ' what came from ' ||
         h.origin_account || ': ' || h.trace_accounts,
       h.observed_at
FROM transactions t
JOIN trace_context h ON t.transaction_id = h.transaction_id;
