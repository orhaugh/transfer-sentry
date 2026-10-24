// The graph model of Neo4j's fraud demo, with the constraints the pipeline's
// statements rely on. Labels, relationship types and property names are the
// demo's own, so its dashboard queries run unchanged on the graph clink keeps.
//
// The constraints are what make a replayed batch, or two transactions writing
// one id at once, find the existing node instead of creating a second; their
// indexes are what the detectors' lookups seek on.

CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customerID IS UNIQUE;
CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.accountID IS UNIQUE;
CREATE CONSTRAINT transaction_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.transactionID IS UNIQUE;
CREATE CONSTRAINT email_id IF NOT EXISTS FOR (e:Email) REQUIRE e.emailID IS UNIQUE;
CREATE CONSTRAINT phone_id IF NOT EXISTS FOR (p:PhoneNumber) REQUIRE p.phoneNumberID IS UNIQUE;
CREATE CONSTRAINT address_id IF NOT EXISTS FOR (a:Address) REQUIRE a.addressID IS UNIQUE;
CREATE CONSTRAINT jurisdiction_id IF NOT EXISTS FOR (j:Jurisdiction) REQUIRE j.jurisdictionID IS UNIQUE;
CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (a:Alert) REQUIRE a.alertID IS UNIQUE;
CREATE INDEX account_number IF NOT EXISTS FOR (a:Account) ON (a.accountNumber);
