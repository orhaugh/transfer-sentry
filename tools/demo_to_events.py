#!/usr/bin/env python3
"""Turn Neo4j's fraud demo into the event streams a bank would emit.

The demo is a database export: every customer, account and transaction at
once. A bank sees them as they happen: a customer onboards with an email
address, a phone number and an address; an account opens; money moves. This
writes the demo's data as those three streams, one JSON object per line, in
the order the pipeline will read them:

  customers.jsonl     one onboarding per customer, with its identifiers
  accounts.jsonl      one per account, internal or external, with its owner
  transactions.jsonl  one per transaction, in date order

Values come from the demo unchanged. Identifier nodes keep the demo's ids
(email, phone and address ids), standing in for the normalised keys a real
onboarding system would assign. A transaction carries both accounts' ids and
numbers, as a payment message names its debtor and creditor accounts, so the
graph can place a transfer without waiting for either account's own event.

Event time is the transaction date at midnight UTC, the demo's own
granularity, so several transactions share a timestamp and are ordered by id
within it. The demo has no onboarding dates, so customers onboard one minute
apart in id order, the last a minute before the first transaction: a
stand-in that gives "already a customer when this one onboarded" a fixed
meaning.

Usage: tools/demo_to_events.py [--data data/neo4j-demo/data] --out state/demo-events
"""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


def rows(data, name):
    path = Path(data) / f"export_transactions_{name}.csv"
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def epoch_ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00"))
               .astimezone(timezone.utc).timestamp() * 1000)


def one_to_one(data, rel, start, end):
    """start id -> end id for a relationship each start has at most once."""
    out = {}
    for r in rows(data, rel):
        out[r[start]] = r[end]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", default=str(Path(__file__).resolve().parent.parent
                                          / "data" / "neo4j-demo" / "data"))
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    data, out = args.data, Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    emails = {r["emailID"]: r["emailAddress"] for r in rows(data, "Email")}
    phones = {r["phoneNumberID"]: r["number"] for r in rows(data, "PhoneNumber")}
    addresses = {r["addressID"]: r for r in rows(data, "Address")}
    jurisdictions = {r["jurisdictionID"]: r for r in rows(data, "Jurisdiction")}
    address_jurisdiction = one_to_one(data, "Address_LOCATED_IN_Jurisdiction",
                                      "start_Address_addressID",
                                      "end_Jurisdiction_jurisdictionID")
    email_of = one_to_one(data, "Customer_USES_EMAIL_Email",
                          "start_Customer_customerID", "end_Email_emailID")
    phone_of = one_to_one(data, "Customer_USES_PHONE_PhoneNumber",
                          "start_Customer_customerID", "end_PhoneNumber_phoneNumberID")
    address_of = one_to_one(data, "Customer_RESIDES_AT_Address",
                            "start_Customer_customerID", "end_Address_addressID")
    owner_of = {r["end_Account_accountID"]: r["start_Customer_customerID"]
                for r in rows(data, "Customer_HAS_ACCOUNT_Account")}

    sender = one_to_one(data, "Account_SENT_Transaction",
                        "end_Transaction_transactionID", "start_Account_accountID")
    receiver = one_to_one(data, "Transaction_RECEIVED_Account",
                          "start_Transaction_transactionID", "end_Account_accountID")
    tx_rows = [t for t in rows(data, "Transaction")
               if t["transactionID"] in sender and t["transactionID"] in receiver]
    first_tx = min(epoch_ms(t["date"]) for t in tx_rows)

    customers = []
    demo_customers = sorted(rows(data, "Customer"), key=lambda r: r["customerID"])
    for i, c in enumerate(demo_customers):
        cid = c["customerID"]
        addr = addresses.get(address_of.get(cid, ""), {})
        jur = jurisdictions.get(address_jurisdiction.get(addr.get("addressID", ""), ""), {})
        customers.append({
            "customer_id": cid,
            "name": c["name"],
            "date_of_birth": c["dateOfBirth"][:10],
            "email_id": email_of.get(cid),
            "email": emails.get(email_of.get(cid, "")),
            "phone_id": phone_of.get(cid),
            "phone": phones.get(phone_of.get(cid, "")),
            "address_id": addr.get("addressID"),
            "street": addr.get("street"),
            "city": addr.get("city"),
            "state": addr.get("state"),
            "country": addr.get("country"),
            "jurisdiction_id": jur.get("jurisdictionID"),
            "jurisdiction": jur.get("name"),
            "jurisdiction_risk": jur.get("riskLevel"),
            # The demo's own label for customers it treats as known fraud.
            "fraud_flag": c["fraudFlag"] == "True",
            "ts": first_tx - (len(demo_customers) - i) * 60_000,
        })

    accounts = []
    for a in sorted(rows(data, "Account_with_labels"), key=lambda r: r["accountID"]):
        accounts.append({
            "account_id": a["accountID"],
            "account_number": a["accountNumber"],
            "account_type": a["accountType"],
            "status": a["status"],
            "internal": a["internalExternal"] == "Internal",
            "high_risk_jurisdiction": a["highRiskJurisdiction"] == "true",
            "customer_id": owner_of.get(a["accountID"]),
        })

    number = {a["account_id"]: a["account_number"] for a in accounts}
    transactions = []
    for t in tx_rows:
        tid = t["transactionID"]
        transactions.append({
            "transaction_id": tid,
            "from_account_id": sender[tid],
            "from_account_number": number[sender[tid]],
            "to_account_id": receiver[tid],
            "to_account_number": number[receiver[tid]],
            "amount": float(t["amount"]),
            "type": t["type"],
            "status": t["status"],
            "ts": epoch_ms(t["date"]),
        })
    transactions.sort(key=lambda t: (t["ts"], t["transaction_id"]))

    for name, items in (("customers", customers), ("accounts", accounts),
                        ("transactions", transactions)):
        with (out / f"{name}.jsonl").open("w") as f:
            for item in items:
                f.write(json.dumps(item, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"demo_to_events: {len(customers)} customers, {len(accounts)} accounts, "
          f"{len(transactions)} transactions -> {out}")


if __name__ == "__main__":
    main()
