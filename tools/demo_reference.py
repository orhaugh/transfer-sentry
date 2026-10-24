#!/usr/bin/env python3
"""What Neo4j's fraud demo finds in its own data, computed without Neo4j.

Reads the demo's CSV export (fetched by scripts/fetch-demo.sh) and applies the
demo's dashboard queries by their own rules, so the streaming pipeline has an
independent answer to be checked against:

  rings        a run of transfers that leaves an account and comes back to it,
               through 3 to 16 distinct accounts, each transfer strictly later
               than the one before and between 80% and 100% of its amount
               (the demo's "Rings" query)
  shared PII   every phone number, email address and street address used by
               more than one customer, with the number of customers using it
               (the demo's "Customers Sharing PII" query)

Usage: tools/demo_reference.py [--data data/neo4j-demo/data] [--json]
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path


def rows(data, name):
    path = Path(data) / f"export_transactions_{name}.csv"
    # utf-8-sig: the Account export starts with a byte-order mark.
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load(data):
    accounts = {r["accountID"]: r for r in rows(data, "Account_with_labels")}
    txs = {r["transactionID"]: r for r in rows(data, "Transaction")}
    sender = {r["end_Transaction_transactionID"]: r["start_Account_accountID"]
              for r in rows(data, "Account_SENT_Transaction")}
    receiver = {r["start_Transaction_transactionID"]: r["end_Account_accountID"]
                for r in rows(data, "Transaction_RECEIVED_Account")}
    transfers = []
    for tid, t in txs.items():
        if tid not in sender or tid not in receiver:
            continue
        transfers.append({
            "id": tid,
            "from": sender[tid],
            "to": receiver[tid],
            "date": t["date"],
            "amount": float(t["amount"]),
        })
    return accounts, transfers


def rings(accounts, transfers):
    """Every ring the demo's quantified path pattern matches, once each.

    The pattern is (a)-[:SENT]->(first)((tx_i)-[:RECEIVED]->(a_i)-[:SENT]->(tx_j)
    WHERE tx_i.date < tx_j.date AND tx_i.amount >= tx_j.amount >= 0.8 *
    tx_i.amount){2,15}(last)-[:RECEIVED]->(a), with a and every a_i distinct:
    k iterations visit k intermediate accounts, so a ring has k + 1 accounts
    and k + 1 transfers, and k is 2 to 15.
    """
    out_of = defaultdict(list)
    for t in transfers:
        out_of[t["from"]].append(t)
    found = []

    def extend(entry, path, seen):
        last = path[-1]
        hops = len(path) - 1  # iterations completed so far
        here = last["to"]
        if here == entry:
            return
        for nxt in out_of[here]:
            if not (last["date"] < nxt["date"]
                    and last["amount"] >= nxt["amount"] >= 0.8 * last["amount"]):
                continue
            if nxt["to"] == entry and hops + 1 >= 2:
                found.append(path + [nxt])
            if hops + 1 < 15 and nxt["to"] not in seen and nxt["to"] != entry:
                extend(entry, path + [nxt], seen | {nxt["to"]})

    for first in transfers:
        entry = first["from"]
        if first["to"] == entry:
            continue
        extend(entry, [first], {first["to"]})
    result = []
    for path in found:
        members = [path[0]["from"]] + [t["to"] for t in path[:-1]]
        result.append({
            "entry_account": accounts[path[0]["from"]]["accountNumber"],
            "ring_size": len(members),
            "accounts": [accounts[a]["accountNumber"] for a in members],
            "transactions": [t["id"] for t in path],
            "first_date": path[0]["date"],
            "last_date": path[-1]["date"],
        })
    result.sort(key=lambda r: (r["entry_account"], r["transactions"]))
    return result


def shared_pii(data):
    """Identifiers used by more than one customer, most shared first."""
    kinds = [
        ("PhoneNumber", "phoneNumberID", "number", "Customer_USES_PHONE_PhoneNumber",
         "end_PhoneNumber_phoneNumberID"),
        ("Email", "emailID", "emailAddress", "Customer_USES_EMAIL_Email",
         "end_Email_emailID"),
        ("Address", "addressID", "street", "Customer_RESIDES_AT_Address",
         "end_Address_addressID"),
    ]
    out = []
    for label, id_col, value_col, rel, rel_end in kinds:
        value = {r[id_col]: r[value_col] for r in rows(data, label)}
        users = defaultdict(set)
        for r in rows(data, rel):
            users[r[rel_end]].add(r["start_Customer_customerID"])
        for ident, customers in users.items():
            if len(customers) > 1:
                out.append({"kind": label, "value": value.get(ident, ident),
                            "customers": len(customers)})
    out.sort(key=lambda r: (-r["customers"], r["kind"], r["value"]))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", default=str(Path(__file__).resolve().parent.parent
                                          / "data" / "neo4j-demo" / "data"))
    ap.add_argument("--json", action="store_true", help="print the whole answer as JSON")
    args = ap.parse_args()
    accounts, transfers = load(args.data)
    answer = {"rings": rings(accounts, transfers), "shared_pii": shared_pii(args.data)}
    if args.json:
        json.dump(answer, sys.stdout, indent=2)
        print()
        return
    print(f"{len(transfers)} transfers between {len(accounts)} accounts")
    print(f"rings: {len(answer['rings'])}")
    for r in answer["rings"]:
        print(f"  entry {r['entry_account']}  size {r['ring_size']}  "
              f"{r['first_date'][:10]} .. {r['last_date'][:10]}")
    print("shared identifiers (top 10):")
    for r in answer["shared_pii"][:10]:
        print(f"  {r['customers']}  {r['kind']:<11} {r['value']}")


if __name__ == "__main__":
    main()
