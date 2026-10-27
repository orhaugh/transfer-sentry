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
  high risk    every account that paid an account in a high-risk jurisdiction
               since the page's start date, once per path traced back from it
               (the demo's "High Risk Accounts" query), and the same trace
               anchored on each payment into a high-risk account, as the
               detectors read it

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


# The start date of the demo's "High Risk Accounts" page.
TRACE_SINCE = "2024-08-22"


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


def near(moved, total):
    return 0.9 * total <= moved <= 1.1 * total


def high_risk_accounts(accounts, transfers, since=TRACE_SINCE):
    """The account numbers the demo's "High Risk Accounts" query returns, one
    per row, as Neo4j returns them.

    For an account l that paid a high-risk account h on or after the start
    date, T is what l paid h since then. The query matches every path
    (first)((a1)-[:SENT]->(t)-[:RECEIVED]->(a2))*(l)-[:SENT]->(tx)-[:RECEIVED]->(h)
    in which 90% to 110% of T moved from a1 to a2 since the start date and tx
    is on or after it; a hop's transfer t may be of any date, and a path uses
    each transfer once. Its last condition is meant to keep only paths that
    start where no account paid first 90% to 110% of T, but it matches
    (before)-[:SENT]->(tx)-[:RECEIVED]->(first) with the tx already bound to
    the payment into h, so it only drops a path that starts at h itself when
    that payment alone is 90% to 110% of T. Every other path back from l is
    a row, however far back it starts.
    """
    hrj = {a for a, r in accounts.items() if r["highRiskJurisdiction"] == "true"}
    moved, between = defaultdict(float), defaultdict(list)
    for t in transfers:
        between[(t["from"], t["to"])].append(t)
        if t["date"][:10] >= since:
            moved[(t["from"], t["to"])] += t["amount"]
    into = defaultdict(set)
    for a1, a2 in between:
        into[a2].add(a1)

    def paths(first, h, tx, total, used):
        n = 0 if first == h and near(tx["amount"], total) else 1
        for b in into[first]:
            if near(moved[(b, first)], total):
                n += sum(paths(b, h, tx, total, used | {t["id"]})
                         for t in between[(b, first)] if t["id"] not in used)
        return n

    out = []
    for (l, h), total in moved.items():
        if h not in hrj:
            continue
        for tx in between[(l, h)]:
            if tx["date"][:10] >= since:
                out += [accounts[l]["accountNumber"]] * paths(l, h, tx, total, {tx["id"]})
    return sorted(out)


def high_risk_traces(accounts, transfers, flagged_owner, since=TRACE_SINCE):
    """The trace the page means, anchored on each payment into a high-risk
    account, as the detectors ask it: only transfers from the start date up
    to the payment, in stream order (date, then id), count, so money that
    reached an account after it paid is not followed. The trace is a chain
    of distinct accounts, at least one hop long, back to an account no one
    paid 90% to 110% of the total; the longest is reported, then the lowest
    by account numbers, with how many there are.
    """
    hrj = {a for a, r in accounts.items() if r["highRiskJurisdiction"] == "true"}
    number = {a: r["accountNumber"] for a, r in accounts.items()}
    order = sorted(transfers, key=lambda t: (t["date"], t["id"]))
    out = {}
    for i, x in enumerate(order):
        l, h = x["from"], x["to"]
        if h not in hrj or x["date"][:10] < since:
            continue
        window = [t for t in order[:i + 1] if t["date"][:10] >= since]
        moved, between = defaultdict(float), defaultdict(list)
        for t in window:
            moved[(t["from"], t["to"])] += t["amount"]
            between[(t["from"], t["to"])].append(t)
        total = moved[(l, h)]
        into = defaultdict(set)
        for a1, a2 in between:
            into[a2].add(a1)
        chains = []

        def back(chain):
            before = [b for b in into[chain[0]] if near(moved[(b, chain[0])], total)]
            if not before:
                if len(chain) > 1:
                    chains.append(chain)
                return
            for b in before:
                if b not in chain:
                    back([b] + chain)

        back([l])
        if not chains:
            continue
        chains.sort(key=lambda c: (-len(c), [number[a] for a in c]))
        chain = chains[0]
        hops = [t["id"] for a1, a2 in zip(chain, chain[1:]) for t in between[(a1, a2)]]
        paid = [t["id"] for t in between[(l, h)] if t["id"] != x["id"]]
        out[x["id"]] = {
            "hops": len(chain) - 1,
            "traces": len(chains),
            "origin_account": number[chain[0]],
            "high_risk_account": number[h],
            "trace_accounts": ",".join(number[a] for a in chain),
            "origin_high_risk": chain[0] in hrj,
            "flagged_on_chain": sum(1 for a in chain if a in flagged_owner),
            "evidence": ",".join(hops + paid),
        }
    return out


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
    flagged = {r["customerID"] for r in rows(args.data, "Customer") if r["fraudFlag"] == "True"}
    flagged_owner = {r["end_Account_accountID"] for r in rows(args.data, "Customer_HAS_ACCOUNT_Account")
                     if r["start_Customer_customerID"] in flagged}
    answer = {"rings": rings(accounts, transfers), "shared_pii": shared_pii(args.data),
              "high_risk_accounts": high_risk_accounts(accounts, transfers),
              "high_risk_traces": high_risk_traces(accounts, transfers, flagged_owner)}
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
    print(f"high risk accounts: {len(answer['high_risk_accounts'])} rows, "
          f"{len(set(answer['high_risk_accounts']))} accounts")
    print(f"high-risk traces, anchored on the payment: {len(answer['high_risk_traces'])}")
    for tid, t in answer["high_risk_traces"].items():
        print(f"  {tid}  {t['trace_accounts']} -> {t['high_risk_account']}  "
              f"traces {t['traces']}  origin high risk {t['origin_high_risk']}  "
              f"flagged {t['flagged_on_chain']}")


if __name__ == "__main__":
    main()
