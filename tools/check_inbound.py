#!/usr/bin/env python3
"""Check the inbound screen: one right decision for every credit, every
onward payment that passes money straight through held, and how long each
took.

The expected decisions are computed from the event streams alone, without
Neo4j or clink:

  credits   for each credit, how many credits the account took in the hour up
            to and including it, and whom the account's owner is connected to
            (tools/identity.py): hold if the owner is flagged as fraud or
            shares an identifier with a flagged customer, or the credit is the
            fourth or more within the hour; review if the owner is two shared
            identifiers from a flagged customer; credit otherwise
  onward    every debit that follows a credit directly, in the account's own
            order, at 80% to 100% of it and within half an hour: held, with
            the credit as its evidence. No other debit is decided.

  inbound decisions   the topic: every expected decision, every copy of one
                      the same, and nothing else
  inbound log         the PostgreSQL log: exactly one row per expected decision

Latency, in milliseconds from when each payment reached Kafka (the broker
stamps the payments and inbound-decisions topics with LogAppendTime): the
receiver's context read (credits only, clink's clock), the decision reaching
Kafka, and the log's row committing (PostgreSQL's commit timestamp).

With --steady N, the latency of the first N payments, sent while nothing was
failing, is reported apart from that of them all.

Usage: tools/check_inbound.py [--wait 120] [--steady 56] [--events state/demo-events]
"""

import argparse
import collections
import importlib.util
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOUR = 3_600_000
HALF_HOUR = 1_800_000


def load_sibling(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load_sibling("identity")
cd = load_sibling("check_decisions")


def expected_decisions(customers, accounts, payments):
    """payment id -> {direction, decision, reasons, evidence, credits_hour}."""
    contexts = identity.account_contexts(customers, accounts)
    by_account = collections.defaultdict(list)
    for p in payments:
        by_account[p["account_id"]].append(p)
    want = {}
    for account, own in by_account.items():
        own.sort(key=lambda p: p["ts"])
        c = contexts[account]
        credits = [p for p in own if p["direction"] == "credit"]
        for p in credits:
            n = sum(1 for q in credits if p["ts"] - HOUR <= q["ts"] <= p["ts"])
            flagged, linked, near = c["owner_flagged"], bool(c["one"]), bool(c["two"])
            reasons = [r for r, on in (("receiver_flagged", flagged), ("receiver_linked", linked),
                                       ("fan_in", n >= 4), ("receiver_near", near)) if on]
            decision = ("hold" if flagged or linked or n >= 4 else "review" if near
                        else "credit")
            want[p["payment_id"]] = {"direction": "credit", "decision": decision,
                                     "reasons": ",".join(reasons),
                                     "evidence": identity.evidence(c), "credits_hour": n}
        for prev, p in zip(own, own[1:]):
            if (prev["direction"] == "credit" and p["direction"] == "debit"
                    and 0.8 * prev["amount"] <= p["amount"] <= prev["amount"]
                    and p["ts"] <= prev["ts"] + HALF_HOUR):
                want[p["payment_id"]] = {"direction": "debit", "decision": "hold",
                                         "reasons": "pass_through",
                                         "evidence": prev["payment_id"], "credits_hour": None}
    return want


def inbound_log():
    out = cd.compose("exec", "-T", "postgres", "psql", "-U", "sentry", "-d", "sentry", "-At",
                     "-c",
                     "SELECT row_to_json(x) FROM (SELECT payment_id, direction, decision, "
                     "reasons, evidence, (extract(epoch FROM pg_xact_commit_timestamp(xmin)) "
                     "* 1000)::bigint AS committed_ms FROM inbound_log) x")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def compare(what, rows, want, exactly_one, with_count):
    fields = ("direction", "decision", "reasons", "evidence")

    def shape(r):
        out = {f: r.get(f) or "" for f in fields}
        if with_count:
            out["credits_hour"] = r.get("credits_hour")
        return out

    def wanted(w):
        out = {f: w[f] or "" for f in fields}
        if with_count:
            out["credits_hour"] = w["credits_hour"]
        return out

    by_id = collections.defaultdict(list)
    for r in rows:
        by_id[r["payment_id"]].append(shape(r))
    missing = sorted(set(want) - set(by_id))
    extra = sorted(set(by_id) - set(want))
    wrong = sorted(p for p in set(by_id) & set(want)
                   if any(c != wanted(want[p]) for c in by_id[p]))
    repeated = sum(len(c) - 1 for c in by_id.values())
    ok = not (missing or extra or wrong) and (repeated == 0 or not exactly_one)
    print(f"  {what}: {len(rows)} rows for {len(by_id)} payments, expected {len(want)}"
          f"{f', {repeated} repeated' if repeated else ''}{'  OK' if ok else ''}")
    for label, items in (("missing", missing), ("unexpected", extra), ("different", wrong)):
        if items:
            print(f"    {label}: {len(items)}, e.g. {items[:3]}")
    if wrong:
        p = wrong[0]
        print(f"      got  {by_id[p]}\n      want {wanted(want[p])}")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--steady", type=int, default=0)
    args = ap.parse_args()
    customers, accounts, payments = identity.read_events(
        args.events, "customers", "accounts", "payments")
    want = expected_decisions(customers, accounts, payments)

    deadline = time.time() + args.wait
    while True:
        log = inbound_log()
        if len({r["payment_id"] for r in log}) >= len(want) or time.time() >= deadline:
            break
        time.sleep(3)
    topic = cd.stamped_topic("inbound-decisions")
    arrived = {}
    for r in cd.stamped_topic("payments"):
        if r.get("sent_at"):
            arrived.setdefault(r["payment_id"], r)

    counts = collections.Counter((w["direction"], w["decision"]) for w in want.values())
    print(f"inbound: {len(payments)} payments, {len(want)} decisions expected "
          f"{dict(sorted(counts.items()))}")
    ok = compare("inbound decisions topic", topic, want, exactly_one=False, with_count=True)
    ok &= compare("inbound log", log, want, exactly_one=True, with_count=False)

    first = {}
    for r in sorted(topic, key=lambda r: r["appended_ms"]):
        first.setdefault(r["payment_id"], r)
    committed = {r["payment_id"]: r["committed_ms"] for r in log}
    at = lambda p: arrived[p]["appended_ms"]

    def stages(ids):
        credits = [p for p in ids if first[p].get("context_at")]
        print("    context read  " + cd.summary([first[p]["context_at"] - at(p) for p in credits]))
        print("    published     " + cd.summary([first[p]["appended_ms"] - at(p) for p in ids]))
        print("    logged        " + cd.summary([committed[p] - at(p) for p in ids
                                                  if p in committed]))

    order = [p["payment_id"] for p in payments]
    decided = [p for p in order if p in arrived and p in first]
    print("latency, ms after each payment reached Kafka:")
    if args.steady:
        steady = [p for p in order[:args.steady] if p in arrived and p in first]
        print(f"  the decisions on the first {args.steady} payments, sent while nothing was "
              f"failing ({len(steady)}):")
        stages(steady)
        slowest = sorted(steady, key=lambda p: first[p]["appended_ms"] - at(p), reverse=True)
        print("    slowest published, by position in the stream: " +
              ", ".join(f"#{order.index(p)} {first[p]['appended_ms'] - at(p)} ms"
                        for p in slowest[:10]))
    print(f"  all {len(decided)}:")
    stages(decided)
    print("check_inbound: " + ("PASS" if ok else "FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
