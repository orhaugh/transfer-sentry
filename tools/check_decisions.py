#!/usr/bin/env python3
"""Check the decision path: one right decision per transfer, and how long each
took to be made, published and logged.

The expected decision for every transfer is computed from the event streams
alone, without Neo4j or clink: who owns the payee account, which customers
that owner shares an email address, phone number or street address with,
directly or through one other customer, which of them are flagged as fraud,
and whether the account is in a high-risk jurisdiction.

  decisions      the decisions topic: every transfer decided, every copy of a
                 decision (a restore can publish one again) the same, and the
                 same as expected
  decision log   the PostgreSQL log: exactly one row per transfer, as expected

Latency, in milliseconds from when each transfer reached Kafka (the broker
stamps the transactions and decisions topics with LogAppendTime):

  context read   the Neo4j lookup's reply arrived (clink's clock)
  published      the decision reached Kafka
  logged         the decision log's transaction committed (PostgreSQL's
                 commit timestamp, track_commit_timestamp=on)

and, apart, how long the feeder's own path took to get a transfer to Kafka
(from its sent_at stamp), which is not clink's. clink and the feeder run on the
host; the broker and PostgreSQL run in Docker, whose clock can differ from the
host's by a few milliseconds.

With --steady N, the latency of the first N transfers, sent while nothing
was failing, is reported apart from that of them all.

Usage: tools/check_decisions.py [--wait 120] [--steady 150]
                                [--events state/demo-events]
"""

import argparse
import collections
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KINDS = ("email_id", "phone_id", "address_id")


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                          capture_output=True, text=True).stdout


def read_events(events):
    load = lambda n: [json.loads(line) for line in (Path(events) / f"{n}.jsonl").open()]
    return load("customers"), load("accounts"), load("transactions")


def expected_decisions(customers, accounts, transactions):
    """transaction id -> {decision, reasons, evidence}."""
    by_id = {c["customer_id"]: c for c in customers}
    holders = collections.defaultdict(set)  # (kind, identifier id) -> customers
    for c in customers:
        for kind in KINDS:
            if c[kind] is not None:
                holders[(kind, c[kind])].add(c["customer_id"])

    def shares(cid):
        out = set()
        for kind in KINDS:
            value = by_id[cid][kind]
            if value is not None:
                out |= holders[(kind, value)]
        out.discard(cid)
        return out

    account = {a["account_id"]: a for a in accounts}
    context = {}
    for aid, a in account.items():
        owner = a["customer_id"]
        if owner is None or owner not in by_id:
            one, two = set(), set()
            flagged = False
        else:
            flagged = by_id[owner]["fraud_flag"]
            near = shares(owner)
            one = {o for o in near if by_id[o]["fraud_flag"]}
            two = set()
            for m in near:
                for o in shares(m):
                    if o not in (owner, m) and by_id[o]["fraud_flag"] and o not in one:
                        two.add(o)
        high_risk = a["high_risk_jurisdiction"]
        reasons = [r for r, on in (("payee_flagged", flagged), ("payee_linked", bool(one)),
                                   ("payee_high_risk", high_risk), ("payee_near", bool(two)))
                   if on]
        decision = ("hold" if flagged or one else "review" if high_risk or two else "approve")
        context[aid] = {"decision": decision, "reasons": ",".join(reasons),
                        "evidence": ",".join((sorted(one) + sorted(two))[:10])}
    return {t["transaction_id"]: context[t["to_account_id"]] for t in transactions}


def stamped_topic(name):
    """Every record of a topic with the broker's LogAppendTime, in offset order."""
    out = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                  "--bootstrap-server", "kafka:9092", "--topic", name,
                  "--from-beginning", "--timeout-ms", "8000",
                  "--property", "print.timestamp=true")
    rows = []
    for line in out.splitlines():
        if not line.startswith(("LogAppendTime:", "CreateTime:")):
            continue
        stamp, _, payload = line.partition("\t")
        row = json.loads(payload)
        row["appended_ms"] = int(stamp.split(":", 1)[1])
        rows.append(row)
    return rows


def decision_log():
    out = compose("exec", "-T", "postgres", "psql", "-U", "sentry", "-d", "sentry", "-At", "-c",
                  "SELECT row_to_json(x) FROM (SELECT transaction_id, decision, reasons, evidence, "
                  "(extract(epoch FROM pg_xact_commit_timestamp(xmin)) * 1000)::bigint "
                  "AS committed_ms FROM decision_log) x")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def summary(values):
    if not values:
        return "no samples"
    v = sorted(values)
    pick = lambda q: v[min(len(v) - 1, int(q * len(v)))]
    return (f"min {v[0]:>6} p50 {pick(0.50):>6} p90 {pick(0.90):>6} p99 {pick(0.99):>6} "
            f"max {v[-1]:>6}  mean {statistics.fmean(v):>8.1f}  (n={len(v)})")


def compare(what, rows, want, exactly_one):
    fields = ("decision", "reasons", "evidence")
    by_tx = collections.defaultdict(list)
    for r in rows:
        by_tx[r["transaction_id"]].append({f: r.get(f) or "" for f in fields})
    missing = sorted(set(want) - set(by_tx))
    extra = sorted(set(by_tx) - set(want))
    wrong = sorted(t for t in set(by_tx) & set(want) if any(c != want[t] for c in by_tx[t]))
    repeated = sum(len(c) - 1 for c in by_tx.values())
    ok = not (missing or extra or wrong) and (repeated == 0 or not exactly_one)
    print(f"  {what}: {len(rows)} rows for {len(by_tx)} transfers, expected {len(want)}"
          f"{f', {repeated} repeated' if repeated else ''}{'  OK' if ok else ''}")
    for label, items in (("missing", missing), ("unexpected", extra), ("different", wrong)):
        if items:
            print(f"    {label}: {len(items)}, e.g. {items[:3]}")
    if wrong:
        t = wrong[0]
        print(f"      got  {by_tx[t]}\n      want {want[t]}")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--steady", type=int, default=0)
    args = ap.parse_args()
    customers, accounts, transactions = read_events(args.events)
    want = expected_decisions(customers, accounts, transactions)

    deadline = time.time() + args.wait
    while True:
        log = decision_log()
        if len({r["transaction_id"] for r in log}) >= len(want) or time.time() >= deadline:
            break
        time.sleep(3)
    topic = stamped_topic("decisions")
    arrived = {}
    for r in stamped_topic("transactions"):
        if r.get("sent_at"):
            arrived.setdefault(r["transaction_id"], r)

    counts = collections.Counter(v["decision"] for v in want.values())
    print(f"decisions: {len(want)} transfers, expected {dict(sorted(counts.items()))}")
    ok = compare("decisions topic", topic, want, exactly_one=False)
    ok &= compare("decision log", log, want, exactly_one=True)

    first = {}
    for r in sorted(topic, key=lambda r: r["appended_ms"]):
        first.setdefault(r["transaction_id"], r)
    committed = {r["transaction_id"]: r["committed_ms"] for r in log}
    at = lambda t: arrived[t]["appended_ms"]

    def stages(ids):
        print("    context read  " + summary([first[t]["context_at"] - at(t) for t in ids
                                               if first[t].get("context_at")]))
        print("    published     " + summary([first[t]["appended_ms"] - at(t) for t in ids]))
        print("    logged        " + summary([committed[t] - at(t) for t in ids
                                               if t in committed]))

    order = [t["transaction_id"] for t in transactions]
    ids = [t for t in order if t in arrived and t in first]
    print("latency, ms after each transfer reached Kafka:")
    if args.steady:
        steady = [t for t in order[:args.steady] if t in arrived and t in first]
        print(f"  the first {len(steady)}, sent while nothing was failing:")
        stages(steady)
        slowest = sorted(steady, key=lambda t: first[t]["appended_ms"] - at(t), reverse=True)
        print("    slowest published, by position in the stream: " +
              ", ".join(f"#{order.index(t)} {first[t]['appended_ms'] - at(t)} ms"
                        for t in slowest[:10]))
    print(f"  all {len(ids)}:")
    stages(ids)
    print("the feeder's own path, sent_at to Kafka:")
    print("    to Kafka      " + summary([at(t) - arrived[t]["sent_at"] for t in ids]))
    print("check_decisions: " + ("PASS" if ok else "FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
