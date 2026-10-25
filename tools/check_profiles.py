#!/usr/bin/env python3
"""Check the profile decisions: one right decision for every transfer, from
the payer's and the payee's profiles and the payee's place in the graph, and
how long each took.

The expected decisions are computed from the transfers alone, without Neo4j
or clink, as of each transfer in event time:

  history     the payer's transfers in the 28 days before this one, counted
              and totalled
  new payee   the payer first paid the payee within the last day, this
              transfer included
  new payers  the payers who paid the payee for the first time in the last day
  graph       whom the payee's owner is connected to (tools/identity.py)

  hold        the payee's owner is flagged, or shares an identifier with a
              flagged customer; or the payee is new and three or more payers
              paid it for the first time in the last day
  review      the payee is new and the amount is at least three times the
              payer's average over three or more earlier transfers; or the
              payee account is in a high-risk jurisdiction, or its owner is
              two shared identifiers from a flagged customer
  approve     otherwise

  profile decisions   the topic: every expected decision with the profile it
                      was made from, every copy the same, and nothing else
  profile log         the PostgreSQL log: exactly one row per transfer

Latency, in milliseconds from when each transfer reached Kafka (the broker
stamps the transfers and profile-decisions topics with LogAppendTime): the
payee's context read (clink's clock), the decision reaching Kafka, and the
log's row committing (PostgreSQL's commit timestamp).

With --steady N, the latency of the first N transfers, sent while nothing was
failing, is reported apart from that of them all.

Usage: tools/check_profiles.py [--wait 120] [--steady 66] [--events state/demo-events]
"""

import argparse
import collections
import importlib.util
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAY = 86_400_000
HISTORY = 28 * DAY


def load_sibling(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load_sibling("identity")
cd = load_sibling("check_decisions")


def expected_decisions(customers, accounts, transfers):
    """transfer id -> the decision, its reasons and evidence, and the profile."""
    contexts = identity.account_contexts(customers, accounts)
    sent = collections.defaultdict(list)          # payer -> [(ts, amount)]
    first_paid = {}                               # (payer, payee) -> ts
    first_payments = collections.defaultdict(list)  # payee -> [ts]
    want = {}
    for t in sorted(transfers, key=lambda r: r["ts"]):
        payer, payee, ts, amount = t["from_account_id"], t["to_account_id"], t["ts"], t["amount"]
        history = [a for at, a in sent[payer] if ts - at <= HISTORY]
        sent[payer].append((ts, amount))
        first = first_paid.setdefault((payer, payee), ts)
        if first == ts:
            first_payments[payee].append(ts)
        new_payee = ts - first <= DAY
        new_payers = sum(1 for at in first_payments[payee] if ts - at <= DAY)
        unusual = len(history) >= 3 and amount * len(history) >= 3 * sum(history)

        c = contexts.get(payee)
        flagged = bool(c and c["owner_flagged"])
        one = bool(c and c["one"])
        two = bool(c and c["two"])
        high_risk = bool(c and c["high_risk"])
        fan_in = new_payee and new_payers >= 3
        reasons = [r for r, on in (("payee_flagged", flagged), ("payee_linked", one),
                                   ("payee_fan_in", fan_in),
                                   ("unusual_amount", new_payee and unusual),
                                   ("payee_high_risk", high_risk), ("payee_near", two)) if on]
        decision = ("hold" if flagged or one or fan_in else
                    "review" if (new_payee and unusual) or high_risk or two else "approve")
        want[t["transfer_id"]] = {
            "decision": decision, "reasons": ",".join(reasons),
            "evidence": identity.evidence(c) if c else "",
            "payer_history": len(history), "payer_history_total": round(sum(history), 2),
            "first_paid_at": first, "payee_new_payers": new_payers,
        }
    return want


def profile_log():
    out = cd.compose("exec", "-T", "postgres", "psql", "-U", "sentry", "-d", "sentry", "-At",
                     "-c",
                     "SELECT row_to_json(x) FROM (SELECT transfer_id, decision, reasons, evidence, "
                     "(extract(epoch FROM pg_xact_commit_timestamp(xmin)) * 1000)::bigint "
                     "AS committed_ms FROM profile_log) x")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def compare(what, rows, want, exactly_one, with_profile):
    fields = ("decision", "reasons", "evidence")
    numbers = ("payer_history", "first_paid_at", "payee_new_payers")

    def shape(r):
        out = {f: r.get(f) or "" for f in fields}
        if with_profile:
            for f in numbers:
                out[f] = int(r[f]) if r.get(f) is not None else None
            total = r.get("payer_history_total")
            out["payer_history_total"] = round(total, 2) if total is not None else None
        return out

    def wanted(w):
        out = {f: w[f] for f in fields}
        if with_profile:
            for f in numbers + ("payer_history_total",):
                out[f] = w[f]
        return out

    by_id = collections.defaultdict(list)
    for r in rows:
        by_id[r["transfer_id"]].append(shape(r))
    missing = sorted(set(want) - set(by_id))
    extra = sorted(set(by_id) - set(want))
    wrong = sorted(t for t in set(by_id) & set(want)
                   if any(c != wanted(want[t]) for c in by_id[t]))
    repeated = sum(len(c) - 1 for c in by_id.values())
    ok = not (missing or extra or wrong) and (repeated == 0 or not exactly_one)
    print(f"  {what}: {len(rows)} rows for {len(by_id)} transfers, expected {len(want)}"
          f"{f', {repeated} repeated' if repeated else ''}{'  OK' if ok else ''}")
    for label, items in (("missing", missing), ("unexpected", extra), ("different", wrong)):
        if items:
            print(f"    {label}: {len(items)}, e.g. {items[:3]}")
    if wrong:
        t = wrong[0]
        print(f"      got  {by_id[t]}\n      want {wanted(want[t])}")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--steady", type=int, default=0)
    args = ap.parse_args()
    customers, accounts, transfers = identity.read_events(
        args.events, "customers", "accounts", "transfers")
    want = expected_decisions(customers, accounts, transfers)

    deadline = time.time() + args.wait
    while True:
        log = profile_log()
        if len({r["transfer_id"] for r in log}) >= len(want) or time.time() >= deadline:
            break
        time.sleep(3)
    topic = cd.stamped_topic("profile-decisions")
    arrived = {}
    for r in cd.stamped_topic("transfers"):
        if r.get("sent_at"):
            arrived.setdefault(r["transfer_id"], r)

    counts = collections.Counter(w["decision"] for w in want.values())
    print(f"profiles: {len(transfers)} transfers, expected {dict(sorted(counts.items()))}")
    ok = compare("profile decisions topic", topic, want, exactly_one=False, with_profile=True)
    ok &= compare("profile log", log, want, exactly_one=True, with_profile=False)

    first = {}
    for r in sorted(topic, key=lambda r: r["appended_ms"]):
        first.setdefault(r["transfer_id"], r)
    committed = {r["transfer_id"]: r["committed_ms"] for r in log}
    at = lambda t: arrived[t]["appended_ms"]

    def stages(ids):
        looked = [t for t in ids if first[t].get("context_at")]
        print("    context read  " + cd.summary([first[t]["context_at"] - at(t) for t in looked]))
        print("    published     " + cd.summary([first[t]["appended_ms"] - at(t) for t in ids]))
        print("    logged        " + cd.summary([committed[t] - at(t) for t in ids
                                                  if t in committed]))

    order = [t["transfer_id"] for t in sorted(transfers, key=lambda r: r["ts"])]
    decided = [t for t in order if t in arrived and t in first]
    print("latency, ms after each transfer reached Kafka:")
    if args.steady:
        steady = [t for t in order[:args.steady] if t in arrived and t in first]
        print(f"  the first {args.steady}, sent while nothing was failing ({len(steady)}):")
        stages(steady)
        slowest = sorted(steady, key=lambda t: first[t]["appended_ms"] - at(t), reverse=True)
        print("    slowest published, by position in the stream: " +
              ", ".join(f"#{order.index(t)} {first[t]['appended_ms'] - at(t)} ms"
                        for t in slowest[:10]))
    print(f"  all {len(decided)}:")
    stages(decided)
    print("check_profiles: " + ("PASS" if ok else "FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
