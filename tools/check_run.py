#!/usr/bin/env python3
"""Check a whole transfer-sentry run against what the demo's data says.

Reads every store the pipeline writes, each through a client of its own, and
compares it with the expected alerts, computed from the demo's export without
the pipeline (tools/graph_check.py's reference):

  graph       tools/graph_check.py --existing: the graph against the demo's
              export, the demo's dashboard queries, every detector read
  announced   committed-customers and committed-transactions, read_committed:
              every onboarding and transaction, each once
  alerts      the alerts topic, read_committed: exactly the expected alerts,
              each once, with the expected subject, evidence and severity
  cases       the case store: one row per expected alert, no other rows
  alert graph one Alert node per expected alert, linked to what raised it and
              to its evidence

  --wait N    poll up to N seconds for the alerts to reach every store first

Usage: tools/check_run.py [--wait 120] [--existing-graph-only]
"""

import argparse
import base64
import collections
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_sibling(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gc = load_sibling("graph_check")


def compose(*args, stdin=None):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                          capture_output=True, text=True, input=stdin).stdout


def topic(name, timeout_ms=8000):
    out = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                  "--bootstrap-server", "kafka:9092", "--topic", name, "--from-beginning",
                  "--isolation-level", "read_committed", "--timeout-ms", str(timeout_ms))
    return [json.loads(line) for line in out.splitlines() if line.startswith("{")]


def cases():
    out = compose("exec", "-T", "postgres", "psql", "-U", "sentry", "-d", "sentry", "-At", "-c",
                  "SELECT row_to_json(c) FROM (SELECT alert_id, kind, severity, at_ms AS at, "
                  "subject, evidence FROM cases) c")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def expected_alerts(events, reference):
    customers, _, transactions = events
    ts = {t["transaction_id"]: t["ts"] for t in transactions}
    cts = {c["customer_id"]: c["ts"] for c in customers}
    out = {}
    for tid, r in gc.expected_rings(reference).items():
        out[f"ring:{tid}"] = {"kind": "transaction_ring", "severity": "high", "at": ts[tid],
                              "subject": tid, "evidence": r["ring_transactions"]}
    for cid, i in gc.expected_identity(customers).items():
        kinds = i["shared_kinds"].split(",")
        severity = ("high" if i["shared_with_fraud"] > 0 else
                    "medium" if "USES_EMAIL" in kinds or "USES_PHONE" in kinds else "low")
        out[f"identity:{cid}"] = {"kind": "shared_identity", "severity": severity,
                                  "at": cts[cid], "subject": cid,
                                  "evidence": i["evidence_customers"]}
    for tid, t in reference["high_risk_traces"].items():
        severity = "high" if t["origin_high_risk"] or t["flagged_on_chain"] > 0 else "medium"
        out[f"trace:{tid}"] = {"kind": "high_risk_trace", "severity": severity, "at": ts[tid],
                               "subject": tid, "evidence": t["evidence"]}
    return out


def compare(what, rows, want):
    """rows: dicts with alert_id and the expected fields."""
    count = collections.Counter(r["alert_id"] for r in rows)
    dupes = {k: n for k, n in count.items() if n > 1}
    got = {r["alert_id"]: {f: r.get(f) for f in ("kind", "severity", "at", "subject", "evidence")}
           for r in rows}
    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    wrong = sorted(k for k in set(got) & set(want) if got[k] != want[k])
    ok = not (dupes or missing or extra or wrong)
    print(f"  {what}: {len(rows)} rows, {len(got)} alerts, expected {len(want)}"
          f"{'  OK' if ok else ''}")
    for label, items in (("duplicated", sorted(dupes)), ("missing", missing),
                         ("unexpected", extra), ("different", wrong)):
        if items:
            print(f"    {label}: {len(items)}, e.g. {items[:3]}")
            if label == "different":
                k = items[0]
                print(f"      got  {got[k]}\n      want {want[k]}")
    return ok


def alert_graph(db, want):
    rows = db.run(
        "MATCH (al:Alert) "
        "OPTIONAL MATCH (al)-[:RAISED_ON]->(s) "
        "WITH al, collect(coalesce(s.transactionID, s.customerID)) AS subjects "
        "OPTIONAL MATCH (al)-[:EVIDENCE]->(e) "
        "WITH al, subjects, collect(coalesce(e.transactionID, e.customerID)) AS evidence "
        "RETURN al.alertID AS alert_id, al.kind AS kind, al.severity AS severity, "
        "al.at.epochMillis AS at, subjects, evidence")
    out = []
    for r in rows:
        w = want.get(r["alert_id"], {})
        # The graph holds evidence as relationships: compare as sets.
        same_evidence = sorted(r["evidence"]) == sorted((w.get("evidence") or "").split(","))
        out.append({"alert_id": r["alert_id"], "kind": r["kind"], "severity": r["severity"],
                    "at": r["at"], "subject": r["subjects"][0] if len(r["subjects"]) == 1 else
                    r["subjects"], "evidence": w.get("evidence") if same_evidence else
                    ",".join(sorted(r["evidence"]))})
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--endpoint", default="http://localhost:7474")
    ap.add_argument("--password", default="transfer-sentry-pw")
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    args = ap.parse_args()

    events = gc.read_events(args.events)
    reference = json.loads(subprocess.run(
        [sys.executable, "-I", str(ROOT / "tools" / "demo_reference.py"), "--json"],
        check=True, capture_output=True, text=True).stdout)
    want = expected_alerts(events, reference)
    db = gc.Neo4j(args.endpoint, "neo4j", args.password)

    deadline = time.time() + args.wait
    while True:
        n_cases = len(cases())
        n_nodes = db.run("MATCH (al:Alert) RETURN count(al) AS n")[0]["n"]
        if (n_cases >= len(want) and n_nodes >= len(want)) or time.time() >= deadline:
            break
        time.sleep(3)

    ok = True
    print("graph: the keeper's graph, as tools/graph_check.py --existing checks it")
    graph = subprocess.run([sys.executable, "-I", str(ROOT / "tools" / "graph_check.py"),
                            "--existing", "--endpoint", args.endpoint,
                            "--password", args.password],
                           capture_output=True, text=True)
    print("\n".join("  " + l for l in graph.stdout.splitlines()
                    if "PASS" in l or "FAIL" in l or "DIFFERENT" in l or "NO" in l))
    ok &= graph.returncode == 0

    print("announced: what the keeper announced, read_committed")
    customers, _, transactions = events
    for name, key, ids in (("committed-customers", "customer_id",
                            [c["customer_id"] for c in customers]),
                           ("committed-transactions", "transaction_id",
                            [t["transaction_id"] for t in transactions])):
        rows = topic(name)
        count = collections.Counter(r[key] for r in rows)
        same = set(count) == set(ids)
        print(f"  {name}: {len(rows)} records, {len(count)} distinct, expected {len(ids)}"
              f"{'' if same else '  DIFFERENT'}"
              f"{f', {sum(n - 1 for n in count.values())} repeated' if len(rows) > len(count) else ''}")
        ok &= same

    print("alerts: the detectors' output, read_committed, against the demo's data")
    ok &= compare("alerts topic", topic("alerts"), want)
    ok &= compare("case store", cases(), want)
    ok &= compare("alert graph", alert_graph(db, want), want)

    print("check_run: " + ("PASS" if ok else "FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
