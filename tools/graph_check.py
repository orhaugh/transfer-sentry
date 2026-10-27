#!/usr/bin/env python3
"""Check the graph keeper's Cypher against a real Neo4j, without clink.

Takes the keeper's three graph statements from pipeline/keeper.sql (so there
is one copy of each) and applies them to the demo events the way clink's
Neo4j sink does: in batches, one transaction per batch, the rows as $rows.
Then it runs the demo's own dashboard queries, read unchanged from the demo's
dashboard file, and compares their answers with tools/demo_reference.py's.

  load      the events once, in sink-sized batches, in stream order
  replay    the events three times: once in small batches with every
            transaction batch holding a copy of its own first row, with the
            transactions before the accounts and customers they name, then
            twice more as a restore would replay them; the graph must come out
            the same as one clean load
  hazard    shows what the per-batch reduction in the transaction statement
            prevents: without it, a batch holding one transaction twice gives
            it two SENT relationships

  detectors the detectors' lookups from pipeline/detectors.sql, run for every
            transaction and every customer as clink's lookup runs them (one
            read per key, $key bound, read mode), against what the demo's data
            says each should find

With --existing it loads nothing: it checks the graph already in Neo4j (the
one clink's keeper wrote) against the export, the dashboard and the
detectors, and leaves it as it is. --reset empties the graph and applies
neo4j/schema.cypher, for a fresh run.

Usage: tools/graph_check.py [--existing | --reset] [--endpoint http://localhost:7474]
                            [--events state/demo-events]
"""

import argparse
import base64
import hashlib
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class Neo4j:
    def __init__(self, endpoint, user, password, database="neo4j"):
        self.url = f"{endpoint}/db/{database}/query/v2"
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Content-Type": "application/json",
                        "Accept": "application/json",
                        "Authorization": f"Basic {token}"}

    def run(self, statement, read=False, **params):
        request = {"statement": statement, "parameters": params}
        if read:
            request["accessMode"] = "READ"
        body = json.dumps(request).encode()
        req = urllib.request.Request(self.url, data=body, headers=self.headers)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                reply = json.load(resp)
        except urllib.error.HTTPError as e:
            reply = json.load(e)
        if reply.get("errors"):
            raise RuntimeError(f"{reply['errors'][0].get('code')}: "
                               f"{reply['errors'][0].get('message')}")
        data = reply.get("data", {})
        return [dict(zip(data.get("fields", []), v)) for v in data.get("values", [])]


def sql_statements(path):
    """table name -> the statement option of each CREATE TABLE that has one."""
    sql = Path(path).read_text()
    out = {}
    for m in re.finditer(r"CREATE TABLE (\w+)", sql):
        end = sql.find(";", m.end())
        body = sql[m.end():end if end >= 0 else len(sql)]
        s = re.search(r"statement\s*=\s*'", body)
        if not s:
            continue
        # A SQL string literal: '' is a quote, a lone ' ends it. The table's
        # text up to its ';' can hold a ';' inside the literal, so read on
        # from the full text rather than the slice.
        i = m.end() + s.end()
        buf = []
        while True:
            q = sql.index("'", i)
            buf.append(sql[i:q])
            if sql[q + 1:q + 2] == "'":
                buf.append("'")
                i = q + 2
            else:
                break
        out[m.group(1)] = "".join(buf)
    return out


def dashboard_queries(path):
    d = json.loads(Path(path).read_text())
    out = {}
    for page in d.get("pages", []):
        for rep in page.get("reports", []):
            out[rep.get("title")] = rep.get("query", "")
    return out


def read_events(events):
    load = lambda n: [json.loads(l) for l in (Path(events) / f"{n}.jsonl").open()]
    return load("customers"), load("accounts"), load("transactions")


def reset(db):
    db.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS")
    text = "\n".join(l for l in (ROOT / "neo4j" / "schema.cypher").read_text().splitlines()
                     if not l.strip().startswith("//"))
    for stmt in text.split(";"):
        if stmt.strip():
            db.run(stmt)


def apply(db, statement, rows, batch):
    for i in range(0, len(rows), batch):
        db.run(statement, rows=rows[i:i + batch])


def fingerprint(db):
    """Every node the keeper writes with its labels and properties, and every
    relationship between them, as one digest plus counts. Alert nodes are the
    case router's, and are left out."""
    key = ("coalesce(n.customerID, n.accountID, n.transactionID, n.emailID, "
           "n.phoneNumberID, n.addressID, n.jurisdictionID)")
    nodes = db.run(f"MATCH (n) WHERE NOT n:Alert WITH n, {key} AS k "
                   "RETURN k, labels(n) AS labels, properties(n) AS props ORDER BY k")
    rels = db.run(f"MATCH (n)-[r]->(m) WHERE NOT n:Alert AND NOT m:Alert "
                  f"WITH {key} AS a, type(r) AS t, m "
                  f"WITH a, t, {key.replace('n.', 'm.')} AS b "
                  "RETURN a, t, b ORDER BY a, t, b")
    h = hashlib.sha256()
    for n in nodes:
        n["labels"] = sorted(n["labels"])
        h.update(json.dumps(n, sort_keys=True).encode())
    for r in rels:
        h.update(json.dumps(r, sort_keys=True).encode())
    counts = {}
    for n in nodes:
        for label in n["labels"]:
            counts[label] = counts.get(label, 0) + 1
    for r in rels:
        counts[r["t"]] = counts.get(r["t"], 0) + 1
    return h.hexdigest(), counts


def load_clean(db, stmts, events):
    customers, accounts, transactions = events
    apply(db, stmts["customer_graph"], customers, 200)
    apply(db, stmts["account_graph"], accounts, 200)
    apply(db, stmts["transaction_graph"], transactions, 200)


def load_replayed(db, stmts, events):
    customers, accounts, transactions = events
    # Transactions first: each creates its accounts with their numbers only,
    # and the account and customer events fill them in afterwards.
    batches = [transactions[i:i + 7] for i in range(0, len(transactions), 7)]
    for b in batches:
        db.run(stmts["transaction_graph"], rows=b + b[:1])
    apply(db, stmts["account_graph"], accounts + accounts[:5], 11)
    apply(db, stmts["customer_graph"], customers + customers[:5], 13)
    for _ in range(2):
        apply(db, stmts["customer_graph"], customers, 200)
        apply(db, stmts["account_graph"], accounts, 200)
        apply(db, stmts["transaction_graph"], transactions, 50)


def csv_rows(data, name):
    import csv
    with (Path(data) / f"export_transactions_{name}.csv").open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def epoch_ms(iso):
    from datetime import datetime, timezone
    iso = iso.replace("Z", "+00:00")
    return int(datetime.fromisoformat(iso).astimezone(timezone.utc).timestamp() * 1000)


def check_against_export(db, data):
    """The graph clink keeps against the demo's own export: every relationship
    the pipeline models, pair for pair, and every property it carries, value
    for value. What the demo computed with graph algorithms (communities,
    PageRank, the TRANSACTED_TO and SHARED_IDENTIFIERS summaries) is not part
    of the event streams and is left out."""
    rels = [
        ("SENT", "Account", "accountID", "Transaction", "transactionID",
         "Account_SENT_Transaction"),
        ("RECEIVED", "Transaction", "transactionID", "Account", "accountID",
         "Transaction_RECEIVED_Account"),
        ("HAS_ACCOUNT", "Customer", "customerID", "Account", "accountID",
         "Customer_HAS_ACCOUNT_Account"),
        ("USES_EMAIL", "Customer", "customerID", "Email", "emailID",
         "Customer_USES_EMAIL_Email"),
        ("USES_PHONE", "Customer", "customerID", "PhoneNumber", "phoneNumberID",
         "Customer_USES_PHONE_PhoneNumber"),
        ("RESIDES_AT", "Customer", "customerID", "Address", "addressID",
         "Customer_RESIDES_AT_Address"),
        ("LOCATED_IN", "Address", "addressID", "Jurisdiction", "jurisdictionID",
         "Address_LOCATED_IN_Jurisdiction"),
    ]
    ok = True
    for rel, a, ak, b, bk, name in rels:
        rows = csv_rows(data, name)
        start, end = list(rows[0].keys())[:2]
        want = sorted({(r[start], r[end]) for r in rows})
        got = sorted((r["a"], r["b"]) for r in db.run(
            f"MATCH (x:{a})-[:{rel}]->(y:{b}) RETURN x.{ak} AS a, y.{bk} AS b"))
        same = got == want
        ok &= same
        print(f"  {rel:<12} {len(got):>4} of the export's {len(want):>4}  "
              f"{'same pairs' if same else 'DIFFERENT'}")

    props = [
        ("Transaction", "transactionID", "Transaction",
         {"date": "date", "amount": "amount", "type": "type", "status": "status"}),
        ("Account", "accountID", "Account_with_labels",
         {"accountNumber": "accountNumber", "accountType": "accountType",
          "status": "status", "internalExternal": "internalExternal",
          "highRiskJurisdiction": "highRiskJurisdiction"}),
        ("Customer", "customerID", "Customer", {"name": "name", "fraudFlag": "fraudFlag"}),
        ("Email", "emailID", "Email", {"emailAddress": "emailAddress"}),
        ("PhoneNumber", "phoneNumberID", "PhoneNumber", {"number": "number"}),
        ("Address", "addressID", "Address",
         {"street": "street", "city": "city", "state": "state", "country": "country"}),
        ("Jurisdiction", "jurisdictionID", "Jurisdiction", {"name": "name", "riskLevel": "riskLevel"}),
    ]

    def norm(prop, v):
        if v is None:
            return None
        if prop == "date":
            return epoch_ms(v)
        if prop == "amount":
            return round(float(v), 2)
        if prop in ("fraudFlag", "highRiskJurisdiction"):
            return str(v).lower() == "true"
        return str(v)

    for label, key, name, mapping in props:
        export = {r[key]: r for r in csv_rows(data, name)}
        ret = ", ".join(f"n.{p} AS {p}" for p in mapping)
        graph = {r["k"]: r for r in db.run(f"MATCH (n:{label}) RETURN n.{key} AS k, {ret}")}
        missing = sorted(set(export) - set(graph))
        wrong = [k for k in graph if k in export and any(
            norm(p, graph[k][p]) != norm(p, export[k][c]) for p, c in mapping.items())]
        extra = sorted(set(graph) - set(export))
        same = not wrong and not extra and not missing
        note = ""
        if missing and label == "Jurisdiction" and not wrong and not extra:
            # Only jurisdictions someone lives in reach the event streams.
            same, note = True, f", {len(missing)} with no resident not carried"
        ok &= same
        print(f"  {label:<12} {len(graph):>4} nodes, properties "
              f"{'as exported' if not wrong else f'DIFFERENT on {len(wrong)}'}"
              f"{', MISSING ' + str(len(missing)) if missing and not note else ''}"
              f"{', EXTRA ' + str(len(extra)) if extra else ''}{note}")
    return ok


def check_dashboard(db, queries, reference):
    ok = True
    rings = db.run(queries["Rings"])
    got = sorted((r["ringSize"], r["EntryAccount"]) for r in rings)
    want = sorted((r["ring_size"], r["entry_account"]) for r in reference["rings"])
    print(f"  Rings: {len(got)} found, reference {len(want)}"
          f"  {'OK' if got == want else 'DIFFERENT'}")
    for size, entry in got:
        print(f"    entry {entry}  size {size}")
    ok &= got == want

    pii = db.run(queries["Customers Sharing PII"])
    ref = {(r["kind"], r["value"]): r["customers"] for r in reference["shared_pii"]}
    rows_ok = all(ref.get((r["itemType"], r["identity"])) == r["nbSharedIdentifierRelationships"]
                  for r in pii)
    counts = [r["nbSharedIdentifierRelationships"] for r in pii]
    want_counts = [r["customers"] for r in reference["shared_pii"][:len(pii)]]
    same = rows_ok and counts == want_counts and len(pii) == min(10, len(reference["shared_pii"]))
    print(f"  Customers Sharing PII: {len(pii)} rows, counts {counts}"
          f"  {'OK' if same else 'DIFFERENT'}")
    ok &= same

    hrj = sorted(r["l.accountNumber"] for r in db.run(queries["High Risk Accounts"]))
    same = hrj == reference["high_risk_accounts"]
    print(f"  High Risk Accounts: {len(hrj)} rows for {len(set(hrj))} accounts, reference "
          f"{len(reference['high_risk_accounts'])}  {'OK' if same else 'DIFFERENT'}")
    ok &= same
    return ok


def expected_rings(reference):
    """closing transaction id -> what the ring lookup should return for it."""
    by_close = {}
    for r in reference["rings"]:
        by_close.setdefault(r["transactions"][-1], []).append(r)
    out = {}
    for tid, found in by_close.items():
        best = min(found, key=lambda r: (r["ring_size"], r["transactions"]))
        out[tid] = {"ring_size": best["ring_size"], "rings": len(found),
                    "entry_account": best["entry_account"],
                    "ring_accounts": ",".join(best["accounts"]),
                    "ring_transactions": ",".join(best["transactions"])}
    return out


def expected_identity(customers):
    """customer id -> what the identity lookup should return for them."""
    kinds = (("email_id", "USES_EMAIL"), ("phone_id", "USES_PHONE"), ("address_id", "RESIDES_AT"))
    order = lambda c: (c["ts"], c["customer_id"])
    out = {}
    for c in customers:
        earlier, found_kinds = [], set()
        for o in customers:
            if order(o) >= order(c):
                continue
            shared = [k for field, k in kinds if c[field] is not None and c[field] == o[field]]
            if shared:
                earlier.append(o)
                found_kinds.update(shared)
        if earlier:
            earlier.sort(key=lambda o: o["customer_id"])
            out[c["customer_id"]] = {
                "shared_customers": len(earlier),
                "shared_with_fraud": sum(o["fraud_flag"] for o in earlier),
                "shared_kinds": ",".join(sorted(found_kinds)),
                "evidence_customers": ",".join(o["customer_id"] for o in earlier[:10])}
    return out


def check_detectors(db, lookups, events, reference):
    customers, _, transactions = events
    ok = True
    for name, keys, want in (
            ("ring_context", [t["transaction_id"] for t in transactions], expected_rings(reference)),
            ("identity_context", [c["customer_id"] for c in customers], expected_identity(customers)),
            ("trace_context", [t["transaction_id"] for t in transactions],
             reference["high_risk_traces"])):
        got = {}
        for k in keys:
            rows = db.run(lookups[name], read=True, key=k)
            if len(rows) > 1:
                raise RuntimeError(f"{name} returned {len(rows)} rows for one key")
            if rows:
                got[k] = {f: rows[0][f] for f in next(iter(want.values()))}
        same = got == want
        ok &= same
        print(f"  {name}: {len(keys)} reads, {len(got)} matches, expected {len(want)}"
              f"  {'OK' if same else 'DIFFERENT'}")
        if not same:
            for k in sorted(set(got) | set(want)):
                if got.get(k) != want.get(k):
                    print(f"    {k}: got {got.get(k)} want {want.get(k)}")
        if name == "ring_context":
            for k, v in sorted(got.items(), key=lambda kv: kv[1]["entry_account"]):
                print(f"    {k} closes a ring of {v['ring_size']} entered at {v['entry_account']}: "
                      f"{v['ring_accounts']}")
        elif name == "trace_context":
            for k, v in got.items():
                print(f"    {k} pays high-risk account {v['high_risk_account']} what came from "
                      f"{v['origin_account']}: {v['trace_accounts']}")
        else:
            flagged = sum(1 for v in got.values() if v["shared_with_fraud"])
            print(f"    {flagged} of them share with a customer the demo flags as fraud")
    return ok


def hazard(db, stmts, events):
    """The transaction statement without its per-batch reduction."""
    _, _, transactions = events
    naive = stmts["transaction_graph"].replace(
        "WITH row.transaction_id AS id, collect(row)[0] AS row\n", "").replace(
        "{transactionID: id}", "{transactionID: row.transaction_id}")
    assert naive != stmts["transaction_graph"]
    reset(db)
    t = transactions[0]
    db.run(naive, rows=[t, t])
    naive_sent = db.run("MATCH (:Account)-[s:SENT]->(t:Transaction {transactionID: $id}) "
                        "RETURN count(s) AS n", id=t["transaction_id"])[0]["n"]
    reset(db)
    db.run(stmts["transaction_graph"], rows=[t, t])
    sent = db.run("MATCH (:Account)-[s:SENT]->(t:Transaction {transactionID: $id}) "
                  "RETURN count(s) AS n", id=t["transaction_id"])[0]["n"]
    print(f"  one batch holding a transaction twice: {naive_sent} SENT relationship(s) "
          f"without the reduction, {sent} with it")
    return sent == 1


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--endpoint", default="http://localhost:7474")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--password", default="transfer-sentry-pw")
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--existing", action="store_true",
                      help="check the graph already in Neo4j; load nothing")
    mode.add_argument("--reset", action="store_true",
                      help="empty the graph and apply the schema, then stop")
    args = ap.parse_args()

    db = Neo4j(args.endpoint, args.user, args.password)
    stmts = sql_statements(ROOT / "pipeline" / "keeper.sql")
    queries = dashboard_queries(ROOT / "data" / "neo4j-demo" / "src" / "neodash"
                                / "fraud-dashboard.json")
    reference = json.loads(subprocess.run(
        [sys.executable, "-I", str(ROOT / "tools" / "demo_reference.py"), "--json"],
        check=True, capture_output=True, text=True).stdout)
    events = read_events(args.events)
    ok = True

    if args.reset:
        reset(db)
        print("graph_check: graph emptied, schema applied")
        return
    if args.existing:
        print("existing: the graph as clink's keeper left it")
    else:
        print("load: one clean load in stream order")
        reset(db)
        load_clean(db, stmts, events)
    clean, counts = fingerprint(db)
    print(f"  {counts}")
    print("export: the graph against the demo's own export")
    ok &= check_against_export(db, ROOT / "data" / "neo4j-demo" / "data")
    print("dashboard: the demo's queries, unchanged, on the graph")
    ok &= check_dashboard(db, queries, reference)

    print("detectors: every transaction and customer read as the detectors read them")
    ok &= check_detectors(db, sql_statements(ROOT / "pipeline" / "detectors.sql"),
                          events, reference)
    saved = ROOT / "state" / "clean-graph.json"
    if args.existing:
        # Every node, label, property and relationship, against the checker's
        # own clean load of the same statements (saved by a run without
        # --existing): leftover _lock or _created markers, a stray label or a
        # duplicate relationship would all show here.
        if saved.exists():
            want = json.loads(saved.read_text())
            same = want["digest"] == clean
            print(f"  same graph as the checker's clean load: {'yes' if same else 'NO'}")
            if not same:
                print(f"  {want['counts']}")
            ok &= same
        else:
            print("  (no saved clean load to compare with: run tools/graph_check.py once first)")
        print("graph_check: " + ("PASS" if ok else "FAIL"))
        sys.exit(0 if ok else 1)
    saved.parent.mkdir(parents=True, exist_ok=True)
    saved.write_text(json.dumps({"digest": clean, "counts": counts}, indent=1))

    print("replay: out of order, duplicated in and across batches, applied three times")
    reset(db)
    load_replayed(db, stmts, events)
    replayed, rcounts = fingerprint(db)
    same = replayed == clean
    print(f"  same graph as the clean load: {'yes' if same else 'NO'}")
    if not same:
        print(f"  {rcounts}")
    ok &= same

    print("hazard: why the transaction statement reduces each batch first")
    ok &= hazard(db, stmts, events)

    # Leave the clean graph behind for a look in Neo4j Browser.
    reset(db)
    load_clean(db, stmts, events)
    print("graph_check: " + ("PASS" if ok else "FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
