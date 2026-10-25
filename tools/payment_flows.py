#!/usr/bin/env python3
"""Payments into and out of the bank's own accounts, minute by minute, with
money mules among them.

The demo's transactions are a year at the granularity of a day: too coarse to
show an account taking money from many payers in an hour and sending it
straight on. This writes a second stream, payments, over a synthetic morning
the day after the demo's last transaction, on the demo's own internal
accounts, so the graph knows each account's owner and whom that owner is
connected to:

  mules        four accounts take a credit from a different payer every six
               to nine minutes, six to eight in all, and send 85% to 97% of
               each one on within two to five minutes, before the next lands.
               Two belong to owners the graph links to a flagged customer,
               two to owners it does not.
  near misses  three credits within an hour; 70% sent on; 95% sent on after
               45 minutes; a credit sent on after a second credit landed; five
               credits over three hours
  ordinary     accounts that receive a salary and pay a few bills
  known links  accounts with ordinary activity whose owners the graph links
               to a flagged customer, one step or two steps away

Each payment names the bank's account (the payee of a credit, the payer of a
debit), the other party's account, which is outside the bank, the direction,
the amount and the time. Times are distinct across the whole stream, which
is written in time order. Seeded, so every run writes the same stream.

Usage: tools/payment_flows.py [--events state/demo-events] [--seed 20261025]
"""

import argparse
import importlib.util
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MINUTE = 60_000


def load_sibling(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load_sibling("identity")


class Flows:
    def __init__(self, rng, accounts):
        self.rng = rng
        self.accounts = accounts
        self.payments = []
        self.externals = 0

    def external(self):
        self.externals += 1
        return {"id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
                "number": str(self.rng.randrange(10**9, 10**10))}

    def pay(self, account_id, direction, amount, ts, counterparty=None):
        other = counterparty or self.external()
        self.payments.append({
            "payment_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "account_id": account_id,
            "account_number": self.accounts[account_id]["account_number"],
            "counterparty_id": other["id"],
            "counterparty_number": other["number"],
            "direction": direction,
            "amount": round(amount, 2),
            "ts": ts,
        })
        return self.payments[-1]

    def second(self):
        return self.rng.randrange(0, 60) * 1000


def generate(customers, accounts, seed, start):
    rng = random.Random(seed)
    by_id = {a["account_id"]: a for a in accounts}
    contexts = identity.account_contexts(customers, accounts)
    internal = sorted(a["account_id"] for a in accounts
                      if a["internal"] and contexts[a["account_id"]]["owner"] is not None)
    linked = [a for a in internal if contexts[a]["owner_flagged"] or contexts[a]["one"]]
    near = [a for a in internal if not (contexts[a]["owner_flagged"] or contexts[a]["one"])
            and contexts[a]["two"]]
    clean = [a for a in internal if a not in linked and a not in near]
    rng.shuffle(linked)
    rng.shuffle(near)
    rng.shuffle(clean)
    if len(linked) < 4 or len(near) < 2 or len(clean) < 15:
        raise SystemExit(f"payment_flows: too few accounts (linked {len(linked)}, near "
                         f"{len(near)}, clean {len(clean)})")
    roles = {
        "mules": linked[:2] + clean[:2],
        "near_misses": clean[2:7],
        "ordinary": clean[7:15],
        "known_links": linked[2:4] + near[:2],
    }
    f = Flows(rng, by_id)

    # Mules: a credit every six to nine minutes, each sent on before the next.
    for k, account in enumerate(roles["mules"]):
        t = start + k * 7 * MINUTE
        for _ in range(rng.randint(6, 8)):
            t += rng.randint(6, 9) * MINUTE + f.second()
            credit = f.pay(account, "credit", rng.uniform(150, 2400), t)
            onward = t + rng.randint(2, 5) * MINUTE + f.second()
            f.pay(account, "debit", credit["amount"] * rng.uniform(0.85, 0.97), onward)

    near_misses = roles["near_misses"]
    t = start + 5 * MINUTE
    # Three credits within the hour: one short of the fan-in rule.
    for i in range(3):
        f.pay(near_misses[0], "credit", rng.uniform(100, 900), t + i * 15 * MINUTE + f.second())
    # 70% sent on: below the share the pass-through rule asks for.
    c = f.pay(near_misses[1], "credit", rng.uniform(500, 2000), t + 20 * MINUTE + f.second())
    f.pay(near_misses[1], "debit", c["amount"] * 0.70, c["ts"] + 4 * MINUTE)
    # 95% sent on, but after 45 minutes: outside the rule's half hour.
    c = f.pay(near_misses[2], "credit", rng.uniform(500, 2000), t + 25 * MINUTE + f.second())
    f.pay(near_misses[2], "debit", c["amount"] * 0.95, c["ts"] + 45 * MINUTE)
    # 95% of the first credit sent on after a second, smaller credit landed:
    # the debit follows a credit it is not 80% to 100% of.
    c1 = f.pay(near_misses[3], "credit", 1000.0, t + 30 * MINUTE + f.second())
    f.pay(near_misses[3], "credit", 300.0, c1["ts"] + 3 * MINUTE)
    f.pay(near_misses[3], "debit", 950.0, c1["ts"] + 6 * MINUTE)
    # Five credits over three hours: never four within one hour.
    for i in range(5):
        f.pay(near_misses[4], "credit", rng.uniform(100, 900), t + i * 40 * MINUTE + f.second())

    # Ordinary accounts and the known links: a salary, then a few bills.
    for account in roles["ordinary"] + roles["known_links"]:
        t = start + rng.randint(0, 120) * MINUTE + f.second()
        f.pay(account, "credit", rng.uniform(1800, 4200), t)
        for _ in range(rng.randint(2, 3)):
            t += rng.randint(3, 25) * MINUTE + f.second()
            f.pay(account, "debit", rng.uniform(15, 250), t)

    # Distinct times across the stream, in time order: a later payment at the
    # same second moves on by a millisecond.
    f.payments.sort(key=lambda p: (p["ts"], p["payment_id"]))
    last = None
    for p in f.payments:
        if last is not None and p["ts"] <= last:
            p["ts"] = last + 1
        last = p["ts"]
    return f.payments, roles


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--seed", type=int, default=20261025)
    args = ap.parse_args()
    customers, accounts, transactions = identity.read_events(
        args.events, "customers", "accounts", "transactions")
    last_day = max(t["ts"] for t in transactions)
    start = last_day + 24 * 60 * MINUTE + 9 * 60 * MINUTE  # 09:00 UTC the day after
    payments, roles = generate(customers, accounts, args.seed, start)
    out = Path(args.events)
    with (out / "payments.jsonl").open("w") as fh:
        for p in payments:
            fh.write(json.dumps(p, sort_keys=True) + "\n")
    (out / "payment-roles.json").write_text(json.dumps(roles, indent=2, sort_keys=True) + "\n")
    first = datetime.fromtimestamp(payments[0]["ts"] / 1000, tz=timezone.utc)
    print(f"payment_flows: {len(payments)} payments from {first:%Y-%m-%d %H:%M} UTC on "
          f"{sum(len(v) for v in roles.values())} accounts "
          f"({', '.join(f'{len(v)} {k}' for k, v in roles.items())})")


if __name__ == "__main__":
    main()
