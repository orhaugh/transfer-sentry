#!/usr/bin/env python3
"""Transfers out of the bank's own accounts over four weeks, then a morning
with authorised push payment scams among them.

The demo's transactions give each payer two or three transfers a year, too
few for a profile to say what is usual for anyone. This writes a third
stream, transfers, on the demo's own accounts, so the graph knows each
payee it holds: four weeks of each payer's ordinary payments to a few
regular payees, then a morning on which:

  scam         four customers pay one account the graph does not know, each
               for the first time and each far above what they usually send;
               the first pays it twice. A fifth pays an account whose owner
               the graph links to a flagged customer, an ordinary amount.
  near misses  a new payee, an ordinary amount; five times the usual amount to
               a payee paid for weeks; six times the usual amount to a new
               payee from a customer with two transfers in four weeks; one
               payee paid for the first time by two customers; five times the
               usual amount to a payee first paid thirty hours earlier
  ordinary     payers paying their regular payees

Each payer has one usual amount, and each ordinary payment is 0.6 to 1.4
times it, so nothing in the history is three times a payer's average. Each
regular payee is paid by at most two payers, so none is paid by three new
payers in a day. Times are distinct across the stream, which is written in
time order. Seeded, so every run writes the same stream.

Usage: tools/transfer_history.py [--events state/demo-events] [--seed 20261026]
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
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def load_sibling(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load_sibling("identity")


class Ledger:
    def __init__(self, rng, accounts):
        self.rng = rng
        self.accounts = accounts
        self.transfers = []

    def outside(self):
        """An account outside the bank that the graph does not hold."""
        return {"account_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
                "account_number": str(self.rng.randrange(10**9, 10**10))}

    def payee(self, account_id):
        return self.accounts[account_id]

    def send(self, payer, payee, amount, ts):
        self.transfers.append({
            "transfer_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "from_account_id": payer,
            "from_account_number": self.accounts[payer]["account_number"],
            "to_account_id": payee["account_id"],
            "to_account_number": payee["account_number"],
            "amount": round(amount, 2),
            "ts": ts,
        })
        return self.transfers[-1]

    def second(self):
        return self.rng.randrange(0, 60) * 1000


def generate(customers, accounts, seed, morning):
    rng = random.Random(seed)
    by_id = {a["account_id"]: a for a in accounts}
    contexts = identity.account_contexts(customers, accounts)
    owned = sorted(a["account_id"] for a in accounts
                   if a["internal"] and contexts[a["account_id"]]["owner"] is not None)
    linked = [a for a in owned if contexts[a]["owner_flagged"] or contexts[a]["one"]]
    clean = [a for a in owned if a not in linked and not contexts[a]["two"]]
    # Payees the graph holds and has nothing against: internal accounts with no
    # owner, and outside accounts not in a high-risk jurisdiction.
    quiet = sorted(a["account_id"] for a in accounts
                   if (a["internal"] and contexts[a["account_id"]]["owner"] is None)
                   or (not a["internal"] and not a["high_risk_jurisdiction"]))
    rng.shuffle(linked)
    rng.shuffle(clean)
    rng.shuffle(quiet)
    if len(clean) < 14 or len(linked) < 1 or len(quiet) < 26:
        raise SystemExit(f"transfer_history: too few accounts (clean {len(clean)}, linked "
                         f"{len(linked)}, quiet {len(quiet)})")
    roles = {
        "victims": clean[:4],
        "linked_victim": clean[4:5],
        "near_misses": clean[5:11],
        "ordinary": clean[11:14],
        "scam_payee": [],
        "linked_payee": linked[:1],
    }
    payers = clean[:14]
    ledger = Ledger(rng, by_id)

    # Regular payees: three each, none shared by more than two payers. The
    # first two quiet accounts are kept back as the new payees of the morning.
    spare = quiet[:2]
    pool = quiet[2:]
    regular = {}
    for i, payer in enumerate(payers):
        regular[payer] = [pool[(i * 3 + k) % len(pool)] for k in range(3)]
    usual = {payer: rng.uniform(40, 400) for payer in payers}

    def ordinary(payer):
        return usual[payer] * rng.uniform(0.6, 1.4)

    # Four weeks of history, morning minus 27 days to morning minus an hour.
    thin = roles["near_misses"][2]
    for payer in payers:
        count = 2 if payer == thin else rng.randint(7, 10)
        for _ in range(count):
            ts = morning - rng.randint(1, 27 * 24) * HOUR + rng.randint(0, 59) * MINUTE
            ledger.send(payer, ledger.payee(rng.choice(regular[payer])), ordinary(payer),
                        ts + ledger.second())

    t = morning
    # The scam: four customers pay one account outside the bank, each for the
    # first time and each far above their usual amount. The first pays twice.
    scam = ledger.outside()
    roles["scam_payee"] = [scam["account_id"]]
    v = roles["victims"]
    ledger.send(v[0], scam, usual[v[0]] * rng.uniform(4, 8), t + 5 * MINUTE + ledger.second())
    ledger.send(v[0], scam, usual[v[0]] * rng.uniform(4, 8), t + 25 * MINUTE + ledger.second())
    ledger.send(v[1], scam, usual[v[1]] * rng.uniform(4, 8), t + 40 * MINUTE + ledger.second())
    ledger.send(v[2], scam, usual[v[2]] * rng.uniform(4, 8), t + 75 * MINUTE + ledger.second())
    # The fourth sends an amount that is ordinary for them.
    ledger.send(v[3], scam, ordinary(v[3]), t + 110 * MINUTE + ledger.second())
    # An ordinary amount to an account the graph links to a flagged customer.
    ledger.send(roles["linked_victim"][0], ledger.payee(roles["linked_payee"][0]),
                ordinary(roles["linked_victim"][0]), t + 140 * MINUTE + ledger.second())

    n = roles["near_misses"]
    # A new payee, an ordinary amount.
    ledger.send(n[0], ledger.payee(spare[0]), ordinary(n[0]), t + 15 * MINUTE + ledger.second())
    # Five times the usual amount to a payee paid for weeks.
    ledger.send(n[1], ledger.payee(regular[n[1]][0]), usual[n[1]] * 5,
                t + 30 * MINUTE + ledger.second())
    # Six times the usual amount to a new payee, from two transfers of history.
    ledger.send(n[2], ledger.outside(), usual[n[2]] * 6, t + 50 * MINUTE + ledger.second())
    # One payee paid for the first time by two customers.
    ledger.send(n[3], ledger.payee(spare[1]), ordinary(n[3]), t + 60 * MINUTE + ledger.second())
    ledger.send(n[4], ledger.payee(spare[1]), ordinary(n[4]), t + 90 * MINUTE + ledger.second())
    # Five times the usual amount to a payee first paid thirty hours earlier.
    newish = ledger.outside()
    ledger.send(n[5], newish, ordinary(n[5]), t - 30 * HOUR + 35 * MINUTE + ledger.second())
    ledger.send(n[5], newish, usual[n[5]] * 5, t + 65 * MINUTE + ledger.second())

    # Ordinary payers keep paying their regular payees.
    for payer in roles["ordinary"]:
        for k in range(2):
            ledger.send(payer, ledger.payee(regular[payer][k]), ordinary(payer),
                        t + rng.randint(5, 150) * MINUTE + ledger.second())

    # Distinct times across the stream, in time order: a later transfer at the
    # same second moves on by a millisecond.
    ledger.transfers.sort(key=lambda r: (r["ts"], r["transfer_id"]))
    last = None
    for r in ledger.transfers:
        if last is not None and r["ts"] <= last:
            r["ts"] = last + 1
        last = r["ts"]
    return ledger.transfers, roles


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--seed", type=int, default=20261026)
    args = ap.parse_args()
    customers, accounts, transactions = identity.read_events(
        args.events, "customers", "accounts", "transactions")
    last_day = max(t["ts"] for t in transactions)
    morning = last_day + 3 * DAY + 9 * HOUR  # 09:00 UTC three days after
    transfers, roles = generate(customers, accounts, args.seed, morning)
    out = Path(args.events)
    with (out / "transfers.jsonl").open("w") as fh:
        for r in transfers:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
    (out / "transfer-roles.json").write_text(json.dumps(roles, indent=2, sort_keys=True) + "\n")
    first = datetime.fromtimestamp(transfers[0]["ts"] / 1000, tz=timezone.utc)
    print(f"transfer_history: {len(transfers)} transfers from {first:%Y-%m-%d %H:%M} UTC, "
          f"{sum(len(roles[k]) for k in ('victims', 'linked_victim', 'near_misses', 'ordinary'))}"
          f" payers ({', '.join(f'{len(roles[k])} {k}' for k in roles)})")


if __name__ == "__main__":
    main()
