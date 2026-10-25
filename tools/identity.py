"""Who an account's owner is connected to, computed from the event streams.

Shared by the checks and the payment generator, so that "linked to a flagged
customer" means one thing everywhere: the owner shares an email address,
phone number or street address with a customer the bank has flagged as fraud
(one step), or shares one with a customer who shares one with a flagged
customer (two steps). The lookups in pipeline/decisions.sql and
pipeline/inbound.sql ask the graph the same question.
"""

import collections
import json
from pathlib import Path

KINDS = ("email_id", "phone_id", "address_id")


def read_events(events, *names):
    """The named streams of an events directory, each a list of dicts."""
    return [[json.loads(line) for line in (Path(events) / f"{n}.jsonl").open()] for n in names]


def account_contexts(customers, accounts):
    """account id -> {owner, owner_flagged, one, two, high_risk}.

    one: the flagged customers one shared identifier from the owner; two: those
    two steps away and not already one step away. An account with no owner
    among the customers has neither.
    """
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

    contexts = {}
    for a in accounts:
        owner = a["customer_id"]
        if owner is None or owner not in by_id:
            flagged, one, two = False, set(), set()
        else:
            flagged = by_id[owner]["fraud_flag"]
            near = shares(owner)
            one = {o for o in near if by_id[o]["fraud_flag"]}
            two = set()
            for m in near:
                for o in shares(m):
                    if o not in (owner, m) and by_id[o]["fraud_flag"] and o not in one:
                        two.add(o)
        contexts[a["account_id"]] = {"owner": owner, "owner_flagged": flagged, "one": one,
                                     "two": two, "high_risk": a["high_risk_jurisdiction"]}
    return contexts


def evidence(context):
    """The flagged customers behind a context, as the lookups list them."""
    return ",".join((sorted(context["one"]) + sorted(context["two"]))[:10])
