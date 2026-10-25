#!/usr/bin/env python3
"""Send transactions, payments or transfers onto their topic at a steady pace, each
stamped with the moment it was sent.

The decision paths' latency is measured from when each one reached Kafka (the
topics are stamped with LogAppendTime). sent_at, the wall-clock
milliseconds at which this script handed the transfer to the producer,
measures the feeder's own path to Kafka, reported apart. The producer runs
inside the broker container with linger.ms=0, so a record leaves as soon as
it is read rather than waiting to fill a batch, and is given a few seconds to
start before the first transfer, so the first ones are not held while its JVM
starts and then arrive together.

  --stream S        transactions (the default), payments or transfers
  --from N --to M   which events, by position in the stream (0-based, M
                    exclusive; default all)
  --pace-ms P       wait P ms between events (default 20)

Usage: tools/feed_transactions.py [--stream transactions] [--from 0] [--to 300]
                                  [--pace-ms 20] [--events state/demo-events]
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WARM_UP_S = 4


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", default=str(ROOT / "state" / "demo-events"))
    ap.add_argument("--stream", choices=("transactions", "payments", "transfers"), default="transactions")
    ap.add_argument("--from", dest="start", type=int, default=0)
    ap.add_argument("--to", dest="stop", type=int, default=None)
    ap.add_argument("--pace-ms", type=int, default=20)
    args = ap.parse_args()

    with (Path(args.events) / f"{args.stream}.jsonl").open() as f:
        events = [json.loads(line) for line in f][args.start:args.stop]
    producer = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", "kafka:9092", "--topic", args.stream,
         "--producer-property", "linger.ms=0",
         "--producer-property", "enable.idempotence=true",
         "--producer-property", "acks=all"],
        cwd=ROOT, stdin=subprocess.PIPE, text=True)
    time.sleep(WARM_UP_S)
    for event in events:
        event["sent_at"] = time.time_ns() // 1_000_000
        producer.stdin.write(json.dumps(event, sort_keys=True) + "\n")
        producer.stdin.flush()
        time.sleep(args.pace_ms / 1000)
    producer.stdin.close()
    code = producer.wait()
    print(f"feed_transactions: {len(events)} {args.stream}, one every {args.pace_ms} ms")
    sys.exit(code)


if __name__ == "__main__":
    main()
