#!/usr/bin/env bash
# Produce the demo's event streams onto their topics, in the order a bank's
# systems would emit them: onboardings, then account openings, then
# transactions in date order. The producer runs inside the broker container,
# so nothing beyond Docker is needed on the host.
#
#   scripts/feed.sh [all|onboard|first|second|retry] [events dir]
#     all      everything (the default)
#     onboard  the customers and the accounts, no transactions
#     first    the customers, the accounts and the first half of the
#              transactions
#     second   the rest of the transactions
#     retry    the 20 transactions before the halfway point again: a
#              producer that retried a batch it had delivered
#   PACE_MS=50 scripts/feed.sh second
#              one record every 50 ms, so the stream is still flowing while
#              something else happens
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PART="${1:-all}"
EVENTS="${2:-${ROOT}/state/demo-events}"
cd "${ROOT}"

produce() {  # <topic> <file>
  local pace="${PACE_MS:-0}"
  if [ "${pace}" -gt 0 ]; then
    while IFS= read -r line; do
      printf '%s\n' "${line}"
      sleep "$(printf '0.%03d' "${pace}")"
    done <"$2"
  else
    cat "$2"
  fi | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic "$1" \
    --producer-property enable.idempotence=true --producer-property acks=all
  echo "feed: $(wc -l <"$2" | tr -d ' ') records -> $1"
}

total="$(wc -l <"${EVENTS}/transactions.jsonl" | tr -d ' ')"
half=$((total / 2))
part="$(mktemp)"
trap 'rm -f "${part}"' EXIT
case "${PART}" in
  onboard)
    produce customers "${EVENTS}/customers.jsonl"
    produce accounts "${EVENTS}/accounts.jsonl"
    ;;
  all | first)
    produce customers "${EVENTS}/customers.jsonl"
    produce accounts "${EVENTS}/accounts.jsonl"
    if [ "${PART}" = all ]; then
      produce transactions "${EVENTS}/transactions.jsonl"
    else
      head -n "${half}" "${EVENTS}/transactions.jsonl" >"${part}"
      produce transactions "${part}"
    fi
    ;;
  second)
    tail -n "+$((half + 1))" "${EVENTS}/transactions.jsonl" >"${part}"
    produce transactions "${part}"
    ;;
  retry)
    head -n "${half}" "${EVENTS}/transactions.jsonl" | tail -n 20 >"${part}"
    produce transactions "${part}"
    ;;
  *)
    echo "feed.sh: unknown part '${PART}' (all, onboard, first, second or retry)" >&2
    exit 2
    ;;
esac
