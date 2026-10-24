#!/usr/bin/env bash
# Run transfer-sentry end to end, a Worker kill included, and check every
# store it writes against what the demo's data says.
#
#   CLINK_BIN_DIR=<clink build dir> ./run.sh   clink from a local build
#   CLEANUP=1 ... ./run.sh                     and remove everything at the end
#
# Steps: fetch the demo and turn it into event streams; start the broker, the
# graph and the case store; apply their schemas; start a Coordinator and two
# Workers; submit the five jobs (pipeline/keeper.sql, detectors.sql,
# cases.sql, decisions.sql, audit.sql); send the onboardings and the accounts
# and wait until the graph holds them; send the first half of the
# transactions one every 20 ms, each stamped with when it was sent, and the
# second half one every 50 ms, killing a Worker with SIGKILL while they flow
# and starting it again; resend a batch, as a producer that retried would; then tools/check_run.py checks the graph, the
# announcements, the alerts, the case store and the alert graph, and
# tools/check_decisions.py every decision, the decision log and the latency.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

: "${CLINK_BIN_DIR:?set CLINK_BIN_DIR to a clink build directory (clink_node and clink)}"
BIN="$(cd "${CLINK_BIN_DIR}" && pwd)"
for b in clink_node clink; do
  [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-25432}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-8081}"
export KAFKA_HOST_PORT NEO4J_HTTP_PORT POSTGRES_HOST_PORT
# Read by the Workers for every Neo4j table's env://NEO4J_PASSWORD.
export NEO4J_PASSWORD=transfer-sentry-pw
STATE="${ROOT}/state/run"

say() { printf '\n==> %s\n' "$*"; }

pids=()
worker1_pid=""
cleanup() {
  local pid
  for pid in "${pids[@]}" ${worker1_pid}; do
    kill "${pid}" 2>/dev/null || true
  done
  # A process that has not stopped after ten seconds is killed.
  for _ in $(seq 1 10); do
    alive=0
    for pid in "${pids[@]}" ${worker1_pid}; do
      kill -0 "${pid}" 2>/dev/null && alive=1
    done
    [ "${alive}" = 0 ] && break
    sleep 1
  done
  for pid in "${pids[@]}" ${worker1_pid}; do
    kill -9 "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "${CLEANUP:-0}" = 1 ]; then
    docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

await_http() {  # <url> <what>
  for _ in $(seq 1 60); do
    curl -fsS "$1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "run.sh: $2 did not come up" >&2
  exit 1
}

start_worker() {  # <n>
  "${BIN}/clink_node" --role=worker --id="worker-$1" --coordinator-host=127.0.0.1 \
    --coordinator-port=16123 --port=$((16123 + $1)) --data-host=127.0.0.1 --slots=32 \
    --http-port=$((18081 + $1)) >>"${STATE}/worker-$1.log" 2>&1 &
  last_pid=$!
  await_http "http://127.0.0.1:$((18081 + $1))/api/v1/health" "Worker $1"
}

say "The demo's data, as the event streams a bank would emit"
scripts/fetch-demo.sh
python3 -I tools/demo_to_events.py --out state/demo-events

say "Starting the broker, the graph and the case store, from empty"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --wait
for i in $(seq 1 60); do
  python3 -I tools/graph_check.py --reset >/dev/null 2>&1 && break
  [ "${i}" = 60 ] && { echo "run.sh: Neo4j's Query API did not answer" >&2; exit 1; }
  sleep 2
done
echo "graph: emptied, neo4j/schema.cypher applied"
docker compose exec -T postgres psql -q -U sentry -d sentry -v ON_ERROR_STOP=1 \
  -c "DROP TABLE IF EXISTS cases" -f - <postgres/schema.sql
echo "case store: postgres/schema.sql applied"

say "Starting a Coordinator and two Workers, and submitting the three jobs"
rm -rf "${STATE}" && mkdir -p "${STATE}"
"${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16123 \
  --http-port="${CLINK_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator.log" 2>&1 &
pids+=($!)
await_http "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" "the Coordinator"
start_worker 1
worker1_pid="${last_pid}"
start_worker 2
pids+=("${last_pid}")
for job in keeper detectors cases decisions audit; do
  # The same SQL, reaching the services through their host ports.
  sed -e "s#kafka:9092#localhost:${KAFKA_HOST_PORT}#g" \
      -e "s#http://neo4j:7474#http://localhost:${NEO4J_HTTP_PORT}#g" \
      -e "s#host=postgres port=5432#host=localhost port=${POSTGRES_HOST_PORT}#g" \
      "pipeline/${job}.sql" >"${STATE}/${job}.sql"
  "${BIN}/clink" run "${STATE}/${job}.sql" --coordinator-host=127.0.0.1 \
    --coordinator-port="${CLINK_HTTP_PORT}" --name="${job}" \
    --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000
done

graph_count() {  # <cypher returning one count>
  curl -s -u "neo4j:${NEO4J_PASSWORD}" -H 'Content-Type: application/json' \
    -d "{\"statement\":\"$1\"}" \
    "http://localhost:${NEO4J_HTTP_PORT}/db/neo4j/query/v2" |
    python3 -I -c 'import json,sys; print(json.load(sys.stdin)["data"]["values"][0][0])' \
    2>/dev/null || echo 0
}

say "Sending the onboardings and the accounts"
scripts/feed.sh onboard
# The decision path reads where a transfer is going from the customers and
# accounts the graph holds, so the transfers wait until it holds all of them.
for _ in $(seq 1 120); do
  c="$(graph_count 'MATCH (c:Customer) WHERE c.onboardedAt IS NOT NULL RETURN count(c)')"
  a="$(graph_count 'MATCH (a:Account) WHERE a.accountType IS NOT NULL RETURN count(a)')"
  [ "${c}" = 74 ] && [ "${a}" = 120 ] && break
  sleep 1
done
echo "graph: ${c} customers onboarded, ${a} accounts opened"

say "Sending the first half of the transactions, one every 20 ms"
python3 -I tools/feed_transactions.py --to 150 --pace-ms 20
for _ in $(seq 1 60); do
  [ "$(graph_count 'MATCH (t:Transaction) RETURN count(t)')" -ge 150 ] 2>/dev/null && break
  sleep 1
done

say "Sending the rest, one every 50 ms, and killing Worker 1 while they flow"
python3 -I tools/feed_transactions.py --from 150 --pace-ms 50 &
feed_pid=$!
for _ in $(seq 1 120); do
  [ "$(graph_count 'MATCH (t:Transaction) RETURN count(t)')" -gt 165 ] 2>/dev/null && break
  sleep 0.25
done
echo "transactions in the graph at the kill: $(graph_count 'MATCH (t:Transaction) RETURN count(t)')"
kill -9 "${worker1_pid}"
wait "${worker1_pid}" 2>/dev/null || true
sleep 2
start_worker 1
worker1_pid="${last_pid}"
wait "${feed_pid}"

say "A producer resends a batch it had already delivered"
scripts/feed.sh retry

say "Checking every store against the demo's data"
rc=0
python3 -I tools/check_run.py --wait 180 || rc=1

say "Checking every decision, the decision log and how long each took"
python3 -I tools/check_decisions.py --wait 120 --steady 150 || rc=1
echo
echo "Neo4j Browser: http://localhost:${NEO4J_HTTP_PORT} (neo4j / ${NEO4J_PASSWORD})."
echo "Coordinator: http://localhost:${CLINK_HTTP_PORT}. Logs: ${STATE}."
exit "${rc}"
