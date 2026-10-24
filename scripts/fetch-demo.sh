#!/usr/bin/env bash
# Fetch Neo4j's fraud demo (github.com/neo4j-product-examples/demo-fraud,
# Apache-2.0) at a pinned commit: the CSV export of its graph, its dashboard
# and its licence. Every file is checked against scripts/neo4j-demo.sha256, so
# a moved branch or an edited file stops the run instead of changing the gate.
#
#   scripts/fetch-demo.sh            fetch into data/neo4j-demo (idempotent)
#   RECORD=1 scripts/fetch-demo.sh   (re)write the checksum list from what it fetched
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMMIT="174d3d787e2cb3abdbd0c3dfaea992deb05e64a9"
BASE="https://raw.githubusercontent.com/neo4j-product-examples/demo-fraud/${COMMIT}"
DEST="${ROOT}/data/neo4j-demo"
SUMS="${ROOT}/scripts/neo4j-demo.sha256"

FILES=(
  LICENSE
  src/neodash/fraud-dashboard.json
  src/cypher/Importer_Post_Processing.cypher
  data/export_transactions_Customer.csv
  data/export_transactions_Account_with_labels.csv
  data/export_transactions_Customer_HAS_ACCOUNT_Account.csv
  data/export_transactions_Transaction.csv
  data/export_transactions_Account_SENT_Transaction.csv
  data/export_transactions_Transaction_RECEIVED_Account.csv
  data/export_transactions_Email.csv
  data/export_transactions_PhoneNumber.csv
  data/export_transactions_Address.csv
  data/export_transactions_Customer_USES_EMAIL_Email.csv
  data/export_transactions_Customer_USES_PHONE_PhoneNumber.csv
  data/export_transactions_Customer_RESIDES_AT_Address.csv
  data/export_transactions_Jurisdiction.csv
  data/export_transactions_Country.csv
  data/export_transactions_Address_LOCATED_IN_Jurisdiction.csv
  data/export_transactions_Jurisdiction_ASSOCIATED_WITH_Country.csv
  data/export_transactions_Account_TRANSACTED_TO_Account.csv
  data/export_transactions_Customer_SHARED_IDENTIFIERS_Customer.csv
)

mkdir -p "${DEST}"
for f in "${FILES[@]}"; do
  out="${DEST}/${f}"
  if [ ! -s "${out}" ]; then
    mkdir -p "$(dirname "${out}")"
    curl --fail --silent --show-error --location --retry 3 -o "${out}.part" "${BASE}/${f}"
    mv "${out}.part" "${out}"
  fi
done

cd "${DEST}"
if [ "${RECORD:-0}" = 1 ]; then
  shasum -a 256 "${FILES[@]}" > "${SUMS}"
  echo "fetch-demo: recorded ${#FILES[@]} checksums in scripts/neo4j-demo.sha256"
else
  shasum -a 256 --check --quiet "${SUMS}"
  echo "fetch-demo: ${#FILES[@]} files at ${COMMIT:0:8}, checksums OK"
fi
