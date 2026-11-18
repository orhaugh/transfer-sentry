#!/usr/bin/env bash
# Build the clink this project runs against, from source, with Kafka.
#
# run.sh runs clink's own binaries, clink_node and clink, from CLINK_BIN_DIR.
# clink's prebuilt SDK leaves the broker connectors out (each would pin a
# client library the host must also have), and transfer-sentry reads and
# writes Kafka, so this builds the two binaries from clink's source, with the
# Kafka and PostgreSQL connectors, and puts them in .clink/bin:
#
#   scripts/get-clink.sh && CLINK_BIN_DIR=.clink/bin ./run.sh
#
# CI runs exactly that.
#
#   CLINK_VERSION  release tag to build (default: the version this repository
#                  ships against)
#   CLINK_COMMIT   a clink commit to build instead; CI sets it to run this
#                  project against a clink release candidate before it is
#                  tagged
#   CLINK_REPO     clink's repository (default: https://github.com/orhaugh/clink)
#   CLINK_JOBS     build parallelism (default: the number of CPUs, capped at 10)
#
# Needs CMake, a C++23 compiler (GCC 14 or newer, or Xcode's clang),
# librdkafka with its C++ API (librdkafka-dev on Ubuntu, librdkafka on
# Homebrew) and libpq. clink's pinned Arrow toolchain is bootstrapped into
# ~/.clink-deps by clink's own scripts/build-arrow.sh, which reads
# CLINK_ARROW_OBJECT_STORES (CI turns the object stores off: nothing here uses
# them). Iceberg is left out for the same reason. The binaries find Arrow
# there, so .clink/bin and ~/.clink-deps are all a later run needs.
# Idempotent: re-running against a matching build is a no-op.
set -euo pipefail

CLINK_VERSION="${CLINK_VERSION:-v0.16.2}"
CLINK_REPO="${CLINK_REPO:-https://github.com/orhaugh/clink}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${ROOT}/.clink/src"
BUILD="${ROOT}/.clink/build"
BIN="${ROOT}/.clink/bin"
STAMP="${BIN}/.transfer-sentry-built"

ncpu="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
JOBS="${CLINK_JOBS:-$(( ncpu < 10 ? ncpu : 10 ))}"

want="${CLINK_COMMIT:-${CLINK_VERSION}}"
if [[ -x "${BIN}/clink_node" && -x "${BIN}/clink" && -f "${STAMP}" ]] \
   && grep -qx "${want}" "${STAMP}"; then
    echo "clink ${want} already built in ${BIN}"
    exit 0
fi

# The source, shallow, at the tag or the commit: a different one starts over.
if [[ "$(cat "${ROOT}/.clink/src.ref" 2>/dev/null || true)" != "${want}" ]]; then
    echo "== transfer-sentry: fetching clink ${want}"
    rm -rf "${SRC}" "${BUILD}"
    mkdir -p "${ROOT}/.clink"
    git init -q "${SRC}"
    git -C "${SRC}" fetch -q --depth 1 "${CLINK_REPO}" "${want}"
    git -C "${SRC}" checkout -q FETCH_HEAD
    echo "${want}" > "${ROOT}/.clink/src.ref"
fi

echo "== transfer-sentry: building clink ${want} (clink_node and clink) in ${BUILD}"
(cd "${SRC}" && scripts/build-arrow.sh)
cmake -S "${SRC}" -B "${BUILD}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLINK_BUILD_SQL=ON \
    -DCLINK_BUILD_TESTS=OFF \
    -DCLINK_BUILD_EXAMPLES=OFF \
    -DCLINK_WITH_ICEBERG=OFF \
    -DCLINK_WITH_KAFKA=ON \
    -DCLINK_WITH_POSTGRES=ON
cmake --build "${BUILD}" --target clink_node clink --parallel "${JOBS}"
rm -rf "${BIN}"
mkdir -p "${BIN}"
cp "${BUILD}/clink_node" "${BUILD}/clink" "${BIN}/"
echo "${want}" > "${STAMP}"
echo "== clink ${want} built."
echo "   run with: CLINK_BIN_DIR=${BIN} ./run.sh"
