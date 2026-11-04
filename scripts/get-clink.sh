#!/usr/bin/env bash
# Install the clink this project runs against, from clink's prebuilt SDK.
#
# run.sh runs clink's own binaries, clink_node and clink, from CLINK_BIN_DIR.
# This installs them under .clink/prefix from the SDK tarball a clink release
# publishes for linux x86_64 (glibc floor Ubuntu 24.04), so that
#
#   scripts/get-clink.sh && CLINK_BIN_DIR=.clink/prefix/bin ./run.sh
#
# runs the showcase against the pinned release. CI does exactly that. On any
# other platform, build clink and point CLINK_BIN_DIR at its build directory.
#
#   CLINK_VERSION  release tag whose SDK to install (default: the version this
#                  repository ships against)
#   CLINK_SDK      path to an SDK tarball to install instead: a release's
#                  asset, or the one a clink release-binaries dry run uploads.
#                  CI uses it to run this project against a clink release
#                  candidate before it is tagged.
#   CLINK_REPO     clink's repository (default: https://github.com/orhaugh/clink)
#
# Idempotent: re-running against an existing matching install is a no-op.
set -euo pipefail

CLINK_VERSION="${CLINK_VERSION:-v0.15.0}"
CLINK_REPO="${CLINK_REPO:-https://github.com/orhaugh/clink}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX="${ROOT}/.clink/prefix"
STAMP="${PREFIX}/.transfer-sentry-installed"

install_sdk() {
    # $1 = SDK tarball, $2 = stamp contents. Verifies the tarball against the
    # .sha256 beside it when there is one, then unpacks it as the prefix.
    local tarball="$1" stamp="$2"
    local dir name tmp
    dir="$(cd "$(dirname "${tarball}")" && pwd)"
    name="$(basename "${tarball}")"
    if [[ -f "${dir}/${name}.sha256" ]]; then
        ( cd "${dir}" && sha256sum -c "${name}.sha256" >/dev/null )
    fi
    tmp="$(mktemp -d)"
    tar xzf "${tarball}" -C "${tmp}"
    mkdir -p "${ROOT}/.clink"
    rm -rf "${PREFIX}"
    mv "${tmp}/${name%.tar.gz}" "${PREFIX}"
    rm -rf "${tmp}"
    echo "${stamp}" > "${STAMP}"
    echo "== clink ${stamp} installed from the prebuilt SDK."
    echo "   run with: CLINK_BIN_DIR=${PREFIX}/bin ./run.sh"
}

if [[ -n "${CLINK_SDK:-}" ]]; then
    want="sdk:$(basename "${CLINK_SDK}" .tar.gz)"
else
    want="${CLINK_VERSION}"
fi
if [[ -x "${PREFIX}/bin/clink_node" && -f "${STAMP}" ]] && grep -qx "${want}" "${STAMP}"; then
    echo "clink ${want} already installed at ${PREFIX}"
    exit 0
fi

if [[ -n "${CLINK_SDK:-}" ]]; then
    echo "== transfer-sentry: installing clink from ${CLINK_SDK}"
    install_sdk "${CLINK_SDK}" "${want}"
    exit 0
fi

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
    echo "get-clink.sh: clink's SDK is published for linux x86_64 only;" \
         "build clink and run with CLINK_BIN_DIR=<its build directory>" >&2
    exit 2
fi
asset="clink-${CLINK_VERSION}-linux-x86_64-ubuntu24.04.tar.gz"
base="${CLINK_REPO%.git}/releases/download/${CLINK_VERSION}"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
echo "== transfer-sentry: installing clink ${CLINK_VERSION} from ${base}/${asset}"
curl -fsSL -o "${tmp}/${asset}" "${base}/${asset}"
curl -fsSL -o "${tmp}/${asset}.sha256" "${base}/${asset}.sha256"
install_sdk "${tmp}/${asset}" "${CLINK_VERSION}"
