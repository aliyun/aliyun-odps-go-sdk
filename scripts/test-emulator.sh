#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--release" ) ]]; then
  echo "usage: bash scripts/test-emulator.sh [--release]" >&2
  exit 2
fi

report_root="${SDK_TEST_REPORT_DIR:-${TMPDIR:-/tmp}}"
mkdir -p "$report_root"
reports="$(mktemp -d "$report_root/odps-sdk-coverage.XXXXXX")"
reports="$(cd "$reports" && pwd)"
echo "Test reports: $reports"

image='maxcompute/maxcompute-emulator:1.2.0@sha256:c4f708449e309cdb6900641b7d90097e6c9b2633a943dac382c27683fc3daf5f'
container="$(docker run --rm --detach --platform linux/amd64 \
  --publish 127.0.0.1::8080 "$image" \
  --listen 0.0.0.0:8080 --test-mode --test-network --max-sessions 1024)"
trap 'docker stop "$container" >/dev/null || true' EXIT
endpoint="http://$(docker port "$container" 8080/tcp)"
curl --fail --silent --show-error --max-time 3 --retry 10 --retry-delay 1 --retry-all-errors "$endpoint/readyz"
printf '\n'

export MAXCOMPUTE_EMULATOR_ENDPOINT="$endpoint"
export GOTOOLCHAIN=local GOWORK=off
unset MAXCOMPUTE_LIVE_TESTS ALIBABA_CLOUD_ACCESS_KEY_ID ALIBABA_CLOUD_ACCESS_KEY_SECRET ALIBABA_CLOUD_SECURITY_TOKEN
unset odps_endpoint tunnel_endpoint tunnel_odps_endpoint

go build -mod=readonly ./...
go test -mod=readonly -count=1 -timeout=90s -race -covermode=atomic \
  -coverpkg=./... -coverprofile="$reports/go.cover" ./...
(cd examples && go build -mod=readonly ./...)

branch_tool="${SDK_GOBCO:-$reports/tools/gobco}"
if [[ ! -x "$branch_tool" ]]; then
  GOBIN="$reports/tools" go install github.com/rillig/gobco@v1.3.4
  branch_tool="$reports/tools/gobco"
fi

module="$(go list -m)"
packages="$(go list ./...)"
while IFS= read -r package; do
  path="${package#"$module/"}"
  log="$reports/${path//\//-}.branches.log"
  stats="$reports/${path//\//-}.branches.json"
  if ! "$branch_tool" -branch -stats "$stats" -test=-count=1 -test=-timeout=90s "./$path" > "$log" 2>&1; then
    cat "$log" >&2
    echo "Branch coverage failed for $path" >&2
    exit 1
  fi
done <<< "$packages"

python3 scripts/sdk_coverage.py "$reports" "$@"
