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

source_commit='7c2b75a8a99589b6ea4573e2fe507b502d7c1c74'
platform="${SDK_EMULATOR_PLATFORM:-linux/amd64}"
image="${SDK_EMULATOR_IMAGE:-maxcompute-emulator:sdk-${source_commit:0:12}}"
if [[ -z "${SDK_EMULATOR_IMAGE:-}" ]]; then
  docker build --platform "$platform" --target release -t "$image" \
    "https://github.com/dingxin-tech/maxcompute-emulator.git#$source_commit"
fi

container=
strict_container=
cleanup() {
  if [[ -n "$strict_container" ]]; then docker stop "$strict_container" >/dev/null || true; fi
  if [[ -n "$container" ]]; then docker stop "$container" >/dev/null || true; fi
}
trap cleanup EXIT

container="$(docker run --rm --detach --platform "$platform" --publish 127.0.0.1::8080 \
  "$image" --listen 0.0.0.0:8080 --test-mode --test-network --max-sessions 1024)"

cat > "$reports/strict-auth.json" <<'EOF'
{
  "fixture-go-sdk": {
    "secret": "local-go-sdk-fixture",
    "read": ["test_project.*"],
    "write": ["test_project.*"]
  }
}
EOF
chmod 644 "$reports/strict-auth.json"
strict_container="$(docker run --rm --detach --platform "$platform" --publish 127.0.0.1::8080 \
  --mount "type=bind,source=$reports/strict-auth.json,target=/fixture-auth.json,readonly" \
  "$image" --listen 0.0.0.0:8080 --test-mode --test-network \
  --auth-mode strict --auth-config /fixture-auth.json --quotas go-batch)"

export MAXCOMPUTE_EMULATOR_ENDPOINT="http://$(docker port "$container" 8080/tcp)"
export MAXCOMPUTE_EMULATOR_AUTH_ENDPOINT="http://$(docker port "$strict_container" 8080/tcp)"
for endpoint in "$MAXCOMPUTE_EMULATOR_ENDPOINT" "$MAXCOMPUTE_EMULATOR_AUTH_ENDPOINT"; do
  curl --fail --silent --show-error --max-time 3 --retry 10 --retry-delay 1 --retry-all-errors "$endpoint/readyz"
  printf '\n'
done

export GOTOOLCHAIN=local GOWORK=off
unset MAXCOMPUTE_LIVE_TESTS ALIBABA_CLOUD_ACCESS_KEY_ID ALIBABA_CLOUD_ACCESS_KEY_SECRET ALIBABA_CLOUD_SECURITY_TOKEN
unset odps_endpoint tunnel_endpoint tunnel_odps_endpoint

go build -mod=readonly ./...
(cd examples && go build -mod=readonly ./...)

branch_tool="${SDK_GOBCO:-$reports/tools/gobco}"
if [[ ! -x "$branch_tool" ]]; then
  GOBIN="$reports/tools" go install github.com/rillig/gobco@v1.3.4
  branch_tool="$reports/tools/gobco"
fi

run_coverage() {
  local output="$1" module packages package path log stats
  mkdir -p "$output"
  go test -mod=readonly -count=1 -timeout=90s -race -covermode=atomic \
    -coverpkg=./... -coverprofile="$output/go.cover" ./...
  module="$(go list -m)"
  packages="$(go list ./...)"
  while IFS= read -r package; do
    path="${package#"$module/"}"
    log="$output/${path//\//-}.branches.log"
    stats="$output/${path//\//-}.branches.json"
    if ! "$branch_tool" -branch -stats "$stats" -test=-count=1 -test=-timeout=90s "./$path" > "$log" 2>&1; then
      cat "$log" >&2
      echo "Branch coverage failed for $package" >&2
      return 1
    fi
  done <<< "$packages"
}

run_coverage "$reports"
(cd tea && run_coverage "$reports/tea")
python3 scripts/sdk_coverage.py "$reports" "$@"
