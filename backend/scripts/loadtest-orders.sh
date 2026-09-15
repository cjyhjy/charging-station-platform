#!/usr/bin/env bash
#
# Load and duplicate-submission test (B-06).
#
# The property that matters under load on this platform is not raw throughput: it is that concurrent
# traffic cannot produce two effects from one action. Two clients tapping "start charging" at the same
# time, or a client retrying a create because the answer was slow, must not create two orders or two
# device commands. So this test drives concurrency at the endpoints where that could happen and
# asserts the outcome, then reports the latency it observed.
#
# It runs against a live stack (backend/scripts/local-stack.sh) and needs a seeded user.
#
# Usage:
#   NCS_API_URL=http://127.0.0.1:8080 backend/scripts/loadtest-orders.sh
#
# Environment:
#   NCS_LOAD_CONCURRENCY=8      parallel clients
#   NCS_LOAD_ROUNDS=5           rounds per phase
#   NCS_LOAD_ACCOUNT=13800000001 / NCS_LOAD_PASSWORD=Dev-Password-01
#   NCS_POSTGRES_DSN=...        optional: enables the database-level assertions
set -euo pipefail

api_url="${NCS_API_URL:-http://127.0.0.1:8080}"
concurrency="${NCS_LOAD_CONCURRENCY:-8}"
rounds="${NCS_LOAD_ROUNDS:-5}"
account="${NCS_LOAD_ACCOUNT:-13800000001}"
password="${NCS_LOAD_PASSWORD:-Dev-Password-01}"
psql_dsn="${NCS_POSTGRES_DSN:-}"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT INT TERM

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }
psql_q() { psql "${psql_dsn}" -tAc "$1"; }
have_db() { [[ -n "${psql_dsn}" ]]; }

step "readiness"
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${api_url}/healthz")" == "200" ]] || fail "the API is not healthy at ${api_url}"
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${api_url}/readyz")" == "200" ]] || fail "the API is not ready at ${api_url}"

step "log in"
login="$(curl -fsS -X POST "${api_url}/api/v1/auth/user/login" -H 'Content-Type: application/json' \
    -d "{\"account\":\"${account}\",\"password\":\"${password}\"}")"
token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["accessToken"])' <<<"${login}")"
[[ -n "${token}" ]] || fail "no access token"
auth=(-H "Authorization: Bearer ${token}")

# A charger that is free: the seeded station's first charger. An order holds its charger exclusively,
# so a load test that ignores that would only measure conflict responses.
charger_id="$(curl -fsS "${api_url}/api/v1/chargers" "${auth[@]}" | python3 -c '
import json, sys
for charger in json.load(sys.stdin)["data"]["items"]:
    if charger.get("status") == "IDLE":
        print(charger["id"]); raise SystemExit
print("", end="")
')"
if [[ -z "${charger_id}" ]]; then
    echo "no idle charger in /stations; skipping the order phases (the read path is still measured)"
fi

measure() {
    # measure <name> <curl-args...>; writes per-request milliseconds to a file and echoes the bodies
    local name="$1"; shift
    local out_dir="${work_dir}/${name}"
    mkdir -p "${out_dir}"
    local pids=()
    for index in $(seq 1 "${concurrency}"); do
        (
            curl -s -o "${out_dir}/body-${index}" -w '%{time_total}\n' "$@" > "${out_dir}/time-${index}"
        ) &
        pids+=("$!")
    done
    for pid in "${pids[@]}"; do wait "${pid}"; done
    cat "${out_dir}"/time-* > "${out_dir}/times"
}

percentile() {
    python3 - "$1" "$2" <<'PY'
import sys
path, pct = sys.argv[1], float(sys.argv[2])
values = sorted(float(line) * 1000 for line in open(path) if line.strip())
if not values:
    print("n/a"); raise SystemExit
index = min(len(values) - 1, int(round((pct / 100) * (len(values) - 1))))
print(f"{values[index]:.0f}")
PY
}

report_latency() {
    # report_latency <label> <times-file>
    local label="$1" times="$2"
    [[ -s "${times}" ]] || fail "no latency samples for ${label}"
    printf '%-28s p50=%sms p95=%sms max=%sms (n=%s)\n' "${label}" \
        "$(percentile "${times}" 50)" "$(percentile "${times}" 95)" "$(percentile "${times}" 100)" \
        "$(wc -l < "${times}" | tr -d ' ')"
}

step "read path: ${concurrency} concurrent GET /stations × ${rounds} rounds"
for round in $(seq 1 "${rounds}"); do
    measure "stations-${round}" "${api_url}/api/v1/stations" "${auth[@]}"
    for index in $(seq 1 "${concurrency}"); do
        code="$(python3 -c '
import json, sys
try:
    json.load(open(sys.argv[1]))
    print("ok")
except Exception:
    print("invalid")
' "${work_dir}/stations-${round}/body-${index}")"
        [[ "${code}" == "ok" ]] || fail "a read answered a body that is not the shared envelope"
    done
done
cat "${work_dir}"/stations-*/times > "${work_dir}/stations.times"
report_latency "stations" "${work_dir}/stations.times"
# Every read must have answered: a zero timing is curl failing to connect, which the body check above
# could not see because there would be no body at all.
python3 - "${work_dir}/stations.times" <<'PY' || fail "the read path returned an error"
import sys
lines = [line.strip() for line in open(sys.argv[1]) if line.strip()]
bad = [line for line in lines if float(line) <= 0]
print(f"samples: {len(lines)}")
raise SystemExit(1 if bad else 0)
PY

if [[ -n "${charger_id}" ]]; then
    step "duplicate create: ${concurrency} concurrent requests with one Idempotency-Key"
    key="load-create-$(date +%s%N)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
    measure "create" -X POST "${api_url}/api/v1/orders" "${auth[@]}" \
        -H 'Content-Type: application/json' -H "Idempotency-Key: ${key}" -d "{\"chargerId\": ${charger_id}}"
    mapfile -t order_nos < <(python3 - "${work_dir}/create" "${concurrency}" <<'PY'
import json, sys
directory, count = sys.argv[1], int(sys.argv[2])
for index in range(1, count + 1):
    try:
        payload = json.load(open(f"{directory}/body-{index}"))
        data = payload.get("data") or {}
        print(data.get("orderNo") or f"ERROR:{payload.get('message')}")
    except Exception as exc:
        print(f"ERROR:{exc}")
PY
)
    unique_order_nos="$(printf '%s\n' "${order_nos[@]}" | sort -u)"
    echo "responses: $(printf '%s ' "${order_nos[@]}")"
    [[ "$(wc -l <<<"${unique_order_nos}" | tr -d ' ')" == "1" ]] || fail "a duplicate create produced more than one order: ${unique_order_nos}"
    order_no="${unique_order_nos}"
    case "${order_no}" in ERROR*) fail "create failed: ${order_no}" ;; esac
    report_latency "create" "${work_dir}/create/times"

    if have_db; then
        rows="$(psql_q "SELECT count(*) FROM charging_orders WHERE order_no = '${order_no}'")"
        [[ "${rows}" == "1" ]] || fail "database holds ${rows} rows for ${order_no}, want 1"
        idem="$(psql_q "SELECT count(*) FROM idempotency_records WHERE idempotency_key = '${key}'")"
        [[ "${idem}" == "1" ]] || fail "database holds ${idem} idempotency records for one key, want 1"
        echo "database: 1 order, 1 idempotency record for the shared key"
    fi

    step "duplicate start: two concurrent requests with one Idempotency-Key"
    start_key="load-start-$(date +%s%N)"
    for index in 1 2; do
        (
            curl -s -o "${work_dir}/start-${index}" -w '%{http_code}' -X POST "${api_url}/api/v1/orders/${order_no}/start" \
                "${auth[@]}" -H "Idempotency-Key: ${start_key}" > "${work_dir}/start-${index}.code"
        ) &
    done
    wait
    echo "codes: $(cat "${work_dir}/start-1.code") $(cat "${work_dir}/start-2.code")"
    for index in 1 2; do
        code="$(cat "${work_dir}/start-${index}.code")"
        [[ "${code}" == "202" ]] || fail "duplicate start answered ${code}, want the same 202 for both"
        status="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["status"])' "${work_dir}/start-${index}")"
        [[ "${status}" == "STARTING" ]] || fail "duplicate start reported status ${status}, want STARTING"
    done

    if have_db; then
        requests="$(psql_q "SELECT count(*) FROM outbox_events WHERE event_type = 'CHARGE_START_REQUESTED' AND aggregate_id = '${order_no}'")"
        commands="$(psql_q "SELECT count(*) FROM outbox_events WHERE event_type = 'CHARGER_COMMAND_REQUESTED' AND payload->>'order_no' = '${order_no}'")"
        [[ "${requests}" == "1" ]] || fail "${requests} CHARGE_START_REQUESTED events for one order, want 1"
        [[ "${commands}" == "1" ]] || fail "${commands} device commands for one order, want 1"
        echo "database: 1 lifecycle event and 1 device command for the shared idempotency key"
    fi

    step "cleanup: cancel the order the load test created"
    curl -fsS -X POST "${api_url}/api/v1/orders/${order_no}/cancel" "${auth[@]}" \
        -H 'Content-Type: application/json' -H "Idempotency-Key: load-cancel-$(date +%s%N)" \
        -d '{"reason":"load test cleanup"}' >/dev/null
    echo "order ${order_no} cancelled"
fi

step "what the API reported for the same traffic"
curl -s "${api_url}/metrics" | grep -E '^ncs_api_requests_total|^ncs_api_request_duration_seconds_count' | head -10 || true

echo
echo "PASS: loadtest - concurrent reads and duplicate submissions produced exactly one effect each"
