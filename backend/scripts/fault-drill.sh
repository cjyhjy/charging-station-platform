#!/usr/bin/env bash
#
# Dependency fault drill (B-06).
#
# The readiness behaviour added in this module is only worth having if it is true under a real
# outage, so this drill takes the dependencies away from a running API and watches what happens:
#
#   * /readyz must flip to 503 while PostgreSQL or Redis is unreachable, and back to 200 afterwards;
#   * /healthz must stay 200, because the process itself is alive - the two endpoints answer
#     different questions and a deployment that conflates them restarts a healthy process;
#   * ncs_dependency_up must follow reality, so an alert on it means something;
#   * the API must not exit.
#
# The outage is injected with a TCP cut rather than by stopping the services: this verification host
# has no root and a shared PostgreSQL, so stopping them would be both impossible and rude. A cut
# looks exactly like a network partition from the client's side (connections are torn down and new
# ones are refused), which is the failure mode the readiness policy exists for. On a real deployment
# the same assertions run against `systemctl stop postgresql`.
#
# Usage:
#   NCS_TEST_PG_DSN=postgres://... backend/scripts/fault-drill.sh
#   NCS_REDIS_ADDR=127.0.0.1:6379  (default), NCS_FAULT_TIMEOUT=25 (seconds to wait for a flip)
set -euo pipefail

: "${NCS_TEST_PG_DSN:?set NCS_TEST_PG_DSN to a disposable PostgreSQL}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
backend_dir="$(cd "${script_dir}/.." && pwd)"
redis_addr="${NCS_REDIS_ADDR:-127.0.0.1:6379}"
redis_db="${NCS_REDIS_DB:-14}"
flip_timeout="${NCS_FAULT_TIMEOUT:-25}"
work_dir="$(mktemp -d)"
api_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
pg_proxy_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
redis_proxy_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
api_url="http://127.0.0.1:${api_port}"

pids=()
cleanup() {
    local status=$?
    for pid in "${pids[@]:-}"; do
        [[ -n "${pid}" ]] || continue
        kill "${pid}" 2>/dev/null || true
        wait "${pid}" 2>/dev/null || true
    done
    if [[ ${status} -ne 0 ]]; then
        echo "--- api log (${work_dir}) ---" >&2
        tail -25 "${work_dir}/api.log" >&2 || true
    fi
    rm -rf "${work_dir}"
    exit "${status}"
}
trap cleanup EXIT INT TERM

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }

pg_host="${NCS_TEST_PG_DSN#*@}"
pg_host="${pg_host%%/*}"
pg_host="${pg_host%%\?*}"
pg_host_port="${pg_host##*:}"
pg_host_name="${pg_host%%:*}"
base_dsn="$(python3 - "$NCS_TEST_PG_DSN" "$pg_proxy_port" <<'DSN'
import sys, urllib.parse
parsed = urllib.parse.urlparse(sys.argv[1])
port = sys.argv[2]
host = parsed.hostname or '127.0.0.1'
print(parsed._replace(netloc=f"{parsed.username or ''}{':' + parsed.password if parsed.password else ''}@{host}:{port}").geturl())
DSN
)"

# A byte-level TCP proxy: it keeps no state, so killing it is indistinguishable from a partition.
cat > "${work_dir}/tcpcut.py" <<'PROXY'
import socket, sys, threading

listen_port, target_host, target_port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])

def pump(source, sink):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            sink.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (source, sink):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

def handle(client):
    try:
        upstream = socket.create_connection((target_host, target_port), timeout=10)
    except OSError:
        client.close()
        return
    threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pump, args=(upstream, client), daemon=True).start()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("127.0.0.1", listen_port))
server.listen(64)
while True:
    client, _ = server.accept()
    threading.Thread(target=handle, args=(client,), daemon=True).start()
PROXY

start_proxy() {
    local port="$1" host="$2" target_port="$3"
    python3 "${work_dir}/tcpcut.py" "${port}" "${host}" "${target_port}" >"${work_dir}/proxy-${port}.log" 2>&1 &
    echo $!
}

wait_for_status() {
    local path="$1" want="$2" timeout="$3"
    local deadline=$(( $(date +%s) + timeout ))
    while [[ $(date +%s) -lt ${deadline} ]]; do
        local got
        got="$(curl -s -o /dev/null -w '%{http_code}' "${api_url}${path}" || echo 000)"
        [[ "${got}" == "${want}" ]] && return 0
        sleep 0.5
    done
    return 1
}

readyz_status() { curl -s -o /dev/null -w '%{http_code}' "${api_url}/readyz" || echo 000; }
healthz_status() { curl -s -o /dev/null -w '%{http_code}' "${api_url}/healthz" || echo 000; }
dependency_gauge() { curl -s "${api_url}/metrics" | sed -n "s/^ncs_dependency_up{dependency=\"$1\"} //p" | head -1; }

step "build and start through the cut"
(cd "${backend_dir}" && go build -o "${work_dir}/ncs-api" ./cmd/api)
pg_proxy_pid="$(start_proxy "${pg_proxy_port}" "${pg_host_name}" "${pg_host_port}")"
pids+=("${pg_proxy_pid}")
redis_proxy_pid="$(start_proxy "${redis_proxy_port}" "${redis_addr%%:*}" "${redis_addr##*:}")"
pids+=("${redis_proxy_pid}")
sleep 0.5

env NCS_POSTGRES_DSN="${base_dsn}" \
    NCS_REDIS_ADDR="127.0.0.1:${redis_proxy_port}" NCS_REDIS_DB="${redis_db}" \
    NCS_HTTP_ADDR="127.0.0.1:${api_port}" NCS_CHARGER_GATEWAY_TOKEN=fault-drill-token \
    "${work_dir}/ncs-api" >"${work_dir}/api.log" 2>&1 &
api_pid=$!
pids+=("${api_pid}")
wait_for_status /healthz 200 20 || fail "the API did not become healthy"
wait_for_status /readyz 200 20 || fail "the API did not become ready with both dependencies up"
echo "baseline: healthz=$(healthz_status) readyz=$(readyz_status) postgres=$(dependency_gauge postgres) redis=$(dependency_gauge redis)"

step "cut Redis"
kill "${redis_proxy_pid}" 2>/dev/null || true
wait "${redis_proxy_pid}" 2>/dev/null || true
wait_for_status /readyz 503 "${flip_timeout}" || fail "readyz did not report 503 within ${flip_timeout}s of the Redis outage"
[[ "$(healthz_status)" == "200" ]] || fail "healthz must stay 200 while the process is alive"
[[ "$(dependency_gauge redis)" == "0" ]] || fail "ncs_dependency_up{redis} did not follow the outage"
echo "redis down:  healthz=$(healthz_status) readyz=$(readyz_status) redis=$(dependency_gauge redis) postgres=$(dependency_gauge postgres)"
grep -q "redis probe failed" "${work_dir}/api.log" || fail "the outage was not logged"

step "restore Redis"
redis_proxy_pid="$(start_proxy "${redis_proxy_port}" "${redis_addr%%:*}" "${redis_addr##*:}")"
pids+=("${redis_proxy_pid}")
wait_for_status /readyz 200 "${flip_timeout}" || fail "readyz did not recover within ${flip_timeout}s"
[[ "$(dependency_gauge redis)" == "1" ]] || fail "ncs_dependency_up{redis} did not recover"
echo "redis back:  readyz=$(readyz_status) redis=$(dependency_gauge redis)"

step "cut PostgreSQL"
kill "${pg_proxy_pid}" 2>/dev/null || true
wait "${pg_proxy_pid}" 2>/dev/null || true
wait_for_status /readyz 503 "${flip_timeout}" || fail "readyz did not report 503 within ${flip_timeout}s of the PostgreSQL outage"
[[ "$(healthz_status)" == "200" ]] || fail "healthz must stay 200 while the process is alive"
[[ "$(dependency_gauge postgres)" == "0" ]] || fail "ncs_dependency_up{postgres} did not follow the outage"
echo "postgres down: healthz=$(healthz_status) readyz=$(readyz_status) postgres=$(dependency_gauge postgres) redis=$(dependency_gauge redis)"
grep -q "postgres probe failed" "${work_dir}/api.log" || fail "the outage was not logged"

step "restore PostgreSQL"
pg_proxy_pid="$(start_proxy "${pg_proxy_port}" "${pg_host_name}" "${pg_host_port}")"
pids+=("${pg_proxy_pid}")
wait_for_status /readyz 200 "${flip_timeout}" || fail "readyz did not recover within ${flip_timeout}s"
[[ "$(dependency_gauge postgres)" == "1" ]] || fail "ncs_dependency_up{postgres} did not recover"

step "the API survived both outages as one process"
kill -0 "${api_pid}" 2>/dev/null || fail "the API exited during the drill"
echo "api pid ${api_pid} still running"
# The migration and backlog gauges are only published while PostgreSQL answers, so a stale number can
# never be mistaken for a current one after an outage: the probe republishes them on recovery.
grep -q "ncs_pg_migrations_version\|migrations_version" <(curl -s "${api_url}/metrics") || fail "the schema gauge was not republished after recovery"
kill -0 "${api_pid}" 2>/dev/null || fail "the API was not running when the drill finished"

cat <<'REPORT'

PASS: dependency drill (real API, injected outages)

  * /readyz reports 503 while PostgreSQL or Redis is unreachable and returns to 200 after recovery;
  * /healthz stays 200 throughout: the process is alive while it refuses to serve;
  * ncs_dependency_up follows the outages in both directions, and each outage is logged;
  * the API is one process that survived both outages rather than restarting around them.

See also backend/scripts/verify-closed-loop.sh for the worker-crash, pending-recovery, duplicate
delivery and single-active-publisher assertions.
REPORT
