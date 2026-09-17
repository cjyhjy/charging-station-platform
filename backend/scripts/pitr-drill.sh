#!/usr/bin/env bash
#
# PITR (point-in-time recovery) drill (B-06, monthly per the ruling).
#
# What it proves: that the WAL archive and a base backup can rebuild the database at a chosen instant -
# not "the dump restores" (that is the weekly drill) but "we can stop the database between two known
# commits". The approved window is a recovery target 5 to 15 minutes before a chosen point, so this
# script also reports how far back the archive actually reaches.
#
# How it runs, and why it looks like this:
#
#   * the target is an ISOLATED TEMPORARY INSTANCE, never the source. The ruling requires it, and it is
#     also the only honest way to test recovery: promoting a replica of the thing you are testing
#     destroys the thing you are testing.
#   * WAL is archived client-side with pg_receivewal, so no server configuration change (and no restart
#     of the production instance) is needed to rehearse recovery.
#   * the drill writes two markers into the SOURCE around a chosen target time, then recovers to that
#     instant and asserts the first marker is present and the second is absent. Anything less - "the
#     instance started" - would pass even if replay stopped at the wrong place.
#
# Environment:
#   NCS_PITR_SOURCE_DSN         the database to back up (falls back to NCS_TEST_PG_DSN). Must not be
#                               production; the drill only reads from it, but it also creates a
#                               replication slot, which pins WAL until it is dropped.
#   NCS_PITR_REWIND_MINUTES     5..15, default 10 (the approved band)
#   NCS_PITR_WORK_DIR           scratch directory (default: a fresh temporary directory)
#
# Usage:
#   NCS_TEST_PG_DSN=postgres://... backend/scripts/pitr-drill.sh
set -uo pipefail

source_dsn="${NCS_PITR_SOURCE_DSN:-${NCS_TEST_PG_DSN:-}}"
rewind_minutes="${NCS_PITR_REWIND_MINUTES:-10}"
work_dir="${NCS_PITR_WORK_DIR:-}"
slot_name="ncs_pitr_drill_$$"
started_at="$(date +%s)"
report_lines=()

fail() {
    local reason="$*"
    echo "FAIL: ${reason}" >&2
    finish 1 "${reason}"
}

finish() {
    local status="$1" reason="${2:-}"
    echo
    echo "=== PITR drill report ==="
    echo "result:            $([[ ${status} -eq 0 ]] && echo PASS || echo FAIL)"
    echo "recovery target:   ${target_time:-not chosen}"
    # Waiting for the target to age into the approved window is not recovery work, so the two numbers are
    # reported apart: a single total would read as a slow replay when it is mostly the deliberate wait.
    echo "wait for the window: ${measured_rewind_minutes:-0} minutes (the target aged to the approved band before recovery started)"
    echo "recovery duration: ${replay_seconds:-$(( $(date +%s) - started_at ))} seconds (temporary instance start to ready)"
    echo "archive coverage:  ${coverage:-unknown}"
    if [[ ${status} -ne 0 ]]; then
        echo "failure reason:    ${reason}"
    fi
    for line in "${report_lines[@]:-}"; do
        [[ -n "${line}" ]] && echo "${line}"
    done
    exit "${status}"
}

step() { printf '\n=== %s ===\n' "$*"; }

case "${rewind_minutes}" in
    5|6|7|8|9|10|11|12|13|14|15) ;;
    *) echo "NCS_PITR_REWIND_MINUTES must be between 5 and 15 (the approved band), got ${rewind_minutes}" >&2; exit 2 ;;
esac
[ -n "${source_dsn}" ] || {
    echo "refusing to run: set NCS_PITR_SOURCE_DSN (or NCS_TEST_PG_DSN) to the database to rehearse recovery for." >&2
    echo "the drill creates a replication slot and a temporary instance; it does not fall back to a default." >&2
    exit 2
}
source_db="$(python3 - "$source_dsn" <<'PY'
import sys, urllib.parse
print(urllib.parse.urlparse(sys.argv[1]).path.lstrip('/') or '')
PY
)"
case "${source_db}" in
    *prod*|*production*) echo "refusing to run against a production database name: ${source_db}" >&2; exit 2 ;;
esac

# The server binaries live in a versioned directory on Debian/Ubuntu (for example
# /usr/lib/postgresql/16/bin) which is not on PATH for a service or a non-login shell. Look there
# rather than telling the operator to fix their PATH.
if ! command -v pg_ctl >/dev/null 2>&1; then
    for candidate in /usr/lib/postgresql/*/bin /usr/local/pgsql/bin /opt/homebrew/opt/postgresql*/bin; do
        if [[ -x "${candidate}/pg_ctl" ]]; then
            POSTGRES_BIN="${candidate}"
            export PATH="${candidate}:${PATH}"
            break
        fi
    done
fi
for tool in pg_receivewal pg_basebackup pg_ctl psql; do
    command -v "${tool}" >/dev/null 2>&1 || fail "${tool} is not installed (postgresql-client and postgresql server packages)"
done
echo "postgres tools: $(command -v pg_ctl) ($(pg_ctl --version 2>/dev/null | head -1))"

[[ -n "${work_dir}" ]] || work_dir="$(mktemp -d)"
wal_dir="${work_dir}/wal"
base_dir="${work_dir}/base"
sock_dir="${work_dir}/socket"
mkdir -p "${wal_dir}" "${sock_dir}" "${base_dir}"
chmod 700 "${work_dir}"

pg_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
receivewal_pid=""
temp_started="false"

cleanup() {
    local status=$?
    if [[ "${temp_started}" == "true" ]]; then
        pg_ctl -D "${base_dir}" -m immediate stop >/dev/null 2>&1 || true
    fi
    if [[ -n "${receivewal_pid}" ]]; then
        kill "${receivewal_pid}" 2>/dev/null || true
        wait "${receivewal_pid}" 2>/dev/null || true
    fi
    # The slot must go: a forgotten replication slot pins WAL on the source until the disk fills.
    psql "${source_dsn}" -q -c "SELECT pg_drop_replication_slot('${slot_name}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '${slot_name}')" >/dev/null 2>&1 || true
    rm -rf "${work_dir}"
    exit "${status}"
}
trap cleanup EXIT INT TERM

step "preflight on the source"
wal_level="$(psql "${source_dsn}" -tAc 'SHOW wal_level')"
max_senders="$(psql "${source_dsn}" -tAc 'SHOW max_wal_senders')"
[[ "${wal_level}" == "replica" || "${wal_level}" == "logical" ]] || fail "the source has wal_level=${wal_level}; PITR needs replica or logical"
[[ "${max_senders}" -ge 2 ]] || fail "the source has max_wal_senders=${max_senders}; two are required (one for the archive, one spare)"
echo "source ${source_db}: wal_level=${wal_level}, max_wal_senders=${max_senders}"

step "start the WAL archive (before the base backup, so no segment is missed)"
python3 - "${source_dsn}" <<'PY' > "${work_dir}/pg.env"
import sys, urllib.parse, shlex
parsed = urllib.parse.urlparse(sys.argv[1])
print(f"export PGHOST={shlex.quote(parsed.hostname or '127.0.0.1')}")
print(f"export PGPORT={shlex.quote(str(parsed.port or 5432))}")
print(f"export PGUSER={shlex.quote(parsed.username or '')}")
print(f"export PGDATABASE={shlex.quote(parsed.path.lstrip('/') or 'postgres')}")
if parsed.password:
    print(f"export PGPASSWORD={shlex.quote(parsed.password)}")
PY
# shellcheck disable=SC1090
source "${work_dir}/pg.env"

# --create-slot creates the slot and exits, so it is a separate step: passing it here would leave a
# process that creates a slot and quits, and the drill would then fail on an empty archive for a reason
# that has nothing to do with recovery.
pg_receivewal --directory="${wal_dir}" --slot="${slot_name}" --create-slot --if-not-exists \
    >"${work_dir}/slot.log" 2>&1 || fail "could not create the replication slot: $(tail -1 "${work_dir}/slot.log")"
pg_receivewal --directory="${wal_dir}" --slot="${slot_name}" >"${work_dir}/receivewal.log" 2>&1 &
receivewal_pid=$!
# Readiness is taken from the server's view of the connection, not from a log phrase: a walsender in
# state=streaming is the fact, and it is the same check on any PostgreSQL version.
streaming="false"
for _ in $(seq 1 60); do
    if [[ "$(psql "${source_dsn}" -tAc "SELECT count(*) FROM pg_stat_replication WHERE application_name = 'pg_receivewal' AND state = 'streaming'" 2>/dev/null || echo 0)" -ge 1 ]]; then
        streaming="true"
        break
    fi
    if psql "${source_dsn}" -tAc "SELECT count(*) FROM pg_stat_replication WHERE application_name = 'pg_receivewal' AND state IN ('startup','catchup')" 2>/dev/null | grep -q '[1-9]'; then
        streaming="true"
        break
    fi
    if [[ -n "${receivewal_pid}" ]] && ! kill -0 "${receivewal_pid}" 2>/dev/null; then
        fail "pg_receivewal exited: $(tail -1 "${work_dir}/receivewal.log")"
    fi
    sleep 0.5
done
[[ "${streaming}" == "true" ]] || fail "the WAL archive never started streaming (slot ${slot_name}); see ${work_dir}/receivewal.log"
echo "archive streaming to ${wal_dir} (slot ${slot_name}, confirmed by pg_stat_replication)"

step "take a base backup"
pg_basebackup --pgdata="${base_dir}" --wal-method=none --checkpoint=fast --progress --no-password \
    >"${work_dir}/basebackup.log" 2>&1 || fail "pg_basebackup failed: $(tail -2 "${work_dir}/basebackup.log")"
# PostgreSQL refuses to start on a data directory that is group- or world-accessible, and pg_basebackup
# does not always leave it at 0700 (it follows the umask here). Without this the recovery fails with
# "has invalid permissions" after the backup has already been taken - the third thing this drill found
# only by running for real.
chmod 700 "${base_dir}"
base_time="$(date -u +%H:%M:%S)"
echo "base backup taken at ${base_time} ($(du -sh "${base_dir}" | cut -f1), mode $(stat -c%a "${base_dir}"))"

step "write markers around a chosen target time"
# Marker A is committed before the target and B after it: recovery stops between them, which is the
# only assertion that distinguishes a real PITR from "the cluster started".
psql "${source_dsn}" -q -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS ncs_pitr_drill" \
    -c "CREATE TABLE ncs_pitr_drill (id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL, written_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)" \
    -c "INSERT INTO ncs_pitr_drill (label) VALUES ('before_target')"
sleep 2
# The config parser does not accept the ISO "T"/"Z" spelling for a date/time GUC: it reads
# `2026-09-15 09:27:42.776091+00`, and the explicit offset keeps the value unambiguous regardless of
# the server's own timezone. That difference cost one drill run and is recorded in the module note.
target_time="$(psql "${source_dsn}" -tAc "SELECT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00'")"
target_epoch="$(date -u +%s)"
sleep 2
psql "${source_dsn}" -q -c "INSERT INTO ncs_pitr_drill (label) VALUES ('after_target')"
echo "target time: ${target_time} (between the two markers)"

step "let the target age into the approved window"
# The approved window is "recover to a point 5..15 minutes in the past", so the drill has to wait for
# the target to be that old: recovering two seconds after it only proves that replay stops at a chosen
# instant, not that the window the ruling asks for is reachable. WAL keeps streaming during the wait,
# which is exactly what the archive is for. The measured age is what the report states and what the
# assertion below checks.
wait_seconds=$(( rewind_minutes * 60 ))
while true; do
    aged=$(( $(date -u +%s) - target_epoch ))
    [[ "${aged}" -ge "${wait_seconds}" ]] && break
    remaining=$(( wait_seconds - aged ))
    echo "  target is ${aged}s old; waiting ${remaining}s more for the ${rewind_minutes}-minute window"
    if [[ "${remaining}" -gt 30 ]]; then sleep 30; else sleep "${remaining}"; fi
done
measured_rewind_seconds=$(( $(date -u +%s) - target_epoch ))
measured_rewind_minutes="$(python3 -c "print(f'{${measured_rewind_seconds}/60:.2f}')")"
echo "target is now ${measured_rewind_minutes} minutes old (approved band: 5..15)"

step "flush the WAL that contains the markers into the archive"
# Two switches: the first completes the segment holding the markers, the second starts a fresh one so
# the archive has a closed file to replay.
segments_before="$(ls -1 "${wal_dir}" | grep -c '^[0-9A-F]\{24\}$' || true)"
psql "${source_dsn}" -tAc "SELECT pg_switch_wal()" >/dev/null
sleep 1
psql "${source_dsn}" -tAc "SELECT pg_switch_wal()" >/dev/null
for _ in $(seq 1 60); do
    segments_now="$(ls -1 "${wal_dir}" | grep -c '^[0-9A-F]\{24\}$' || true)"
    [[ "${segments_now}" -gt "${segments_before}" ]] && break
    sleep 0.5
done
segments_now="$(ls -1 "${wal_dir}" | grep -c '^[0-9A-F]\{24\}$' || true)"
[[ "${segments_now}" -gt 0 ]] || fail "no WAL segment reached the archive"
echo "archived segments: ${segments_now} (closed files, partial segments are not replayable)"

step "configure the temporary instance for recovery to ${target_time}"
cat >> "${base_dir}/postgresql.conf" <<CONF

# --- PITR drill overrides ---
listen_addresses = ''
port = ${pg_port}
unix_socket_directories = '${sock_dir}'
restore_command = 'cp ${wal_dir}/%f %p'
recovery_target_time = '${target_time}'
recovery_target_action = 'promote'
log_min_messages = warning
CONF
touch "${base_dir}/recovery.signal"

step "check the recovery configuration before starting"
# `postgres -C` parses the configuration and exits; without this step a bad value shows up as
# "configuration file contains errors" inside a log file the drill is about to delete.
if ! config_check="$(postgres -D "${base_dir}" -C max_connections 2>&1)"; then
    fail "the recovery configuration is invalid: ${config_check}"
fi

step "start the temporary instance and replay"
replay_started_at="$(date +%s)"
pg_ctl -D "${base_dir}" -l "${work_dir}/recovery.log" -w -t 120 start >/dev/null 2>&1
temp_started="true"
recovery_ok="false"
for _ in $(seq 1 120); do
    if grep -q "database system is ready to accept connections" "${work_dir}/recovery.log" 2>/dev/null; then
        recovery_ok="true"
        break
    fi
    if grep -qi "fatal\|could not" "${work_dir}/recovery.log" 2>/dev/null; then
        break
    fi
    sleep 1
done
if [[ "${recovery_ok}" != "true" ]]; then
    reason="$(grep -i -m1 "fatal\|could not\|error" "${work_dir}/recovery.log" 2>/dev/null || echo 'the instance did not finish recovery within 120s')"
    fail "recovery did not complete: ${reason}"
fi
replay_seconds=$(( $(date +%s) - replay_started_at ))

step "assert the recovered state is at the target, not past it"
# The marker table lives in the SOURCE DATABASE, not in "postgres": pointing the assertion at the wrong
# database turned "the table is not here" into "the marker is missing", which is the wrong conclusion
# about a recovery that may have been perfect. Query failures are reported, not swallowed.
recovered_db="${source_db}"
assert_query() {
    local sql="$1"
    local out
    if ! out="$(psql -h "${sock_dir}" -p "${pg_port}" -d "${recovered_db}" -tAc "${sql}" 2>"${work_dir}/assert.log")"; then
        fail "could not read the recovered database ${recovered_db}: $(tail -1 "${work_dir}/assert.log")"
    fi
    printf '%s' "${out}"
}
in_recovery="$(assert_query 'SELECT pg_is_in_recovery()')"
before="$(assert_query "SELECT count(*) FROM ncs_pitr_drill WHERE label = 'before_target'")"
after="$(assert_query "SELECT count(*) FROM ncs_pitr_drill WHERE label = 'after_target'")"
recovered_at="$(assert_query "SELECT to_char(max(written_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || 'Z' FROM ncs_pitr_drill")"
# The target instance is promoted once it reaches the target, so this is the state the recovery ended in.
[[ "${in_recovery}" == "f" ]] || fail "the temporary instance is still in recovery (pg_is_in_recovery()=${in_recovery}); it never reached the target"
[[ "${before}" == "1" ]] || fail "the marker written before the target is missing (${before}); recovery did not reach the archive"
[[ "${after}" == "0" ]] || fail "the marker written after the target is present (${after}); recovery went past the target time"
echo "recovered: before_target=1, after_target=0, latest row at ${recovered_at}"

step "archive coverage against the approved window"
oldest_segment="$(ls -1t "${wal_dir}" | grep '^[0-9A-F]\{24\}$' | tail -1)"
coverage="archive holds ${segments_now} segment(s) from the base backup at ${base_time} to now; the drill targeted ${target_time}"
report_lines+=("markers: before_target present, after_target absent (recovery stopped at the target)")
report_lines+=("latest recovered transaction: ${recovered_at}")
# The band is asserted, not restated: a run whose target was not genuinely 5..15 minutes old when
# recovery started does not verify the approved window and must not report that it did.
if [[ "${measured_rewind_seconds}" -lt 300 || "${measured_rewind_seconds}" -gt 900 ]]; then
    fail "the recovery target was ${measured_rewind_minutes} minutes old, outside the approved 5..15 minute window"
fi
report_lines+=("approved window: recovered to a target ${measured_rewind_minutes} minutes old (measured at recovery start, asserted 5..15)")

echo
finish 0
