#!/usr/bin/env bash
#
# PostgreSQL restore (B-06).
#
# This is the other half of the backup script, and the half that is only trustworthy if it has been
# rehearsed: a restore that has never run is a hope. It restores a dump into a target database,
# creating that database when it does not exist and refusing anything whose name does not look
# disposable, because the first thing a restore does is drop what is there.
#
# Usage:
#   backend/scripts/restore-postgres.sh --dump FILE --dsn postgres://.../target [--jobs N]
#   NCS_RESTORE_ALLOW_NON_DISPOSABLE=true   only if the target really is not disposable
set -euo pipefail

dump=""
target_dsn=""
jobs="${NCS_RESTORE_JOBS:-2}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dump) dump="$2"; shift 2 ;;
        --dsn) target_dsn="$2"; shift 2 ;;
        --jobs) jobs="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }

[[ -n "${dump}" ]] || fail "--dump is required"
[[ -f "${dump}" ]] || fail "dump not found: ${dump}"
[[ -n "${target_dsn}" ]] || fail "--dsn is required"

target_name="$(python3 - "$target_dsn" <<'READDB'
import sys, urllib.parse
print(urllib.parse.urlparse(sys.argv[1]).path.lstrip('/') or '')
READDB
)"
[[ -n "${target_name}" ]] || fail "could not read a database name from --dsn"

if [[ "${NCS_RESTORE_ALLOW_NON_DISPOSABLE:-}" != "true" ]]; then
    case "${target_name}" in
        *test*|*dev*|*ci*|*scratch*|*sandbox*|ncs_*|*drill*) ;;
        *)
            echo "refusing to restore into \"${target_name}\": the name does not look disposable." >&2
            echo "a restore drops what is already there; set NCS_RESTORE_ALLOW_NON_DISPOSABLE=true to override" >&2
            exit 2
            ;;
    esac
fi

maintenance_dsn="$(python3 - "$target_dsn" <<'MAINT'
import sys, urllib.parse
parsed = urllib.parse.urlparse(sys.argv[1])
print(parsed._replace(path='/postgres').geturl())
MAINT
)"

step "recreate ${target_name}"
psql "${maintenance_dsn}" -q -v ON_ERROR_STOP=1 \
    -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${target_name}' AND pid <> pg_backend_pid()" \
    -c "DROP DATABASE IF EXISTS \"${target_name}\"" \
    -c "CREATE DATABASE \"${target_name}\""

step "restore $(basename "${dump}")"
started="$(date +%s)"
pg_restore --dbname="${target_dsn}" --no-owner --no-privileges --exit-on-error --jobs="${jobs}" "${dump}"
duration="$(( $(date +%s) - started ))"

step "verify the restored database"
version="$(psql "${target_dsn}" -tAc 'SELECT coalesce(max(version), 0) FROM schema_migrations' 2>/dev/null || echo 0)"
orders="$(psql "${target_dsn}" -tAc 'SELECT count(*) FROM charging_orders' 2>/dev/null || echo 0)"
outbox="$(psql "${target_dsn}" -tAc 'SELECT count(*) FROM outbox_events' 2>/dev/null || echo 0)"
echo "schema version: ${version}"
echo "orders:         ${orders}"
echo "outbox rows:    ${outbox}"

echo
echo "restored ${target_name} from $(basename "${dump}") in ${duration}s"
echo "RESTORE_SECONDS=${duration}"
echo "RESTORED_SCHEMA_VERSION=${version}"
