#!/usr/bin/env bash
#
# Continuous WAL archiving (B-06, the RPO mechanism).
#
# The approved RPO is at most 15 minutes. A daily dump cannot promise that, so the archive is streamed
# continuously instead: pg_receivewal runs as a service and writes each WAL segment to the archive
# directory as the server completes it. Recovery is then "restore the newest base backup or dump, then
# replay WAL to the point you want", which is what makes a 15-minute (in practice much smaller) RPO
# real rather than aspirational.
#
# Requirements on the database side:
#   wal_level = replica            (the PostgreSQL 16 default)
#   max_wal_senders >= 2           (one for pg_receivewal, one spare for a base backup)
#   a role with REPLICATION, not a superuser (see deploy/README.md for the SQL)
#
# Usage:
#   NCS_POSTGRES_DSN=postgres://... backend/scripts/wal-archive.sh [--dir DIR] [--once]
#
# --once is for a smoke check (it exits as soon as the stream is established); without it the script
# runs as a service and restarts are handled by systemd.
set -euo pipefail

: "${NCS_POSTGRES_DSN:?set NCS_POSTGRES_DSN to the database whose WAL should be archived}"
wal_dir="${NCS_BACKUP_WAL_DIR:-/var/backups/ncs/wal}"
once="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) wal_dir="$2"; shift 2 ;;
        --once) once="true"; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

command -v pg_receivewal >/dev/null 2>&1 || { echo "FAIL: pg_receivewal is not installed (postgresql-client)" >&2; exit 2; }

# pg_receivewal wants the connection parameters, not the whole DSN, and the password goes through the
# environment so it never appears in the process list.
eval "$(python3 - "$NCS_POSTGRES_DSN" <<'PARSE'
import sys, urllib.parse, shlex
parsed = urllib.parse.urlparse(sys.argv[1])
host = parsed.hostname or '127.0.0.1'
port = parsed.port or 5432
user = parsed.username or ''
dbname = parsed.path.lstrip('/') or 'postgres'
# Every assignment is exported: a plain shell variable would not reach pg_receivewal, which would
# then fall back to the default socket and a different role - the failure mode this script shipped
# with for one revision, where the smoke check "passed" while the connection had actually failed.
print(f"export PGHOST={shlex.quote(host)}")
print(f"export PGPORT={shlex.quote(str(port))}")
print(f"export PGUSER={shlex.quote(user)}")
print(f"export PGDATABASE={shlex.quote(dbname)}")
if parsed.password:
    print(f"export PGPASSWORD={shlex.quote(parsed.password)}")
PARSE
)"

mkdir -p "${wal_dir}"
echo "archiving WAL from ${PGHOST}:${PGPORT}/${PGDATABASE} to ${wal_dir}"

if [[ "${once}" == "true" ]]; then
    # A bounded smoke check that actually asserts: connect, create the slot if it is missing, and
    # verify the slot exists afterwards. Swallowing the exit code here would report success for a
    # stream that never connected, which is the one thing this check exists to catch.
    set +e
    output="$(timeout 15 pg_receivewal --directory="${wal_dir}" --slot=ncs_wal_archive --create-slot --if-not-exists --verbose --no-loop 2>&1)"
    status=$?
    set -e
    echo "${output}" | tail -3
    if echo "${output}" | grep -qi "error\|FATAL"; then
        echo "FAIL: pg_receivewal could not connect: $(echo "${output}" | grep -i 'error\|FATAL' | head -1)" >&2
        exit 1
    fi
    # --no-loop exits after one segment boundary; a timeout is therefore expected, a connection error
    # is not. The slot is the proof that the server accepted this client.
    slot="$(psql -tAc "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'ncs_wal_archive'" 2>/dev/null || true)"
    [[ "${slot}" == "ncs_wal_archive" ]] || { echo "FAIL: the replication slot was not created (exit ${status})" >&2; exit 1; }
    echo "smoke check complete: connected as ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}, slot ${slot} present, archive dir ${wal_dir}"
    exit 0
fi

exec pg_receivewal --directory="${wal_dir}" --slot=ncs_wal_archive --create-slot --if-not-exists --verbose
