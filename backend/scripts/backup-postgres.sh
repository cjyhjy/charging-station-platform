#!/usr/bin/env bash
#
# PostgreSQL backup (B-06).
#
# The approved production baseline is: 30 days of retention and an RPO of at most 15 minutes. This
# script takes one custom-format dump and prunes dumps older than the retention window, so running it
# from a systemd timer every 15 minutes meets the RPO by construction: at most one interval of writes
# can be lost. A tighter RPO needs WAL archiving (continuous recovery); the dump interval is the
# honest bound this script can promise, and docs/operations-guide.md says so.
#
# Usage:
#   NCS_POSTGRES_DSN=postgres://... backend/scripts/backup-postgres.sh [--dir DIR] [--retention-days N]
#   NCS_BACKUP_ALLOW_NON_DISPOSABLE=true   only if the target really is not disposable
#
# The dump is written to a temporary file and renamed only after pg_restore can read its table of
# contents, so a truncated dump never looks like a usable backup.
set -euo pipefail

: "${NCS_POSTGRES_DSN:?set NCS_POSTGRES_DSN to the database to back up}"
backup_dir="${NCS_BACKUP_DIR:-/var/backups/ncs/postgres}"
retention_days="${NCS_BACKUP_RETENTION_DAYS:-30}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) backup_dir="$2"; shift 2 ;;
        --retention-days) retention_days="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }

database_name="$(python3 - "$NCS_POSTGRES_DSN" <<'READDB'
import sys, urllib.parse
print(urllib.parse.urlparse(sys.argv[1]).path.lstrip('/') or '')
READDB
)"
[[ -n "${database_name}" ]] || fail "could not read a database name from NCS_POSTGRES_DSN"

# A dump of a production database lands on disk in the clear; the name check is what keeps an
# accidental run pointed at the wrong place from being routine.
if [[ "${NCS_BACKUP_ALLOW_NON_DISPOSABLE:-}" != "true" ]]; then
    case "${database_name}" in
        *test*|*dev*|*ci*|*scratch*|*sandbox*|ncs_*) ;;
        *)
            echo "refusing to back up database \"${database_name}\": the name does not look disposable." >&2
            echo "if it really is a disposable environment, set NCS_BACKUP_ALLOW_NON_DISPOSABLE=true" >&2
            exit 2
            ;;
    esac
fi

mkdir -p "${backup_dir}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="${backup_dir}/${database_name}-${stamp}.dump"
temporary="${target}.partial"

step "dump ${database_name}"
started="$(date +%s)"
pg_dump --format=custom --compress=6 --no-owner --no-privileges --file="${temporary}" "${NCS_POSTGRES_DSN}"
duration="$(( $(date +%s) - started ))"

step "verify the dump is readable"
pg_restore --list "${temporary}" > "${temporary}.toc"
entries="$(grep -c ';' "${temporary}.toc" || true)"
[[ "${entries}" -gt 0 ]] || fail "the dump lists no objects: ${temporary}"
mv "${temporary}" "${target}"
mv "${temporary}.toc" "${target}.toc"

# The manifest is what the restore drill and an operator read: it records the numbers the RPO/RTO
# targets are measured against, and the checksum proves the file on disk is the file that was taken.
checksum="$(sha256sum "${target}" | cut -d' ' -f1)"
size_bytes="$(stat -c%s "${target}")"
cat > "${target}.manifest" <<MANIFEST
database=${database_name}
taken_at=${stamp}
duration_seconds=${duration}
size_bytes=${size_bytes}
sha256=${checksum}
objects=${entries}
retention_days=${retention_days}
MANIFEST

step "prune dumps older than ${retention_days} days"
pruned=0
while IFS= read -r old; do
    [[ -n "${old}" ]] || continue
    rm -f "${old}" "${old}.toc" "${old}.manifest"
    pruned="$((pruned + 1))"
done < <(find "${backup_dir}" -maxdepth 1 -name "${database_name}-*.dump" -type f -mtime "+${retention_days}")
echo "pruned ${pruned} expired dump(s)"

echo
echo "backup:   ${target}"
echo "size:     ${size_bytes} bytes, ${duration}s, ${entries} objects"
echo "sha256:   ${checksum}"
echo "retention: ${retention_days} days (RPO is the backup interval, not this window)"
