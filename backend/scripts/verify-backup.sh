#!/usr/bin/env bash
#
# Backup integrity verification (B-06, daily).
#
# A backup that cannot be read is not a backup, and the failure is silent until somebody needs it. So
# this runs daily against the newest dumps: it checks the manifest checksum, reads the archive's table
# of contents, and optionally restores into a scratch database when --restore is given (which is what
# the weekly drill does with a real RPO/RTO measurement).
#
# Usage:
#   backend/scripts/verify-backup.sh [--dir DIR] [--count N] [--restore]
#   NCS_VERIFY_RESTORE_DSN=postgres://.../scratch   required with --restore
set -euo pipefail

backup_dir="${NCS_BACKUP_DIR:-/var/backups/ncs/postgres}"
count="${NCS_VERIFY_COUNT:-3}"
restore="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) backup_dir="$2"; shift 2 ;;
        --count) count="$2"; shift 2 ;;
        --restore) restore="true"; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }

[[ -d "${backup_dir}" ]] || fail "backup directory not found: ${backup_dir}"

mapfile -t dumps < <(find "${backup_dir}" -maxdepth 1 -name '*.dump' -type f -printf '%T@ %p\n' | sort -rn | head -"${count}" | cut -d' ' -f2-)
[[ ${#dumps[@]} -gt 0 ]] || fail "no dump found in ${backup_dir}"

verified=0
for dump in "${dumps[@]}"; do
    step "verify $(basename "${dump}")"
    [[ -f "${dump}.manifest" ]] || fail "manifest missing for ${dump}: the checksum cannot be checked"
    recorded="$(sed -n 's/^sha256=//p' "${dump}.manifest")"
    actual="$(if command -v sha256sum >/dev/null 2>&1; then sha256sum "${dump}" | cut -d' ' -f1; else shasum -a 256 "${dump}" | cut -d' ' -f1; fi)"
    [[ "${recorded}" == "${actual}" ]] || fail "checksum mismatch for ${dump}: ${recorded} != ${actual}"

    # The archive's own table of contents is the cheapest proof that it is readable end to end.
    objects="$(pg_restore --list "${dump}" | grep -c ';' || true)"
    [[ "${objects}" -gt 0 ]] || fail "${dump} lists no objects"

    if [[ "${restore}" == "true" ]]; then
        : "${NCS_VERIFY_RESTORE_DSN:?set NCS_VERIFY_RESTORE_DSN with --restore}"
        bash "$(dirname "${BASH_SOURCE[0]}")/restore-postgres.sh" --dump "${dump}" --dsn "${NCS_VERIFY_RESTORE_DSN}" >/dev/null
        tables="$(psql "${NCS_VERIFY_RESTORE_DSN}" -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")"
        [[ "${tables}" -gt 0 ]] || fail "the restored copy has no tables"
        version="$(psql "${NCS_VERIFY_RESTORE_DSN}" -tAc 'SELECT coalesce(max(version),0) FROM schema_migrations')"
        [[ "${version}" -ge 7 ]] || fail "restored schema version = ${version}, want at least 7"
        echo "  restored into a scratch database: ${tables} tables, schema version ${version}"
    fi

    echo "  sha256 ok, ${objects} objects"
    verified="$((verified + 1))"
done

echo
echo "PASS: verified ${verified} backup(s) in ${backup_dir}"
