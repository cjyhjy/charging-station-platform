#!/usr/bin/env bash
#
# Off-site backup push (B-06, 30-day retention at the destination).
#
# The approved baseline keeps 7 days on the host, 12 weeks of weekly snapshots, and 30 days off site.
# The off-site copy is the one that survives losing the host, so it is encrypted before it leaves and
# uploaded with credentials that are not the database's:
#
#   * encryption: openssl AES-256-CBC with PBKDF2, key read from a separate 0600 file (never an
#     argument, never the repository, never the systemd EnvironmentFile that the API uses);
#   * transport: encrypted by the object store client (rclone with an https/S3 remote);
#   * credentials: a dedicated rclone remote whose config file lives outside the repository;
#   * retention: versioning enabled at the destination plus a 30-day delete of unpinned objects, so a
#     mistaken local delete cannot remove the remote copy;
#   * the backup role is not a superuser (see deploy/README.md).
#
# The script is deliberately explicit about what it cannot check here: this verification host has no
# object storage and no rclone, so the upload path is delivered as configuration and is not part of
# the local evidence. It refuses to run rather than pretending.
#
# Usage:
#   NCS_BACKUP_REMOTE_CONFIG=/etc/ncs/backup-remote.env \
#       backend/scripts/push-backup-remote.sh [--dump FILE] [--all] [--dry-run]
#
# /etc/ncs/backup-remote.env (0600 root:ncs, not in the repository):
#   NCS_BACKUP_REMOTE=ncs-backup-prod:ncs-prod-backup          # remote name and bucket are frozen
#   NCS_BACKUP_ENCRYPTION_KEY_FILE=/etc/ncs/backup.key
#   NCS_RCLONE_CONFIG=/etc/ncs/rclone/rclone.conf              # optional; rclone's own default otherwise
#
# The bucket carries three protections that this script cannot turn on and the deployment must:
#   object versioning      so a wrong delete or overwrite is recoverable
#   server-side encryption so the stored object is encrypted at rest as well as in transit
#   a lifecycle rule       that expires the daily/ prefix at 30 days and the weekly/ prefix at 12 weeks
#
# Retention here mirrors the approved baseline: local dumps 7 days, remote daily 30 days, remote weekly
# 12 weeks. The prefixes make those three policies expressible as one lifecycle rule each.
set -euo pipefail

config="${NCS_BACKUP_REMOTE_CONFIG:-/etc/ncs/backup-remote.env}"
dry_run="false"
mode="newest"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dump) dump="$2"; mode="one"; shift 2 ;;
        --all) mode="all"; shift ;;
        --dry-run) dry_run="true"; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }

[[ -r "${config}" ]] || fail "remote backup configuration not readable: ${config}
create it with 0600 root:ncs and the two variables documented in this script's header"
# shellcheck disable=SC1090
source "${config}"
: "${NCS_BACKUP_REMOTE:?set NCS_BACKUP_REMOTE in ${config} (the approved value is ncs-backup-prod:ncs-prod-backup)}"
: "${NCS_BACKUP_ENCRYPTION_KEY_FILE:?set NCS_BACKUP_ENCRYPTION_KEY_FILE in ${config}}"
rclone_config="${NCS_RCLONE_CONFIG:-/etc/ncs/rclone/rclone.conf}"
rclone_args=()
# The remote configuration and its access keys live outside the repository by rule; when the file is
# absent rclone falls back to its own default location, which the deployment is expected to provision.
if [[ -r "${rclone_config}" ]]; then
    rclone_args+=(--config "${rclone_config}")
fi
# Server-side encryption is requested per upload as well, so the object is encrypted at rest even if the
# bucket default were changed by mistake.
rclone_args+=(--s3-server-side-encryption AES256)
[[ -r "${NCS_BACKUP_ENCRYPTION_KEY_FILE}" ]] || fail "encryption key file not readable: ${NCS_BACKUP_ENCRYPTION_KEY_FILE}"

command -v rclone >/dev/null 2>&1 || fail "rclone is required for the off-site copy; install it on the backup host"
command -v openssl >/dev/null 2>&1 || fail "openssl is required to encrypt the backup before it leaves the host"

backup_dir="${NCS_BACKUP_DIR:-/var/backups/ncs/postgres}"
case "${mode}" in
    one) [[ -f "${dump}" ]] || fail "dump not found: ${dump}"; dumps=("${dump}") ;;
    all) mapfile -t dumps < <(find "${backup_dir}" -maxdepth 2 -name '*.dump' -type f) ;;
    *)   mapfile -t dumps < <(find "${backup_dir}" -maxdepth 2 -name '*.dump' -type f -printf '%T@ %p\n' | sort -rn | head -1 | cut -d' ' -f2-) ;;
esac
[[ ${#dumps[@]} -gt 0 ]] || fail "no dump to push in ${backup_dir}"

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT INT TERM

pushed=0
for source_dump in "${dumps[@]}"; do
    step "encrypt $(basename "${source_dump}")"
    # The encrypted copy carries the same name plus .enc; the manifest travels beside it so the
    # destination holds the checksum of the plaintext it no longer has.
    encrypted="${work_dir}/$(basename "${source_dump}").enc"
    openssl enc -aes-256-cbc -pbkdf2 -iter 200000 -salt \
        -pass "file:${NCS_BACKUP_ENCRYPTION_KEY_FILE}" \
        -in "${source_dump}" -out "${encrypted}"
    cat "${source_dump}.manifest" > "${encrypted}.manifest" 2>/dev/null || true

    # daily/ and weekly/ are separate prefixes because they have different retention (30 days versus
    # 12 weeks); a single prefix could only carry one lifecycle rule.
    if [[ "${source_dump}" == *"/weekly/"* ]]; then
        prefix="weekly"
    else
        prefix="daily"
    fi
    target="${NCS_BACKUP_REMOTE}/${prefix}/$(date -u +%Y/%m/%d)/"
    step "upload to ${target}"
    args=("${rclone_args[@]}" copy "${work_dir}" "${target}" --include "*.dump.enc*" --s3-no-check-bucket)
    [[ "${dry_run}" == "true" ]] && args+=(--dry-run)
    rclone "${args[@]}"

    # An upload that returned success is not the same as an object that can be read back: a truncated
    # or half-written object is the failure mode that stays invisible until the day the backup is
    # needed. The check compares size and hash against the encrypted files that were just sent -
    # the same files, so a mismatch is a real difference in what the destination holds.
    if [[ "${dry_run}" != "true" ]]; then
        step "verify the uploaded object against what was sent"
        check_args=("${rclone_args[@]}" check "${work_dir}" "${target}" --include "*.dump.enc*" --s3-no-check-bucket)
        rclone "${check_args[@]}"
    fi
    pushed="$((pushed + 1))"
done

step "retention at the destination (daily 30 days, weekly 12 weeks)"
# A lifecycle rule on the bucket is the primary mechanism and is what the deployment must configure;
# this pass is the belt to that pair of braces and only deletes encrypted objects past their window.
for retention in "daily:30d" "weekly:12w"; do
    prefix="${retention%%:*}"
    age="${retention##*:}"
    args=("${rclone_args[@]}" delete "${NCS_BACKUP_REMOTE}/${prefix}" --min-age "${age}" --include "*.dump.enc*" --s3-no-check-bucket)
    [[ "${dry_run}" == "true" ]] && args+=(--dry-run)
    rclone "${args[@]}"
done

echo
echo "pushed ${pushed} encrypted backup(s) to ${NCS_BACKUP_REMOTE} (daily prefix kept 30 days, weekly prefix kept 12 weeks)"
echo "remote configuration: ${rclone_config} (keys stay outside the repository)"
echo "REMOTE_PUSHED=${pushed}"
