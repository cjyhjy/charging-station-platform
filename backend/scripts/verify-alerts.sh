#!/usr/bin/env bash
#
# Alert rule verification (B-06 补验).
#
# The two checks that make the alert rules reviewable without a live monitoring stack:
#
#   promtool check rules   the delivered file parses and every rule is well formed
#   promtool test rules    thresholds and `for:` durations, evaluated from synthetic series in
#                          seconds instead of waiting 15 minutes, including a healthy/idle
#                          scenario that must produce no alert at all
#
# Both run against the files in the repository, unmodified. The live firing drill (a real
# Prometheus with a controlled fault source) is a separate, slower exercise: see
# docs/migration/b-06-golive-verification.md.
#
# Usage:
#   backend/scripts/verify-alerts.sh
#   NCS_PROMTOOL=/path/to/promtool backend/scripts/verify-alerts.sh
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
monitoring_dir="${script_dir}/../deploy/monitoring"
rules="${monitoring_dir}/ncs-alerts.yml"
tests="${monitoring_dir}/ncs-alerts.test.yml"
promtool_bin="${NCS_PROMTOOL:-promtool}"

fail() { echo "FAIL: $*" >&2; exit 1; }

[[ -r "${rules}" ]] || fail "alert rules not found: ${rules}"
[[ -r "${tests}" ]] || fail "alert rule tests not found: ${tests}"

command -v "${promtool_bin}" >/dev/null 2>&1 || fail "promtool is required to verify the alert rules; install the Prometheus release that matches the deployment (set NCS_PROMTOOL to use a local build)"

echo "=== check rules: $(basename "${rules}") ==="
"${promtool_bin}" check rules "${rules}" || fail "the alert rules do not parse"

echo
echo "=== test rules: $(basename "${tests}") ==="
# promtool resolves rule_files relative to the test file, so run it from that directory.
(cd "${monitoring_dir}" && "${promtool_bin}" test rules "$(basename "${tests}")") \
    || fail "the alert rules do not behave as their thresholds and for-durations claim"

echo
echo "alert rules verified: $(basename "${rules}") against $(basename "${tests}")"
