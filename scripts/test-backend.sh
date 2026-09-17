#!/usr/bin/env bash
set -euo pipefail
: "${NCS_TEST_PG_DSN:?set a disposable PostgreSQL test database}"
: "${NCS_REDIS_TEST_ADDR:?set a disposable Redis address}"
: "${NCS_REDIS_TEST_DB:?set a dedicated nonzero Redis test database}"
if [[ "$NCS_REDIS_TEST_DB" == 0 ]]; then
    echo 'refusing shared Redis database 0' >&2
    exit 2
fi
cd "$(dirname "$0")/../backend"
psql "$NCS_TEST_PG_DSN" -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null
# go test may skip unreachable services; the JSON check makes that a CI failure.
go test -count=1 -race -json ./... > go-tests.jsonl
python3 - <<'CHECK'
import json
from pathlib import Path
rows = [json.loads(line) for line in Path('go-tests.jsonl').read_text().splitlines()]
skips = [r for r in rows if r.get('Action') == 'skip' and r.get('Test')]
fails = [r for r in rows if r.get('Action') == 'fail']
passed = [r for r in rows if r.get('Action') == 'pass' and r.get('Test')]
print(f'{len(passed)} tests/subtests passed, {len(skips)} skipped, {len(fails)} failed')
for r in skips + fails:
    print(r.get('Package'), r.get('Test'), r['Action'])
if skips or fails or not passed:
    raise SystemExit(1)
CHECK
