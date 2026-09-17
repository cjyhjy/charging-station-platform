#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
git diff --check
if [[ -n "${NCS_CHECK_BASE_REF:-}" && ! "$NCS_CHECK_BASE_REF" =~ ^0+$ ]]; then
    git diff --check "${NCS_CHECK_BASE_REF}...HEAD"
fi
unformatted="$(find backend -name '*.go' -not -path '*/vendor/*' -exec gofmt -l {} +)"
if [[ -n "$unformatted" ]]; then
    printf 'Go files need gofmt:\n%s\n' "$unformatted" >&2
    exit 1
fi
(cd backend && go vet ./... && go build ./...)
# Archived sources must never become active compilation targets again.
python3 - <<'CHECK'
from pathlib import Path
roots = [Path('apps/user'), Path('apps/admin'), Path('backend')]
for root in roots:
    for p in root.rglob('*'):
        if 'node_modules' in p.parts or 'dist' in p.parts:
            continue
        if p.suffix in {'.cpp', '.h', '.qml'} or p.name == 'CMakeLists.txt':
            raise SystemExit(f'legacy build source outside archive: {p}')
print('Go checks and active-source boundaries passed')
CHECK
