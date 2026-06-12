#!/usr/bin/env bash
set -euo pipefail
payload="$(cat)"
if command -v jq >/dev/null 2>&1; then
  file="$(printf '%s' "$payload" | jq -r '.tool_input.file_path // empty')"
else
  file="$(printf '%s' "$payload" | grep -oE '"file_path"[^"]*"[^"]*"' | head -1 | sed -E 's/.*"file_path"[^"]*"([^"]*)"/\1/')"
fi
[ -z "${file:-}" ] && exit 0
[ -f "$file" ] || exit 0
case "$file" in
  *.py)
    command -v ruff >/dev/null 2>&1 && ruff format "$file" && ruff check --fix "$file" || true
    command -v mypy >/dev/null 2>&1 && mypy "$file" || true
    ;;
  *.sql)
    command -v sqlfluff >/dev/null 2>&1 && sqlfluff fix --dialect clickhouse "$file" || true
    ;;
esac
exit 0
