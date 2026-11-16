#!/usr/bin/env bash
# Usage: scripts/coverage.sh <coverprofile> <minimum percent>
# Fails when the total statement coverage is below the minimum.
set -euo pipefail
profile=${1:?coverprofile}
min=${2:?minimum}
total=$(go tool cover -func="$profile" | awk '/^total:/ {gsub("%", "", $3); print $3}')
echo "total coverage: ${total}% (minimum ${min}%)"
awk -v t="$total" -v m="$min" 'BEGIN { exit (t + 0 >= m + 0) ? 0 : 1 }' || {
  echo "coverage below ${min}%"
  exit 1
}
