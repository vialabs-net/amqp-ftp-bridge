#!/usr/bin/env bash
# Fails when a tracked file holds a private IPv4 address (10/8, 172.16/12,
# 192.168/16) or a domain name outside the documentation ones.
#
# Allowed without listing: example.com/.net/.org and their subdomains,
# *.example, *.example.internal, *.test, *.invalid, localhost.
# Other false positives go in .leak-allowlist: one extended regex per line,
# matched against "path:finding". Lines starting with # are comments.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

allowlist=.leak-allowlist
self=scripts/check-leaks.sh

# Generated or binary-like files that only hold module paths.
mapfile -d '' files < <(git ls-files -z -- . ':!go.sum' ":!$self")

ip_re='\b(10\.([0-9]{1,3}\.){2}[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3})\b'
# Host names with a public or common private top-level domain.
tlds='com|net|org|io|dev|app|cloud|co|ai|biz|xyz|me|tech|online|site|internal|local|lan|corp|home|intra|private|cl|ar|mx|pe|br|uy|es|uk|de|fr|it|nl|us|ca|eu|au|jp|cn|in|ru'
host_re="\\b([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\\.)+(${tlds})\\b"
documentation_re='(^|\.)(example\.(com|net|org)|example(\.internal)?|test|invalid|localhost)$'

findings=()
while IFS= read -r line; do
  findings+=("$line")
done < <(
  {
    grep -nIoE "$ip_re" -- "${files[@]}" 2>/dev/null | sed 's/^/ip:/' || true
    grep -nIoE "$host_re" -- "${files[@]}" 2>/dev/null | sed 's/^/host:/' || true
  }
)

allowed() {
  local item=$1
  [[ -f $allowlist ]] || return 1
  while IFS= read -r re; do
    [[ -z $re || $re == \#* ]] && continue
    if [[ $item =~ $re ]]; then return 0; fi
  done <"$allowlist"
  return 1
}

fail=0
for f in "${findings[@]}"; do
  kind=${f%%:*}
  rest=${f#*:}
  path=${rest%%:*}
  rest=${rest#*:}
  lineno=${rest%%:*}
  value=${rest#*:}
  if [[ $kind == host ]]; then
    lower=${value,,}
    [[ $lower =~ $documentation_re ]] && continue
  fi
  if allowed "$path:$value"; then continue; fi
  echo "$path:$lineno: $kind $value"
  fail=1
done

if [[ $fail -ne 0 ]]; then
  echo
  echo "Private addresses or real domains found. Use example.com, *.example.internal and 192.0.2.0/24,"
  echo "or add a regex to $allowlist for a false positive."
  exit 1
fi
echo "check-leaks: ok (${#files[@]} files)"
