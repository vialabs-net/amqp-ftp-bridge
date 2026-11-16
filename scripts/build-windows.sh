#!/usr/bin/env bash
# Builds the Windows executable with its version resource.
#
#   scripts/build-windows.sh <version> <commit> <output.exe>
#
# <version> is semantic, without "v": 1.2.3 or 1.2.3-rc.1. The file version
# in the resource is the numeric part plus ".0" (1.2.3.0); the product
# version string is the full value.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

version=${1:?version}
commit=${2:?commit}
out=${3:?output}

if [[ ! $version =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "version must look like 1.2.3 or 1.2.3-rc.1: $version" >&2
  exit 1
fi
for n in "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"; do
  # Windows version fields are 16-bit.
  if (( 10#$n > 65535 )); then
    echo "version field too large for Windows: $n" >&2
    exit 1
  fi
done
file_version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}.0"

GO_WINRES_VERSION=v0.3.3
pkg=./cmd/amqp-ftp-bridge
rm -f "$pkg"/rsrc_windows_*.syso
trap 'rm -f "$pkg"/rsrc_windows_*.syso' EXIT
go run "github.com/tc-hib/go-winres@${GO_WINRES_VERSION}" make \
  --in "$pkg/winres.json" --arch amd64 --out "$pkg/rsrc" \
  --file-version "$file_version" --product-version "$version"

mod=github.com/vialabs-net/amqp-ftp-bridge/internal/version
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X $mod.Version=$version -X $mod.Commit=$commit" \
  -o "$out" "$pkg"
echo "built $out ($version, file version $file_version)"
