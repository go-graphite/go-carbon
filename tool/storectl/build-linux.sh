#!/usr/bin/env bash
# Reproduce the release cgo allocator; optional static musl needs runtime review.
set -euo pipefail
if [[ $# != 3 ]]; then
  echo 'usage: build-linux.sh OUTPUT_DIRECTORY VERSION release|static' >&2
  exit 2
fi
out=$1 version=$2 linkage=$3
mkdir -p "$out"
export GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor CGO_ENABLED=1 GOOS=linux GOARCH=amd64
case "$linkage" in
  release)
    export CC='zig cc -target x86_64-linux-gnu.2.28' CXX='zig c++ -target x86_64-linux-gnu.2.28'
    daemon_flags="-X main.BuildVersion=$version"
    tool_flags="-X main.BuildVersion=$version"
    ;;
  static)
    export CC='zig cc -target x86_64-linux-musl' CXX='zig c++ -target x86_64-linux-musl'
    daemon_flags="-X main.BuildVersion=$version -linkmode external -extldflags -static"
    tool_flags="-X main.BuildVersion=$version -linkmode external -extldflags -static"
    ;;
  *) echo 'linkage must be release or static' >&2; exit 2;;
esac
go build -ldflags "$daemon_flags" -o "$out/go-carbon-$linkage" .
go build -ldflags "$tool_flags" -o "$out/storectl-$linkage" ./tool/storectl
file "$out/go-carbon-$linkage" "$out/storectl-$linkage"
go version -m "$out/go-carbon-$linkage"
shasum -a 256 "$out/go-carbon-$linkage" "$out/storectl-$linkage"
