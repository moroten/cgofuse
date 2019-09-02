#!/usr/bin/env bash
set -eEuo pipefail
# CGO_ENABLED=1
GOOS=linux GOARCH=amd64 go build -v ./examples/bb

go build -v ./examples/memfs
go build -v ./examples/passthrough
