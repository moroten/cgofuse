#!/usr/bin/env bash
set -eEuo pipefail
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -v ./examples/bb
