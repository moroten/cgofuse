#!/usr/bin/env bash
set -eEuo pipefail
mkdir -p tmp/backend-storage
./bb tmp/backend-storage B: '--VolumePrefix=\LocalFuseSystem\BuildBarnWorker'
