#!/usr/bin/env bash
set -eEuo pipefail
mkdir -p tmp/backend-storage tmp/frontend
./bb tmp/backend-storage tmp/frontend -o allow_other,default_permissions,use_ino,attr_timeout=0
