#!/usr/bin/env -S bash --login
set -euo pipefail
# DPS entry point for scripts/dps_s3_probe.py. Takes no arguments.

basedir=$(dirname "$(readlink -f "$0")")
mkdir -p output

conda run --live-stream --name pyduck python ${basedir}/../scripts/dps_s3_probe.py 2>&1 | tee output/probe.txt
