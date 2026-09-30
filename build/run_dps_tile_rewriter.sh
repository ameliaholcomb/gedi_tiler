#!/usr/bin/env -S bash --login
set -euo pipefail
# Called by DPS. Arguments are the positional inputs of
# algorithm_config_tile_rewriter_hub.yml, in order.

basedir=$(dirname "$(readlink -f "$0")")

# DPS copies output/ to the job's results; the rewriter writes nothing
# there, so the results hold only the logs.
mkdir -p output

bucket=$1
prefix=$2
migration=$3
tile_years=$4

conda run --live-stream --name pyduck python ${basedir}/../scripts/dps_tile_rewriter.py --bucket ${bucket} --prefix ${prefix} --migration ${migration} --tile_years ${tile_years}
