#!/usr/bin/env -S bash --login
set -euo pipefail
# This script is the one that is called by the DPS.
# Use this script to prepare input paths for any files
# that are downloaded by the DPS and outputs that are
# required to be persisted

# Get current location of build script
basedir=$(dirname "$(readlink -f "$0")")

# Create output directory to store outputs.
# The name is output as required by the DPS.
# Note how we dont provide an absolute path
# but instead a relative one as the DPS creates
# a temp working directory for our code.

mkdir -p output

# DPS downloads all files provided as inputs to
# this directory called input.
INPUT_DIR=input

# Read the positional argument as defined in the algorithm registration here
bucket=$1
prefix=$2
tile_id=$3

# Is there a better way to pass boolean flags?
if [ -n "$4" ]; then
    test="--test"
else
    test=""
fi
if [ -n "$5" ]; then
    quality="--quality"
else
    quality=""
fi

# Call the script using the absolute paths
# Use the updated environment when calling 'conda run'
# This lets us run the same way in a Terminal as in DPS
# Any output written to the stdout and stderr streams will be
# automatically captured and placed in the output dir

conda run --live-stream --name python python ${basedir}/gtiler/dps_tile_builder.py --bucket ${bucket} --prefix ${prefix} --tile_id ${tile_id} ${test} ${quality}
