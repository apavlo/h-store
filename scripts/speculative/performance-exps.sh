#!/bin/bash

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    exit $exit_status
}

# ---------------------------------------------------------------------

DATA_DIR="/home/pavlo/Documents/H-Store/Papers/speculative/data"
FABRIC_TYPE="ssh"
FIRST_PARAM_OFFSET=0

EXP_TYPES=( \
    "performance-nospec" \
#     "performance-spec-txn" \
#     "performance-spec-query" \
#     "performance-spec-all" \
)
PARTITIONS=( \
    8 \
    16 \
#     32 \
)

# for b in smallbank tpcc seats; do
for b in smallbank tpcc seats; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=3 \
        --partitions ${PARTITIONS[@]} \
        --client.duration=60000
    )
    
    i=0
    cnt=${#EXP_TYPES[@]}
    while [ "$i" -lt "$cnt" ]; do
        ./experiment-runner.py $FABRIC_TYPE ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
            --exp-type=${EXP_TYPES[$i]}
        FIRST_PARAM_OFFSET=0
        i=`expr $i + 1`
    done

done
