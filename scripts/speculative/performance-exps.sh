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
    "performance-spec-all" \
    "performance-spec-query" \
    "performance-spec-txn" \
#      "performance-nospec" \
)
PARTITIONS=( \
#      8 \
#     16 \
    32 \
)

# for b in seats smallbank tpcc ; do
for b in tpcc ; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --overwrite \
#         --retry-on-zero \
        --exp-trials=3 \
        --partitions ${PARTITIONS[@]} \
#         --client.warmup=0 \
        --client.duration=180000 \
#         --client.blocking_concurrent=2 \
#         --site.exec_force_undo_logging_all=true \
#         --site.jvm_asserts=true \
#         --client.txnrate=500 \
#         --client.threads_per_host=100 \
#         --client.scalefactor=1 \
#         --debug-log4j-site \
    )
    
    i=0
    cnt=${#EXP_TYPES[@]}
    while [ "$i" -lt "$cnt" ]; do
        ./experiment-runner.py $FABRIC_TYPE \
            ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
            --exp-type=${EXP_TYPES[$i]}
        FIRST_PARAM_OFFSET=0
        i=`expr $i + 1`
    done

done
