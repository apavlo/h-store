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
#     "spec" \
    "occ" \
)
PARTITIONS=( 16 )
PERCENTAGES=( \
#     00
#     25
#     50
#     75
    100
)

pCnt=${#PERCENTAGES[@]}
eCnt=${#EXP_TYPES[@]}
# for b in smallbank ; do
for b in tpcc ; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --overwrite \
        --retry-on-zero \
#         --site.jvm_asserts=True \
        --exp-trials=3 \
#         --exp-attempts=1 \
        --partitions ${PARTITIONS[@]} \
        --client.duration=60000 \
    )
    
    i=0
    while [ "$i" -lt "$pCnt" ]; do
        j=0
        while [ "$j" -lt "$eCnt" ]; do
            ./experiment-runner.py $FABRIC_TYPE \
                ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
                --exp-type="hotspots-${PERCENTAGES[$i]}-${EXP_TYPES[$j]}" || break
            FIRST_PARAM_OFFSET=0
            j=`expr $j + 1`
        done
        i=`expr $i + 1`
    done
done
