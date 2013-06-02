#!/bin/bash -x

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    exit $exit_status
}

# ---------------------------------------------------------------------

DATA_DIR="/home/pavlo/Documents/H-Store/Papers/speculative/data"
FABRIC_TYPE="ssh"
FIRST_PARAM_OFFSET=1

PERCENTAGES=( \
    00
    20
    40
    60
)
PARTITIONS=( 16 )

for b in tpcc ; do
    PARAMS=( \
        --no-update \
        --results-dir=$DATA_DIR \
        --benchmark=$b \
        --stop-on-error \
        --overwrite \
        --exp-trials=1 \
        --partitions ${PARTITIONS[@]} \
        --client.duration=300000 \
    )
    
    i=0
    cnt=${#PERCENTAGES[@]}
    while [ "$i" -lt "$cnt" ]; do
        ./experiment-runner.py $FABRIC_TYPE \
            ${PARAMS[@]:$FIRST_PARAM_OFFSET} \
            --exp-type="aborts-${PERCENTAGES[$i]}" || break
        FIRST_PARAM_OFFSET=0
        i=`expr $i + 1`
    done

done
