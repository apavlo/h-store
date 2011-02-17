#!/bin/bash -x

BENCHMARKS=( \
#    "tm1" \
    "tpcc.100w.large" \
#     "auctionmark"\
)
PARTITIONS=( \
#     8 \
#     16 \
#     32 \
#     64 \
    128 \
)
HEAP_SIZE=3072
WORKLOAD_SIZE=50000

## -----------------------------------------------------
## buildMarkovs
## -----------------------------------------------------
buildMarkovs() {
    BENCHMARK=$1
    WORKLOAD=$2
    OUTPUT=$3
    GLOBAL=$4
    ant markov \
        -Dvolt.client.memory=$HEAP_SIZE \
        -Dproject=$BENCHMARK \
        -Dworkload=files/workloads/$WORKLOAD.trace.gz \
        -Dlimit=$WORKLOAD_SIZE \
        -Doutput=$OUTPUT \
        -Dglobal=$GLOBAL || exit
}

for BENCHMARK in ${BENCHMARKS[@]}; do
    WORKLOAD=$BENCHMARK
    if [ "$BENCHMARK" = "tpcc.100w" -o "$BENCHMARK" = "tpcc.50w" -o "$BENCHMARK" = "tpcc.100w.large" ]; then
        BENCHMARK="tpcc"
#     elif [ "$BENCHMARK" = "tpce" ]; then
#         # Nothing
#     elif [ "$BENCHMARK" = "auctionmark" ]; then
#         # Nothing
#     elif [ "$BENCHMARK" = "tm1" ]; then
#         # Nothing
    fi

    ## Build project jar
    ant hstore-prepare -Dproject=$BENCHMARK
        
    for NUM_PARTITIONS in ${PARTITIONS[@]}; do
        ant catalog-fix catalog-info \
            -Dproject=$BENCHMARK \
            -Dnumhosts=$NUM_PARTITIONS \
            -Dnumsites=1 \
            -Dnumpartitions=1 \
            -Dcorrelations=files/correlations/${BENCHMARK}.correlations || exit
            
        for GLOBAL in "true" "false" ]; do
            if [ $GLOBAL = "true" ]; then
                OUTPUT=files/markovs/$BENCHMARK.global.${NUM_PARTITIONS}p.markovs
            else
                OUTPUT=files/markovs/$BENCHMARK.${NUM_PARTITIONS}p.markovs
            fi
    
            if [ ! -f $OUTPUT ]; then
                buildMarkovs $BENCHMARK $WORKLOAD $OUTPUT $GLOBAL
            fi
        done # GLOBAL
    done # PARTITIONS
done # BENCHMARK