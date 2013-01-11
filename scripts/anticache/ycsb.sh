#!/bin/bash -x

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    pkill -f hstore.tag
    exit $exit_status
}

# ---------------------------------------------------------------------

ENABLE_ANTICACHE=true

SITE_HOST="modis"

CLIENT_HOSTS=( \
	"modis2" \
	"modis2" \
)

BASE_CLIENT_THREADS=25
BASE_SITE_MEMORY=8192
BASE_SITE_MEMORY_PER_PARTITION=0
BASE_PROJECT="ycsb"
BASE_DIR=`pwd`

ANTICACHE_EVICT_SIZE=268400000
ANTICACHE_THRESHOLD=.75

BASE_ARGS=( \
    "-Dsite.status_enable=true" \
    
    # SITE DEBUG
#     "-Dsite.status_enable=true" \
#     "-Dsite.status_interval=10000" \
#     "-Dsite.status_exec_info=true" \
#     "-Dsite.exec_profiling=true" \
#     "-Dsite.network_profiling=false" \
#     "-Dsite.log_backup=true"\
    
    # Site Params
    "-Dsite.cpu_affinity_one_partition_per_core=true" \
    "-Dsite.exec_preprocessing_threads=false" \
    "-Dsite.exec_preprocessing_threads_count=2" \
    "-Dsite.exec_postprocessing_threads=false" \
    "-Dsite.queue_incoming_max_per_partition=500" \
    "-Dsite.queue_incoming_increase_max=2000" \

    # Client Params
    "-Dclient.scalefactor=1" \
    "-Dclient.output_clients=true" \
    "-Dclient.memory=2048" \
    "-Dclient.txnrate=2000" \
    "-Dclient.warmup=30000" \
    "-Dclient.duration=30000" \
    "-Dclient.shared_connection=false" \
    "-Dclient.blocking=false" \
    "-Dclient.blocking_concurrent=100" \
    "-Dclient.throttle_backoff=100" \
    
    # Anti-Caching Experiments
    "-Dsite.anticache_enable=${ENABLE_ANTICACHE}" \
    "-Dsite.anticache_check_interval=30000" \
    "-Dsite.anticache_evict_size=${ANTICACHE_EVICT_SIZE}" \
    "-Dsite.anticache_threshold=${ANTICACHE_THRESHOLD}" \
    "-Dclient.interval=500" \
	"-Dclient.output_txn_counters=txncounters.csv" \
    "-Dclient.anticache_enable=false" \
    "-Dclient.anticache_evict_interval=30000" \
    "-Dclient.anticache_evict_size=4194304" \
    "-Dclient.output_csv=false" \
    "-Dclient.output_interval=false" \

    # CLIENT DEBUG
    "-Dclient.profiling=false" \
    "-Dclient.output_response_status=true" \
#     "-Dclient.output_basepartitions=true" \
#     "-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" 
)

EVICTABLE_TABLES=( \
    "usertable" \
)
EVICTABLES=""
for t in ${EVICTABLE_TABLES[@]}; do
    EVICTABLES="${t},${EVICTABLES}"
done

ant compile
for i in 8; do

    HSTORE_HOSTS="${SITE_HOST}:0:0-"`expr $i - 1`
    NUM_CLIENTS=`expr $i \* $BASE_CLIENT_THREADS`
#     if [ $i -gt 1 ]; then
#         NUM_CLIENTS=`expr \( $i - 1 \) \* $BASE_CLIENT_THREADS`
#     else
#         NUM_CLIENTS=$BASE_CLIENT_THREADS
#     fi
#    SITE_MEMORY=`expr $BASE_SITE_MEMORY + \( $i \* $BASE_SITE_MEMORY_PER_PARTITION \)`
     SITE_MEMORY=$BASE_SITE_MEMORY
   
    # BUILD PROJECT JAR
    ant hstore-prepare \
        -Dproject=${BASE_PROJECT} \
        -Dhosts=${HSTORE_HOSTS} \
        -Devictable=${EVICTABLES}
    test -f ${BASE_PROJECT}.jar || exit -1
    
    # UPDATE CLIENTS
    CLIENT_COUNT=0
    CLIENT_HOSTS_STR=""
    for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
        if [ $CLIENT_HOST != $SITE_HOST ]; then
            scp -r ${BASE_PROJECT}.jar ${CLIENT_HOST}:${BASE_DIR}
        fi
        CLIENT_COUNT=`expr $CLIENT_COUNT + 1`
        if [ ! -z "$CLIENT_HOSTS_STR" ]; then
            CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR},"
        fi
        CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}${CLIENT_HOST}"
    done
    
    # EXECUTE BENCHMARK
    ant hstore-benchmark ${BASE_ARGS[@]} \
        -Dproject=${BASE_PROJECT} \
        -Dkillonzero=false \
        -Dclient.threads_per_host=25 \
        -Dsite.memory=${SITE_MEMORY} \
        -Dclient.hosts=${CLIENT_HOSTS_STR} \
        -Dclient.count=${CLIENT_COUNT}
    result=$?
    if [ $result != 0 ]; then
        exit $result
    fi
done
