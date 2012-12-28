#!/bin/bash -x

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    pkill -f hstore.tag
    exit $exit_status
}

# ---------------------------------------------------------------------

ENABLE_ANTICACHE=false

SITE_HOST="modis"

CLIENT_HOSTS=( \
    "modis" \
    "modis2" \
    "modis2" \
    "modis2" \
)

BASE_CLIENT_THREADS=15
BASE_SITE_MEMORY=2048
BASE_SITE_MEMORY_PER_PARTITION=1024
BASE_PROJECT="voter"
BASE_DIR=`pwd`

BASE_ARGS=( \
    "-Dsite.status_enable=false" \
    
    # SITE DEBUG
#     "-Dsite.status_enable=true" \
#     "-Dsite.status_interval=10000" \
#     "-Dsite.status_show_executor_info=true" \
#     "-Dsite.exec_profiling=true" \
#     "-Dsite.status_show_txn_info=true" \
#     "-Dsite.network_profiling=false" \
#     "-Dsite.log_backup=true"\
    
    # Site Params
    "-Dsite.cpu_affinity_one_partition_per_core=true" \
    "-Dsite.pool_localtxnstate_idle=4000" \
    "-Dsite.specexec_enable=false" \
    "-Dsite.queue_incoming_max_per_partition=2500" \
    "-Dsite.queue_incoming_increase_max=2000" \
    
    # Client Params
    "-Dclient.scalefactor=1" \
    "-Dclient.memory=2048" \
    "-Dclient.txnrate=2000" \
    "-Dclient.warmup=30000" \
    "-Dclient.duration=300000 "\
    "-Dclient.shared_connection=false" \
    "-Dclient.blocking=false" \
    "-Dclient.blocking_concurrent=100" \
    
    # Anti-Caching Experiments
    "-Dsite.anticache_enable=${ENABLE_ANTICACHE}" \
    "-Dsite.anticache_check_interval=99999999" \
    #"-Dclient.interval=500" \
    "-Dclient.anticache_enable=${ENABLE_ANTICACHE}" \
    "-Dclient.anticache_evict_interval=30000" \
    "-Dclient.anticache_evict_size=4194304" \
    "-Dclient.output_csv=false" \
    "-Dclient.output_interval=true" \

    # CLIENT DEBUG
    "-Dclient.profiling=false" \
    "-Dclient.output_response_status=true" \
#     "-Dclient.output_basepartitions=true" \
#     "-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" \
)

EVICTABLE_TABLES=( \
    "votes" \
)
EVICTABLES=""
for t in ${EVICTABLE_TABLES[@]}; do
    EVICTABLES="${t},${EVICTABLES}"
done



# ant compile
# for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do
for i in `seq 1 4`; do

    HSTORE_HOSTS="${SITE_HOST}:0:0-"`expr $i - 1`
    NUM_CLIENTS=`expr $i \* $BASE_CLIENT_THREADS`
    SITE_MEMORY=`expr $BASE_SITE_MEMORY + \( $i \* $BASE_SITE_MEMORY_PER_PARTITION \)`
    
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
        -Dkillonzero=true \
        -Dclient.threads_per_host=${NUM_CLIENTS} \
        -Dsite.memory=${SITE_MEMORY} \
        -Dclient.hosts=${CLIENT_HOSTS_STR} \
        -Dclient.count=${CLIENT_COUNT}
    result=$?
    if [ $result != 0 ]; then
        exit $result
    fi
done
