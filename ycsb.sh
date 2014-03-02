#!/bin/bash
# YCSB MMAP Experiments
 
USAGE="Usage: `basename $0` 
       [-hv]  
       [-a (enable anticache)]  
       [-r (clean and rebuild)] 
       [-m (enable storage_mmap)] 
       [-g (get latency)] 
       [-s (set latency)] 
        "

ENABLE_ANTICACHE=false
REBUILD=false
ENABLE_MMAP=false

NVM_LATENCY=110

# Parse command line options.
while getopts hvarmgs: OPT; do
    case "$OPT" in
        h)
            echo "$USAGE"
            exit 0
            ;;
        v)
            echo "`basename $0` version 0.1"
            exit 0
            ;;
        a)
            ENABLE_ANTICACHE=true
            ;;

        r)
            REBUILD=true
            ;;
        
        m)
            ENABLE_MMAP=true
            ;;

        g)
            # GO TO SDV directory 
            cd /data/devel/sdv-tools/sdv-release 
            # MEASURE LATENCY
            sudo ./ivt_pm_sdv.sh --measure

            cd `readlink -f /home/user/joy/h-store`
            exit 0
            ;;

        s)
            # GO TO SDV directory 
            cd /data/devel/sdv-tools/sdv-release 

            # MEASURE LATENCY
            NVM_LATENCY=$2
            echo "SETTING NVM LATENCY : " $NVM_LATENCY
            
            sudo ./ivt_pm_sdv.sh --enable --pm-latency=$NVM_LATENCY
            
            cd `readlink -f /home/user/joy/h-store`
        
            exit 0
            ;;                             

        \?)
            # getopts issues an error message
            echo "$USAGE" >&2
            exit 1
            ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

echo "---------------------------------------------------------"
echo "ENABLE_ANTICACHE : " $ENABLE_ANTICACHE
echo "REBUILD : " $REBUILD
echo "ENABLE_MMAP : " $ENABLE_MMAP
echo "---------------------------------------------------------"
 
# Access additional arguments as usual through 
# variables $@, $*, $1, $2, etc. or using this loop:
for PARAM; do
    echo $PARAM
done 

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    pkill -f hstore.tag
    exit $exit_status
}

# ---------------------------------------------------------------------

SITE_HOST="10.212.84.152"

CLIENT_HOSTS=( \
    "client1" \
    "client2" \
    "10.212.84.152" \     
    "10.212.84.152" \
)

BASE_CLIENT_THREADS=1
#BASE_SITE_MEMORY=8192
#BASE_SITE_MEMORY_PER_PARTITION=750
BASE_SITE_MEMORY=8192
BASE_SITE_MEMORY_PER_PARTITION=750
BASE_PROJECT="ycsb"
BASE_DIR=`readlink -f /home/user/joy/h-store`
OUTPUT_DIR="~/data/ycsb/read-heavy/2/80-20"

#ANTICACHE_BLOCK_SIZE=262144
ANTICACHE_BLOCK_SIZE=524288
#ANTICACHE_BLOCK_SIZE=1048576
#ANTICACHE_BLOCK_SIZE=2097152
#ANTICACHE_BLOCK_SIZE=4194304
#ANTICACHE_BLOCK_SIZE=131072
ANTICACHE_THRESHOLD=.5

BASE_ARGS=( \
    # SITE DEBUG
     "-Dsite.status_enable=false" \
     "-Dsite.status_interval=10000" \
#    "-Dsite.status_exec_info=true" \
#    "-Dsite.status_check_for_zombies=true" \
#    "-Dsite.exec_profiling=true" \
#     "-Dsite.profiling=true" \
# "-Dsite.txn_counters=true" \
#    "-Dsite.pool_profiling=true" \
#     "-Dsite.network_profiling=false" \
#     "-Dsite.log_backup=true"\
#    "-Dnoshutdown=true" \    

    # Site Params
    "-Dsite.jvm_asserts=false" \
    "-Dsite.specexec_enable=false" \
    "-Dsite.cpu_affinity_one_partition_per_core=true" \
    #"-Dsite.cpu_partition_blacklist=0,2,4,6,8,10,12,14,16,18" \
    #"-Dsite.cpu_utility_blacklist=0,2,4,6,8,10,12,14,16,18" \
    "-Dsite.network_incoming_limit_txns=10000" \
    "-Dsite.commandlog_enable=false" \
    "-Dsite.txn_incoming_delay=5" \
    "-Dsite.exec_postprocessing_threads=false" \
    "-Dsite.anticache_profiling=false" \
    "-Dsite.anticache_eviction_distribution=even" \
    
#    "-Dsite.queue_allow_decrease=true" \
#    "-Dsite.queue_allow_increase=true" \
#    "-Dsite.queue_threshold_factor=0.5" \
    
    # Client Params
    "-Dclient.scalefactor=1" \
    "-Dclient.memory=2048" \
    "-Dclient.txnrate=50000" \
    "-Dclient.warmup=120000" \
    "-Dclient.duration=120000" \
    "-Dclient.interval=20000" \
    "-Dclient.shared_connection=false" \
    "-Dclient.blocking=false" \
    "-Dclient.blocking_concurrent=100" \
    "-Dclient.throttle_backoff=100" \
    "-Dclient.output_interval=10000" \
#    "-Dclient.output_anticache_evictions=evictions.csv" \
#    "-Dclient.output_memory=memory.csv" \
    "-Dclient.threads_per_host=4" \

    # Anti-Caching Experiments
    "-Dsite.anticache_enable=${ENABLE_ANTICACHE}" \
    "-Dsite.anticache_profiling=false" \
    "-Dsite.anticache_reset=true" \
    "-Dsite.anticache_block_size=${ANTICACHE_BLOCK_SIZE}" \
    "-Dsite.anticache_check_interval=2000" \
    "-Dsite.anticache_threshold_mb=500" \
    "-Dsite.anticache_blocks_per_eviction=1000" \
    "-Dsite.anticache_max_evicted_blocks=1000" \
#    "-Dsite.anticache_evict_size=${ANTICACHE_EVICT_SIZE}" \
    "-Dsite.anticache_threshold=${ANTICACHE_THRESHOLD}" \
    "-Dclient.anticache_enable=false" \
    "-Dclient.anticache_evict_interval=10000" \
    "-Dclient.anticache_evict_size=102400" \
    "-Dclient.output_csv=results.csv" \

    # MMAP Experiments
    "-Dsite.storage_mmap=${ENABLE_MMAP}" \
    "-Dsite.storage_mmap_dir=\"/mnt/pmfs/mmap/\"" \
    "-Dsite.storage_mmap_sync_frequency=1024" \

    # CLIENT DEBUG
#    "-Dclient.output_txn_counters=txncounters.csv" \
    "-Dclient.output_clients=false" \
    "-Dclient.profiling=false" \
    "-Dclient.output_response_status=false" \
#    "-Dclient.output_queue_profiling=${BASE_PROJECT}-queue.csv" \
#     "-Dclient.output_basepartitions=true" \
#     "-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" 
)

EVICTABLE_TABLES=( \
    "USERTABLE" \
)
EVICTABLES=""
if [ "$ENABLE_ANTICACHE" = "true" ]; then
    for t in ${EVICTABLE_TABLES[@]}; do
        EVICTABLES="${t},${EVICTABLES}"
    done
fi

# Compile
HOSTS_TO_UPDATE=("$SITE_HOST")
for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
    NEED_UPDATE=1
    for x in ${HOSTS_TO_UPDATE[@]}; do
        if [ "$CLIENT_HOST" = "$x" ]; then
            NEED_UPDATE=0
            break
        fi
    done
    if [ $NEED_UPDATE = 1 ]; then
        HOSTS_TO_UPDATE+=("$CLIENT_HOST")
    fi
done
for HOST in ${HOSTS_TO_UPDATE[@]}; do
    echo "BASE DIR : " $BASE_DIR
    if [ "$REBUILD" = "false" ]; then
        echo "REUSING BINARIES"
        ssh $HOST "cd $BASE_DIR && ant compile" 
    else
        echo "CLEANING AND REBUILDING BINARIES"
        ssh $HOST "cd $BASE_DIR && git pull && ant compile && ant clean-all && ant build -Dsite.storage_mmap=True" 
    fi
done
wait

if [ "$REBUILD" = "false" ]; then
    echo "REUSING BINARIES"
    ant compile
else
    echo "CLEANING AND REBUILDING BINARIES"
    ant compile
    ant clean-all
    ant build -Dsite.storage_mmap=True
fi

for i in 8; do

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
        CLIENT_COUNT=`expr $CLIENT_COUNT + 1`
        if [ ! -z "$CLIENT_HOSTS_STR" ]; then
            CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR},"
        fi
        CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}${CLIENT_HOST}"
    done
    
    # DISTRIBUTE PROJECT JAR
    for HOST in ${HOSTS_TO_UPDATE[@]}; do
        if [ "$HOST" != $(hostname) ]; then
            scp -r ${BASE_PROJECT}.jar ${HOST}:${BASE_DIR} &
        fi
    done
    wait

    # EXECUTE BENCHMARK
    ant hstore-benchmark ${BASE_ARGS[@]} \
        -Dproject=${BASE_PROJECT} \
        -Dkillonzero=false \
        -Dsite.memory=${SITE_MEMORY} \
        -Dclient.hosts=${CLIENT_HOSTS_STR} \
        -Dclient.count=${CLIENT_COUNT} 
    result=$?
    if [ $result != 0 ]; then
        exit $result
    fi
done
