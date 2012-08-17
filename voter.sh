#!/bin/bash

SITE_HOST="localhost"

CLIENT_HOSTS=("localhost"
)

BASE_CLIENT_THREADS=3
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
#     "-Dsite.network_profiling=true" \
#     "-Dsite.log_backup=true"\

    # Site Params
    "-Dsite.cpu_affinity_one_partition_per_core=true" \
    "-Dsite.exec_preprocessing_threads=false" \
    "-Dsite.exec_preprocessing_threads_count=2" \
    "-Dsite.exec_postprocessing_threads=false" \
    "-Dsite.queue_incoming_max_per_partition=500" \
    "-Dsite.queue_incoming_increase_max=1000" \
#    "-Dsite.exec_command_logging=true" \
#    "-Dsite.exec_command_logging_group_commit_timeout=10" \
    "-Dclient.output_response_status=true" \

    # Client Params
    "-Dclient.scalefactor=1" \
    "-Dclient.memory=4096" \
    "-Dclient.txnrate=2000" \
    "-Dclient.warmup=20000" \
    "-Dclient.duration=120000 "\
    "-Dclient.shared_connection=false" \
    "-Dclient.blocking=false" \
    "-Dclient.blocking_concurrent=100" \
    "-Dclient.throttle_backoff=100" \


    # CLIENT DEBUG
#     "-Dclient.profiling=true" \
#     "-Dclient.output_response_status=true" \
#     "-Dclient.output_basepartitions=true" \
#     "-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" \
)

trap onexit 1 2 3 15

function onexit() {
    local exit_status=${1:-$?}
    pkill -f hstore.tag
    exit $exit_status
}

ant compile
# for i in 1; do
for i in `seq 1 10` ; do

    HSTORE_HOSTS="${SITE_HOST}:0:0-"`expr $i - 1`
    NUM_CLIENTS=`expr $i \* $BASE_CLIENT_THREADS`
#     NUM_CLIENTS=$BASE_CLIENT_THREADS
#     if [ $i -gt 1 ]; then
#     NUM_CLIENTS=`expr \( $i - 1 \) \* $BASE_CLIENT_THREADS`
#     else
#	  NUM_CLIENTS=`expr \( $i + 1 \) \* $BASE_CLIENT_THREADS`
#     fi
    SITE_MEMORY=`expr $BASE_SITE_MEMORY + \( $i \* $BASE_SITE_MEMORY_PER_PARTITION \)`

    ant hstore-prepare -Dproject=${BASE_PROJECT} -Dhosts=${HSTORE_HOSTS}
    test -f ${BASE_PROJECT}.jar || exit -1

    CLIENT_COUNT=0
    CLIENT_HOSTS_STR=""
    for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
	scp -r ${BASE_PROJECT}.jar ${CLIENT_HOST}:${BASE_DIR}
	CLIENT_COUNT=`expr $CLIENT_COUNT + 1`
	if [ ! -z "$CLIENT_HOSTS_STR" ]; then
	    CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR},"
	fi
	CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}${CLIENT_HOST}"
    done

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
