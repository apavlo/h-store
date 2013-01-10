#!/bin/bash

## ---------------------------------------
## General Configuration
## ---------------------------------------
ANT="ant"

COMMANDS_FILE="commands.txt"
if [ -f $COMMANDS_FILE ]; then 
   rm $COMMANDS_FILE
fi
HOSTS_FILE="hosts.txt"
if [ -f $HOSTS_FILE ]; then 
   rm $HOSTS_FILE
fi

BENCHMARK="tpcc"
START_HOST_IDX=01
NUM_HOSTS=10
SERVERS_PER_HOST=2
START_HSTORE_PORT=33333
NO_COORDINATOR=0
NO_SCRIPT=0
NO_TRACE=0

TRACE_PROCEXCLUDE="LoadWarehouse"

COORDINATOR_HOST="d-19.cs.wisc.edu"
COORDINATOR_SLEEP=30
COORDINATOR_MONITOR=15

## ---------------------------------------
## Args
## ---------------------------------------
for arg in $@; do
   if [ "$arg" = "clean" ]; then
      $ANT clean
   ## General Param
   else
      param_key=""
      param_value=""
      for item in `echo $arg | perl -n -e 'print join(" ", split(/=/, $_, 2));'`; do
         if [ -z "$param_key" ]; then
            param_key=$item
         else
            param_value=$item
         fi
      done
      if [ -z "$param_key" -o -z "$param_value" ]; then
         echo "ERROR: Invalid parameter string '$arg'"
         exit 1
      fi
      eval $param_key="$param_value"
   fi
done
if [ ! -f "$BENCHMARK.jar" ]; then
   echo "ERRROR: The benchmark file '$BENCHMARK.jar' does not exist"
   exit 1
fi

HOSTS=""
host_ctr=0
partition_ctr=0
add=""
while [ $host_ctr -lt $NUM_HOSTS ]; do
    hstore_port=$START_HSTORE_PORT
    host_idx=`expr $START_HOST_IDX + $host_ctr`
   
    server_ctr=0
    while [ $server_ctr -lt $SERVERS_PER_HOST ]; do
        dtxn_port=`expr $hstore_port + 1`
    
        hostname=`printf "d-%02d.cs.wisc.edu" $host_idx`
        host=`printf "%s:%d:%d" $hostname $hstore_port $partition_ctr`
        HOSTS="$HOSTS$add$host"
        add=","
   
        ## Write out the command to fire it up
        kill_java=""
        if [ $server_ctr = 0 ]; then
            kill_java="killall java &>/dev/null ; killall protodtxnengine &>/dev/null ; "
        fi
        cmd="cd "`pwd `" ; $kill_java "
        if [ $NO_SCRIPT != 1 ]; then
            cmd="$cmd script -f -c \\\""
        fi
        cmd="$cmd ant hstore-node -Djar=$BENCHMARK.jar "
        cmd="$cmd -Dpartition=$partition_ctr "
        cmd="$cmd -Dport=$dtxn_port " 
#         cmd="$cmd -Dmarkov=./files/markovs/${BENCHMARK}.markovs "
        cmd="$cmd -Dthreshold=./files/thresholds/default.threshold "
        cmd="$cmd -Dcorrelations=./files/correlations/${BENCHMARK}.correlations"
        if [ $NO_TRACE != 1 ]; then
            cmd="$cmd -Dtrace=./files/workloads/${BENCHMARK}.trace"
            cmd="$cmd -Dignore=$TRACE_PROCEXCLUDE"
        fi
        if [ $NO_SCRIPT != 1 ]; then
            cmd="$cmd \\\" ./output/$host.txt "
        fi
        echo $cmd >> $COMMANDS_FILE
        
        ## Write out the hostname to the hosts file so that we can use pusher
        echo $hostname >> $HOSTS_FILE
        
        hstore_port=`expr $hstore_port + 11111`
        server_ctr=`expr $server_ctr + 1`
        partition_ctr=`expr $partition_ctr + 1`
    done
    host_ctr=`expr $host_ctr + 1`
done
echo "HOSTS: $HOSTS"

## Create catalog 
if [ -f "$BENCHMARK.jar-ORIG" ]; then
   mv $BENCHMARK.jar-ORIG $BENCHMARK.jar
fi
$ANT catalog-fix \
   -Djar=$BENCHMARK.jar \
   -Dtype=$BENCHMARK \
   -Dhosts=$HOSTS \
   -Dcores=$SERVERS_PER_HOST \
   -Dthreads=1

## Create the hstore.conf file
$ANT hstore-conf -Djar=$BENCHMARK.jar

## Coordinator
if [ $NO_COORDINATOR = 0 ]; then
    cmd="cd "`pwd`" ; "
    cmd="$cmd killall java &>/dev/null ; killall protodtxncoordinator &>/dev/null ; "
    cmd="$cmd sleep $COORDINATOR_SLEEP ; "
    if [ $NO_SCRIPT != 1 ]; then
        cmd="$cmd script -f -c \\\""
    fi
    cmd="$cmd ant hstore-coordinator -Djar=$BENCHMARK.jar "
    cmd="$cmd -Dport=12347 "
    cmd="$cmd -Dstatusinterval=$COORDINATOR_MONITOR "
    # cmd="$cmd -Dmarkov=./files/markovs/${BENCHMARK}.markovs "
    cmd="$cmd -Dthreshold=./files/thresholds/default.threshold "
    cmd="$cmd -Dcorrelations=./files/correlations/${BENCHMARK}.correlations "
    if [ $NO_TRACE != 1 ]; then
        cmd="$cmd -Dtrace=./files/workloads/${BENCHMARK}.trace"
        cmd="$cmd -Dignore=$TRACE_PROCEXCLUDE"
    fi
    if [ $NO_SCRIPT != 1 ]; then
        cmd="$cmd \\\" ./output/coordinator.txt "
    fi
    echo $cmd >> $COMMANDS_FILE
    echo $COORDINATOR_HOST >> $HOSTS_FILE
fi

## Done
echo
echo "COMMANDS_FILE: $COMMANDS_FILE"
echo "HOSTS_FILE: $HOSTS_FILE"

exit