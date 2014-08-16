#!/bin/bash
ant clean-java build-java
ant hstore-prepare -Dproject=$1 -Dhosts="localhost:0:0"

ant hstore-benchmark -Dproject=$1 -Dclient.threads_per_host=1 -Dglobal.sstore=false -Dglobal.sstore_scheduler=false -Dclient.blocking=false -Dclient.warmup=0 -Dclient.duration=30000 -Dclient.txnrate=$2 $3