#!/bin/sh -x

## ---------------------------------------
## General Configuration
## ---------------------------------------
ANT="ant -f contrib-build.xml"
ANT_TARGET="designer-benchmark"
BENCHMARK="tpcc"

BENCHMARK_JAR=$BENCHMARK.jar
WORKLOAD="workloads/tpcc.real.100w.trace.gz" #tpcc.remote50.trace.gz"
#WORKLOAD="workloads/$BENCHMARK.trace.gz"
XACT_LIMIT=30000
STATS="workloads/$BENCHMARK.stats.gz"
#PARTITIONER="edu.brown.designer.partitioners.HeuristicPartitioner"
PARTITIONER="edu.brown.designer.partitioners.BranchAndBoundPartitioner"
THREADS=$1
EXCLUDE="ResetWarehouse"


$ANT $ANT_TARGET \
   -Djar=$BENCHMARK_JAR \
   -Dworkload=$WORKLOAD \
   -Dlimit=$XACT_LIMIT \
   -Dstats=$STATS \
   -Dpartitioner=$PARTITIONER \
   -Dthreads=$THREADS \
   -Dexclude=$EXCLUDE
