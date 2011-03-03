#!/bin/bash -x

NEWORDER_TRACE="neworder.trace"
NUM_PARTITIONS=2
BASE_PARTITION=0
GLOBAL="false"
NUM_TRANSACTIONS=15000
BENCHMARK="tpcc"
DOT_FILE="/tmp/neworder.dot"

if [ $GLOBAL = "true" ]; then
    MARKOV_FILE=files/markovs/$BENCHMARK.${NUM_PARTITIONS}p.global.markovs
else
    MARKOV_FILE=files/markovs/$BENCHMARK.${NUM_PARTITIONS}p.markovs
fi

if [ ! -f $NEWORDER_TRACE ]; then
    gunzip -c ./files/workloads/tpcc.100w.large.trace.gz | ./scripts/traceutil.py --raw --limit=$NUM_TRANSACTIONS markov > neworder.trace
fi

# ant catalog-fix catalog-info \
#     -Dproject=$BENCHMARK \
#     -Dnumhosts=$NUM_PARTITIONS \
#     -Dnumsites=1 \
#     -Dnumpartitions=1 \
#     -Dcorrelations=files/correlations/${BENCHMARK}.correlations || exit
# 
# ant markov \
#     -Dvolt.client.memory=3048 \
#     -Dproject=$BENCHMARK \
#     -Dworkload=$NEWORDER_TRACE \
#     -Dinclude=neworder \
#     -Dglobal=$GLOBAL \
#     -Doutput=$MARKOV_FILE || exit
    
ant markov-graphviz \
    -Dproject=$BENCHMARK \
    -Dmarkov=$MARKOV_FILE \
    -Dprocedure=neworder \
    -Dpartition=$BASE_PARTITION || exit
    
## Correct labels
perl -pi -e 's/getStockInfo01/CheckStock/g' $DOT_FILE
perl -pi -e 's/getWarehouseTaxRate/GetWarehouse/g' $DOT_FILE
perl -pi -e 's/updateStock/UpdateStock/g' $DOT_FILE
perl -pi -e 's/createOrderLine/InsertOrdLine/g' $DOT_FILE
perl -pi -e 's/createOrder/InsertOrder/g' $DOT_FILE