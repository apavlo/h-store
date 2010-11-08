#!/bin/bash -x

BENCHMARKS=( \
    "tm1"\
    "tpce"\
    "tpcc.100w"\
    "tpcc.50w"\
    "auctionmark"\
)
HSTORE_LOCAL_TIME=1200
MEMORY=3600

tm1_mix="DeleteCallForwarding:2,GetAccessData:35,GetNewDestination:10,GetSubscriberData:35,InsertCallForwarding:2,UpdateLocation:14,UpdateSubscriberData:2"
tpce_mix="BrokerVolume:5,CustomerPosition:13,MarketFeed:1,MarketWatch:18,SecurityDetail:14,TradeLookup:8,TradeOrder:10,TradeResult:10,TradeStatus:19,TradeUpdate:2,DataMaintenance:1,TradeCleanup:1"
tpcc_mix="delivery:4,neworder:45,slev:4,ostatByCustomerId:2,ostatByCustomerName:3,paymentByCustomerIdC:13,paymentByCustomerIdW:13,paymentByCustomerNameC:8,paymentByCustomerNameW:9"
auctionmark_mix="CheckWinningBids:-1,GetItem:35,GetUserInfo:10,GetWatchedItems:10,NewBid:13,NewComment:2,GetComment:2,NewCommentResponse:1,NewFeedback:5,NewItem:10,NewPurchase:2,NewUser:8,PostAuction:-1,UpdateItem:2"

benchmark=$1
if [ "$2" = "1" ]; then
   HSTORE_TOTAL_TIME=( \
   3600\
   7200\
   14400\
   28800\
)
else
   HSTORE_TOTAL_TIME=( 57600 )
fi

workload=$benchmark
if [ "$benchmark" = "tpcc.100w" -o "$benchmark" = "tpcc.50w" ]; then
    benchmark="tpcc"
    txn_mix=$tpcc_mix
elif [ "$benchmark" = "tpce" ]; then
    txn_mix=$tpce_mix
elif [ "$benchmark" = "auctionmark" ]; then
    txn_mix=$auctionmark_mix
elif [ "$benchmark" = "tm1" ]; then
    txn_mix=$tm1_mix
else
    echo "ERROR: Invalid benchmark '$benchmark'"
    exit 1
fi

for total_time in ${HSTORE_TOTAL_TIME[@]}; do
    output=output/typescript-$workload.$total_time
    script -f -c "\
    ant designer-benchmark \
        -Dvolt.client.memory=${MEMORY} \
        -Djar=${benchmark}-designer-benchmark.jar \
        -Dtype=${benchmark} \
        -Dstats=./files/workloads/${benchmark}.stats.gz \
        -Dworkload=./files/workloads/${workload}.trace.gz \
        -Dxactlimit=-1 \
        -Dinclude=${txn_mix} \
        -Dmultiplier=50 \
        -Dpartitioner=edu.brown.designer.partitioners.LNSPartitioner \
        -Dcostmodel=edu.brown.costmodel.TimeIntervalCostModel \
        -Dintervals=10 \
        -Dhints=./files/hints/${benchmark}.hints \
        -Dcheckpoint=./checkpoints/$workload.$total_time.checkpoint \
        -Dcorrelations=./files/correlations/${benchmark}.correlations \
        -Doutput=files/vldb/${workload}.${total_time}.pplan \
        -Dextraparams=\"designer.hints.LIMIT_TOTAL_TIME=${total_time} -designer.hints.LIMIT_LOCAL_TIME=${HSTORE_LOCAL_TIME} designer.hints.LOG_SOLUTIONS_COSTS=$output.costs\"" $output
done
