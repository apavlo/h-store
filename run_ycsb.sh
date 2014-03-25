#!/usr/bin/zsh
# RUN EXPERIMENTS

DEFAULT_LATENCY=100
LOG_DIR=log
SCRIPT=./experiment.sh 

for ((i=2; i<=8; i*=4))
do
    l=$(($i*$DEFAULT_LATENCY))
    
    echo "---------------------------------------------------"
    echo "LATENCY" $l

    $SCRIPT -s $l &> $LOG_DIR/$i.log

    # RESET SKEW AT START
    cp ./src/benchmarks/edu/brown/benchmark/ycsb/YCSBConstants.java.base ./src/benchmarks/edu/brown/benchmark/ycsb/YCSBConstants.java
    cp ./properties/benchmarks/ycsb.properties.base ./properties/benchmarks/ycsb.properties

    grep "skew_factor =" ./properties/benchmarks/ycsb.properties
    grep "ZIPFIAN_CONSTANT =" ./src/benchmarks/edu/brown/benchmark/ycsb/YCSBConstants.java

    echo "---------------------------------------------------"
    
    for ((s=1; s<=5; s+=1))
    do
        p=$((0.25*$s));
        q=$((0.25*$(($s+1))));
        echo "UPDATING SKEW FROM $p TO $q"
         
        if [[ $s -eq 4 ]]; then
            p=$((1.01));
        fi
         
        if [[ $s -eq 3 ]]; then
            q=$((1.01));
        fi

        sed -i "s/skew_factor = $p/skew_factor = $q/g" ./properties/benchmarks/ycsb.properties
        sed -i "s/ZIPFIAN_CONSTANT = $p/ZIPFIAN_CONSTANT = $q/g" ./src/benchmarks/edu/brown/benchmark/ycsb/YCSBConstants.java

        grep "skew_factor =" ./properties/benchmarks/ycsb.properties
        grep "ZIPFIAN_CONSTANT =" ./src/benchmarks/edu/brown/benchmark/ycsb/YCSBConstants.java

        $SCRIPT -y &>> $LOG_DIR/$i.log

        cp results.csv "$LOG_DIR/""$i""_$s.csv"
    
    done
done
