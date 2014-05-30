#!/bin/sh
ant clean-java build-java
ant hstore-prepare -Dproject=voterwintimehstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwintimesstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinhstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinsstore -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0530/voterwinhstore-1c-1000w1s-98-0530-s12.txt" --txnthreshold 0.98 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0530/voterwinsstore-1c-1000w1s-98-0530-s12.txt" --txnthreshold 0.98 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0530/voterwintimehstore-1c-30w1s1000t-98-0530-s12.txt" --txnthreshold 0.98 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0530/voterwintimesstore-1c-30w1s1000t-98-0530-s12.txt" --txnthreshold 0.98 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0530/voterwinhstore-1c-1000w1s-95-0530-s12.txt" --txnthreshold 0.95 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0530/voterwinsstore-1c-1000w1s-95-0530-s12.txt" --txnthreshold 0.95 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0530/voterwintimehstore-1c-30w1s1000t-95-0530-s12.txt" --txnthreshold 0.95 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0530/voterwintimesstore-1c-30w1s1000t-95-0530-s12.txt" --txnthreshold 0.95 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0530/voterwinhstore-1c-1000w1s-90-0530-s12.txt" --txnthreshold 0.90 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0530/voterwinsstore-1c-1000w1s-90-0530-s12.txt" --txnthreshold 0.90 -e "experiments/0530/expout.txt" --winconfig "tuple 1000w1s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0530/voterwintimehstore-1c-30w1s1000t-90-0530-s12.txt" --txnthreshold 0.90 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0530/voterwintimesstore-1c-30w1s1000t-90-0530-s12.txt" --txnthreshold 0.90 -e "experiments/0530/expout.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
