#!/bin/sh
ant clean-java build-java
ant hstore-prepare -Dproject=voterwintimehstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwintimesstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinhstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinsstore -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-10000w10s-98-0602-site12.txt" --txnthreshold 0.98 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-10000w10s-98-0602-site12.txt" --txnthreshold 0.98 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-60w5s1000t-98-0602-site12.txt" --txnthreshold 0.98 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-60w5s1000t-98-0602-site12.txt" --txnthreshold 0.98 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-10000w10s-95-0602-site12.txt" --txnthreshold 0.95 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-10000w10s-95-0602-site12.txt" --txnthreshold 0.95 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-60w5s1000t-95-0602-site12.txt" --txnthreshold 0.95 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-60w5s1000t-95-0602-site12.txt" --txnthreshold 0.95 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-10000w10s-90-0602-site12.txt" --txnthreshold 0.90 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-10000w10s-90-0602-site12.txt" --txnthreshold 0.90 -e "experiments/0602/site12-0602.txt" --winconfig "tuple 10000w10s (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-60w5s1000t-90-0602-site12.txt" --txnthreshold 0.90 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-60w5s1000t-90-0602-site12.txt" --txnthreshold 0.90 -e "experiments/0602/site12-0602.txt" --winconfig "time 60w5s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 70000 --numruns 5 
