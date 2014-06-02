#!/bin/sh
ant clean-all build-all
ant hstore-prepare -Dproject=voterwintimehstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwintimesstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinhstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinsstore -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-1000w10s-98-0602-site10.txt" --txnthreshold 0.98 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-1000w10s-98-0602-site10.txt" --txnthreshold 0.98 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-30w5s1000t-98-0602-site10.txt" --txnthreshold 0.98 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-30w5s1000t-98-0602-site10.txt" --txnthreshold 0.98 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-1000w10s-95-0602-site10.txt" --txnthreshold 0.95 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-1000w10s-95-0602-site10.txt" --txnthreshold 0.95 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-30w5s1000t-95-0602-site10.txt" --txnthreshold 0.95 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-30w5s1000t-95-0602-site10.txt" --txnthreshold 0.95 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 

python ./tools/autorunexp.py -p voterwinhstore -o "experiments/0602/voterwinhstore-1c-1000w10s-90-0602-site10.txt" --txnthreshold 0.90 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwinsstore -o "experiments/0602/voterwinsstore-1c-1000w10s-90-0602-site10.txt" --txnthreshold 0.90 -e "experiments/0602/site10-0602.txt" --winconfig "tuple 1000w10s (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimehstore -o "experiments/0602/voterwintimehstore-1c-30w5s1000t-90-0602-site10.txt" --txnthreshold 0.90 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterwintimesstore -o "experiments/0602/voterwintimesstore-1c-30w5s1000t-90-0602-site10.txt" --txnthreshold 0.90 -e "experiments/0602/site10-0602.txt" --winconfig "time 30w5s1000t (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
