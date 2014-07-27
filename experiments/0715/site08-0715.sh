#!/bin/bash
ant clean-all build-all
BENCH=("winhstore" "winhstorenocleanup" "winhstorenostate" "winsstore")
NEWW=("100" "1000" "10000")
NEWS=("1" "5" "10" "100")

for w in "${NEWW[@]}"
do
for s in "${NEWS[@]}"
do
for d in "${BENCH[@]}"
do
REP="w${w}s${s}"
ant hstore-prepare -Dproject="voter${d}${REP}" -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p "voter${d}${REP}" -o "experiments/0715/voter${d}${REP}-1c-95-0915-site08.txt" --txnthreshold 0.95 -e "experiments/0715/site08-0715.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 
done
done
done
