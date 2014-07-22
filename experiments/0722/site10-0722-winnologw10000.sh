#!/bin/bash
ant clean-all build-all

BENCH=("winhstore" "winhstorenocleanup" "winhstorenostate" "winsstore")
NEWW=("10000")
NEWS=("5" "10" "100")
for w in "${NEWW[@]}"
do
for s in "${NEWS[@]}"
do
REP="w${w}s${s}"
ant hstore-prepare -Dproject="voterwinhstore${REP}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterwinsstore${REP}" -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p "voterwinhstore${REP}" -o "experiments/0722/voterwinhstore${REP}-1c-90-0722-site08.txt" --txnthreshold 0.90 -e "experiments/0722/site08-0722-winnolog10000w.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1
python ./tools/autorunexp.py -p "voterwinsstore${REP}" -o "experiments/0722/voterwinsstore${REP}-1c-90-0722-site08.txt" --txnthreshold 0.90 -e "experiments/0722/site08-0722-winnolog10000w.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1

done
done
