#!/bin/bash
ant clean-all build-all
BENCH=("winhstore" "winhstorenocleanup" "winhstorenostate" "winsstore")
NEWW=("100" "1000" "10000")
NEWS=("1" "5" "10" "100")
for w in "${NEWW[@]}"
do
for s in "${NEWS[@]}"
do
REP="w${w}s${s}"
ant hstore-prepare -Dproject="voterwinsstore${REP}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterwinhstorenostate${REP}" -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p "voterwinsstore${REP}" -o "experiments/0716/voterwinsstore${REP}-1c-95-0716-site08.txt" --txnthreshold 0.95 -e "experiments/0716/site08-0716.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1
python ./tools/autorunexp.py -p "voterwinhstorenostate${REP}" -o "experiments/0716/voterwinhstorenostate${REP}-1c-95-0716-site08.txt" --txnthreshold 0.95 -e "experiments/0716/site08-0716.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1
done
done
