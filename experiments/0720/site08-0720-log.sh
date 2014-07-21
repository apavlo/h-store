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
ant hstore-prepare -Dproject="voterwinhstore${REP}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterwinsstore${REP}" -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p "voterwinhstore${REP}" -o "experiments/0720/voterwinhstore${REP}-1c-90-0720-site08.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1
python ./tools/autorunexp.py -p "voterwinsstore${REP}" -o "experiments/0720/voterwinsstore${REP}-1c-90-0720-site08.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1
python ./tools/autorunexp.py -p "voterwinhstore${REP}" -o "experiments/0720/voterwinhstore${REP}-1c-90-0720-site08-log.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --log
python ./tools/autorunexp.py -p "voterwinsstore${REP}" -o "experiments/0720/voterwinsstore${REP}-1c-90-0720-site08-log.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --log
python ./tools/autorunexp.py -p "voterwinhstore${REP}" -o "experiments/0720/voterwinhstore${REP}-1c-90-0720-site08-sched.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --sched
python ./tools/autorunexp.py -p "voterwinsstore${REP}" -o "experiments/0720/voterwinsstore${REP}-1c-90-0720-site08-sched.txt" --txnthreshold 0.90 -e "experiments/0720/site08-0720.txt" --winconfig "tuple ${REP} (site08)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --sched
done
done
