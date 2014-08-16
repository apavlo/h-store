#!/bin/bash
ant clean-all build-all

for i in `seq 1 5`;
do
ant hstore-prepare -Dproject="microexproutetrigtrig${i}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoroutetrigtrig${i}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexproutetrigclienttrig${i}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexproutetrigtrig${i}" -o "experiments/0816/microexproutetrigtrig${i}-1c-95-0816-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0816/site07-0816-route-p1.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpnoroutetrigtrig${i}" -o "experiments/0816/microexpnoroutetrigtrig${i}-1c-90-0816-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0816/site07-0816-route-p1.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexproutetrigclienttrig${i}" -o "experiments/0816/microexproutetrigclienttrig${i}-1c-90-0816-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0816/site07-0816-route-p1.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
done

