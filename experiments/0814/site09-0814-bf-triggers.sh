ant clean-all build-all

for i in `seq 1 10`;
do
ant hstore-prepare -Dproject="microexpbtriggerstrig${i}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnobtriggerstrig${i}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnobtriggerstrig${i}" -o "experiments/0814/microexpnobtriggerstrig${i}-1c-95-0814-site09-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site09-0814-bftriggers.txt" --winconfig "(site09) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpbtriggerstrig${i}" -o "experiments/0814/microexpbtriggerstrig${i}-1c-90-0814-site09-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site09-0814-bftriggers.txt" --winconfig "(site09) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done

for i in `seq 1 10`;
do
ant hstore-prepare -Dproject="microexpftriggerstrig${i}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig${i}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnoftriggerstrig${i}" -o "experiments/0814/microexpnoftriggerstrig${i}-1c-95-0814-site09-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site09-0814-bftriggers.txt" --winconfig "(site09) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig${i}" -o "experiments/0814/microexpftriggerstrig${i}-1c-90-0814-site09-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site09-0814-bftriggers.txt" --winconfig "(site09) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done
