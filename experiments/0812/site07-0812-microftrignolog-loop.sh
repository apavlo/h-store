ant clean-all build-all
for i in `seq 1 10`;
do
ant hstore-prepare -Dproject="microexpftriggerstrig${i}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig${i}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnoftriggerstrig${i}" -o "experiments/0812/microexpnoftriggerstrig${i}-1c-95-0812-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site07-0812-ftriggers.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig${i}" -o "experiments/0812/microexpftriggerstrig${i}-1c-90-0812-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site07-0812-ftriggers.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done

for i in `seq 1 10`;
do
python ./tools/autorunexp.py -p "microexpnoftriggerstrig${i}" -o "experiments/0812/microexpnoftriggerstrig${i}-1c-95-0812-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site07-0812-ftriggers-10c.txt" --winconfig "(site07) perc_compare" \
--threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig${i}" -o "experiments/0812/microexpftriggerstrig${i}-1c-90-0812-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site07-0812-ftriggers-10c.txt" --winconfig "(site07) perc_compare" \
--threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done