ant clean-all build-all

BENCH=("10" "100" "1000" "10000" "100000")

for i in "${BENCH[@]}";
do
b="w${i}s2"

ant hstore-prepare -Dproject="microexpwindows${b}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnowindows${b}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnowindows${b}" -o "experiments/0814/microexpnowindows${b}-1c-95-0814-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site07-window-wexp.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpwindows${b}" -o "experiments/0814/microexpwindows${b}-1c-90-0814-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site07-window-wexp.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done

SLIDE=("1" "2" "5" "10" "30" "100")

for i in "${SLIDE[@]}";
do
b="w100s${i}"

ant hstore-prepare -Dproject="microexpwindows${b}" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnowindows${b}" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnowindows${b}" -o "experiments/0814/microexpnowindows${b}-1c-95-0814-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site07-window-wexp.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpwindows${b}" -o "experiments/0814/microexpwindows${b}-1c-90-0814-site07-perc.txt" \
--txnthreshold 0.95 -e "experiments/0814/site07-window-wexp.txt" --winconfig "(site07) perc_compare" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
done