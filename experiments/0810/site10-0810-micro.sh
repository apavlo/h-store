ant clean-all build-all
ant hstore-prepare -Dproject="microexpnoftriggers" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpftriggers" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnobtriggers" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpbtriggers" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpnobtriggers" -o "experiments/0810/microexpnobtriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpbtriggers" -o "experiments/0810/microexpbtriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 

python ./tools/autorunexp.py -p "microexpnobtriggers" -o "experiments/0810/microexpnobtriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpbtriggers" -o "experiments/0810/microexpbtriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpbtriggers" -o "experiments/0810/microexpbtriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hscheduler --weakrecovery_off --numruns 1 

#h-store
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 

python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hstore --weakrecovery_off --hscheduler --numruns 1 
#python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
#--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 0 --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10 --hstore --weakrecovery_off --hscheduler --numruns 1
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100 --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 1000 --hstore --weakrecovery_off --hscheduler --numruns 1
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10000 --hstore --weakrecovery_off --hscheduler --numruns 1   
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100000 --hstore --weakrecovery_off --hscheduler --numruns 1   



#s-store weak recovery

python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hscheduler --numruns 1 
#python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
#--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 0 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 1000 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10000 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100000 --hscheduler --numruns 1 



#s-store strong recovery
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --hscheduler --weakrecovery_off --numruns 1 
#python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
#--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 0 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 1000 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 10000 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --groupcommit 100000 --hscheduler --weakrecovery_off --numruns 1 

#h-store
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10 --hstore --weakrecovery_off --hscheduler --numruns 1
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 100 --hstore --weakrecovery_off --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 1000 --hstore --weakrecovery_off --hscheduler --numruns 1
python ./tools/autorunexp.py -p "microexpnoftriggers" -o "experiments/0810/microexpnoftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10000 --hstore --weakrecovery_off --hscheduler --numruns 1   

#s-store weak recovery
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 100 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 1000 --hscheduler --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10000 --hscheduler --numruns 1 


#s-store strong recovery
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 100 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 1000 --hscheduler --weakrecovery_off --numruns 1 
python ./tools/autorunexp.py -p "microexpftriggers" -o "experiments/0810/microexpftriggers-1c-90-0810-site10.txt" --txnthreshold 0.95 --perc_compare -e "experiments/0810/site10-0810-microexp.txt" \
--winconfig "" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --log --logtimeout 10000 --hscheduler --weakrecovery_off --numruns 1 
