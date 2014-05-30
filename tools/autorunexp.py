import sys, argparse
import os, datetime
import re
import math
from array import *

def generateReport(benchmark_result):
	anlyze_result = []

	#clearing to get pure json snippet
	benchmark_result = benchmark_result.replace("  [java] "," ")
	strbegin = "<json>"
	strend = "</json>"
	output  = re.compile('<json>(.*?)</json>', re.DOTALL |  re.IGNORECASE).findall(benchmark_result)

	try:
		jsonsnippet = str(output[0])
		jsonsnippet = jsonsnippet.replace("\n","")
		jsonsnippet = jsonsnippet.replace("\"","")
		
		# get 	THROUGHPUT
		output  = re.compile('TXNTOTALPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		THROUGHPUT = str(output[0])
		basic = " " + THROUGHPUT
		anlyze_result.append(THROUGHPUT)
		# get 	AVG LATENCY
		output  = re.compile('TOTALAVGLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		AVGLATENCY = str(output[0])
		basic += " " + AVGLATENCY
		anlyze_result.append(AVGLATENCY)
		
		# total transaction count
		output  = re.compile('TXNTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNTOTALCOUNT = str(output[0])
		basic += " " + TXNTOTALCOUNT
		anlyze_result.append(TXNTOTALCOUNT)
		# DTXNTOTALCOUNT
		output  = re.compile('DTXNTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		DTXNTOTALCOUNT = str(output[0])
		basic += " " + DTXNTOTALCOUNT
		anlyze_result.append(DTXNTOTALCOUNT)
		# SPECEXECTOTALCOUNT
		output  = re.compile('SPECEXECTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		SPECEXECTOTALCOUNT = str(output[0])
		basic += " " + SPECEXECTOTALCOUNT
		anlyze_result.append(SPECEXECTOTALCOUNT)

		# TXNMINPERSECOND 
		output  = re.compile('TXNMINPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNMINPERSECOND = str(output[0])
		basic += " " + TXNMINPERSECOND
		anlyze_result.append(TXNMINPERSECOND)
		# TXNMAXPERSECOND
		output  = re.compile('TXNMAXPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNMAXPERSECOND = str(output[0])
		basic += " " + TXNMAXPERSECOND
		anlyze_result.append(TXNMAXPERSECOND)
		# STDDEVTXNPERSECOND
		output  = re.compile('STDDEVTXNPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		STDDEVTXNPERSECOND = str(output[0])
		basic += " " + STDDEVTXNPERSECOND
		anlyze_result.append(STDDEVTXNPERSECOND)
		
		# TOTALMINLATENCY
		output  = re.compile('TOTALMINLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALMINLATENCY = str(output[0])
		basic += " " + TOTALMINLATENCY
		anlyze_result.append(TOTALMINLATENCY)
		# TOTALMAXLATENCY
		output  = re.compile('TOTALMAXLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALMAXLATENCY = str(output[0])
		basic += " " + TOTALMAXLATENCY
		anlyze_result.append(TOTALMAXLATENCY)
		# TOTALSTDEVLATENCY
		output  = re.compile('TOTALSTDEVLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALSTDEVLATENCY = str(output[0])
		basic += " " + TOTALSTDEVLATENCY
		anlyze_result.append(TOTALSTDEVLATENCY)

		print basic

	except:
		print "Report Failed"
	
	
	#analyze the content
	#print "This benchmark result is :", jsonsnippet
	return anlyze_result
##enddef

def getReportFromList(list):
	report = ""
	for item in list:
		report += " " + item
	return report
##enddef

def getNumReportFromList(list):
	report = []
	for i in range(0,len(list)):
		report.append(float(list[i]))
	return report
##enddef

# get mean, std for array
def getMeanAndStd(a):
	n = len(a)
	mean = sum(a) / n
	std = math.sqrt(sum((x-mean)**2 for x in a) / n) 
	return mean, std
##enddef

# get the args from command line
# set default values for parameters needed by script
timestamp = datetime.datetime.now()
t = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second
defaultoutput = 'experiment' + '_'.join(str(i) for i in t)+ '.txt'
# get paramets from command line input

parser = argparse.ArgumentParser(description='This is a benchmark auto-run script, made by hawk.')
parser.add_argument('-p','--project', help='Benchmark name', default='tpcc')
parser.add_argument('-o','--output', help='output file', default=defaultoutput)
parser.add_argument('--stop', help='indicate if the threshold will be used to stop expeiments', action='store_true')
parser.add_argument('--blocking', help='indicate if system will blocking', action='store_true')
parser.add_argument('--log', help='indicate if system will run log', action='store_true')
parser.add_argument('--threads', help='threads per host', type=int, default=1)
parser.add_argument('--txnthreshold', help='percentage difference between txnrate and throughput', type=float, default='0.95')
#parser.add_argument('--tmax', help='max - thread per host', type=int, default=1)
#parser.add_argument('--tstep', help='starting step size - thread per host', type=int, default=5)
parser.add_argument('--rmin', help='min - txnrate', type=int, default=1000)
parser.add_argument('--rmax', help='max - txnrate', type=int, default=100000)
parser.add_argument('--rstep', help='initial step size - txnrate', type=int, default=100)
parser.add_argument('--finalrstep', help='final step size- txnrate', type=int, default=0)
parser.add_argument('--logtimeout', help='log timeout', type=int, default=10)
#parser.add_argument('--lmax', help='max - log timeout', type=int, default=10)
#parser.add_argument('--lstep', help='step - log timeout', type=int, default=10)
parser.add_argument('--warmup', help='warmup - time in ms', type=int, default=10000)
parser.add_argument('--numruns', help='number of experiment runs', type=int, default=3)
parser.add_argument('-e', '--expout', help='file that contains the final experiment results', default='expout.txt')
parser.add_argument('--winconfig', help='description of the window configuration', default='')
parser.add_argument('--debug', help='debug mode, only runs once', action='store_true')

args = parser.parse_args()

projectname = args.project
resultfile  = args.output
stopflag    = args.stop
blockingflag= args.blocking
threads	    = args.threads
txn_threshold = args.txnthreshold
#tmax	    = args.tmax
#tstep       = args.tstep
rmin	    = args.rmin
rmax	    = args.rmax
rstep       = args.rstep
frstep      = args.finalrstep
logtimeout  = args.logtimeout
#lmax	    = args.lmax
#lstep       = args.lstep
llog        = args.log
warmup      = args.warmup
numruns     = args.numruns
expout      = args.expout
winconfig   = args.winconfig
debug       = args.debug

if blockingflag==True:
    strblocking = "true"
else:
    strblocking = "false"
#end if

if llog==True:
    strlogging = "true"
else:
    strlogging = "false"
#end if

print projectname, resultfile, stopflag, blockingflag, llog, threads, rmin, rmax, rstep, logtimeout

#exit(0)

file = open(resultfile, "w")

fieldsarray = ["THROUGHPUT(txn/s)","AVGLATENCY(ms)","TotalTXN", "Distributed","SpecExec","THMIN","THMAX","THSTDDEV","LAMIN","LAMAX","LASTDDEV"];

#print fields
fields = "client.theads_per_host client.txnrate site.commandlog_timeout";
for i in range(0,len(fieldsarray)):
	fields += " " + fieldsarray[i];


#fields = "client.threads_per_host " + "client.txnrate " + "site.commandlog_timeout " + "THROUGHPUT(txn/s) " + "AVGLATENCY(ms) " + "TotalTXN "
#fields += "Distributed " + "SpecExec " + "THMIN " + "THMAX " + "THSTDDEV " + "LAMIN " + "LAMAX " + "LASTDDEV"
file.write(fields + "\n")

idx_throughput = 0;
idx_avglatency = 1;
idx_thmin = 5;
idx_thmax = 6;
idx_txnrate = 11;

max_values = [];


#  make command line to execute benchmark with the indicated configuration

number_need_to_determine = 5
stdev_threshold = 0.03

resultlist =  list()

client_threads_per_host = threads;
for rn in range(0, numruns):
	print "RUN NUMBER: " + "{0:d}".format(rn + 1)
	client_txnrate = rmin
	cur_values = []
	while client_txnrate <= rmax:
		if client_txnrate <= 0:
			client_txnrate += rstep
			continue

		site_commandlog_timeout	= logtimeout
		client_warmup = warmup
	
		print "no logging mechanism executed in system..."
		str_antcmd 			= "ant hstore-benchmark"
		str_project 			= " -Dproject=" + projectname
		str_client_output_results_json  = " -Dclient.output_results_json=" + "true"
		str_client_blocking    	        = " -Dclient.blocking=" + strblocking
		str_client_threads_per_host 	= " -Dclient.threads_per_host=" + "{0:d}".format(client_threads_per_host)
		str_client_txnrate		= " -Dclient.txnrate=" + "{0:d}".format(client_txnrate)
		str_client_warmup       = " -Dclient.warmup=" + "{0:d}".format(client_warmup)
		str_site_commandlog_timeout = " -Dsite.commandlog_timeout=" + "{0:d}".format(site_commandlog_timeout)
		str_site_commandlog_enable = " -Dsite.commandlog_enable=" + strlogging
	
		basic = "{0:d}".format(client_threads_per_host) + " " + "{0:d}".format(client_txnrate) + " " +  "{0:d}".format(site_commandlog_timeout)
	
		runcmd = str_antcmd + str_project + str_client_blocking + str_client_output_results_json + str_client_threads_per_host + str_client_txnrate + str_client_warmup + str_site_commandlog_enable
	
		print "running benchmark with following configuration:"
		print runcmd

		# run the benchmark by calling the runcmd with os
		f = os.popen( runcmd )
	
		# get the benchmark running result, and print it on screen
		result = f.read()
		#print "This benchmark result is :", result
		singlereport = generateReport(result)
		numreport = getNumReportFromList(singlereport)

		basic += getReportFromList(singlereport)
		file.write(basic+"\n")

		resultlist.append(singlereport)
		
		print "client_txnrate * txn_threshold: " + "{0:.2f}".format(client_txnrate * txn_threshold)
		print "throughput: " + "{0:.2f}".format(numreport[idx_throughput])

		if debug:
			numreport.append(float(client_txnrate))
			cur_values = numreport
			break

		if numreport[idx_throughput] <= client_txnrate * txn_threshold:
			if rstep != frstep:
				client_txnrate -= rstep
				rstep = frstep
			else:
				break
		else:
			numreport.append(float(client_txnrate))
			cur_values = numreport
			client_txnrate += rstep
	##endwhile
	#print "cur_values length: " + "{0:d}".format(len(cur_values))
	max_values.append(cur_values)
##endfor

file.close()

#append to the final experimental results file
expfile = open(expout, "a")
proj = projectname + " - " + winconfig + " (" + "{0:.2f}".format(txn_threshold) + " threshold)"
config = "threads: " + "{0:d}".format(client_threads_per_host) + ", logging: " +  strlogging + ", log timeout: " + "{0:d}".format(site_commandlog_timeout)
config += "\nblocking: " + strblocking + ", warmup: " + "{0:d}".format(client_warmup) + ", threshold: " + "{0:.2f}".format(txn_threshold)
expfile.write(proj + "\n");
expfile.write("--------------------------------------------------\n");
expfile.write(config + "\n");

avg_values = []
max_throughput = 0;
max_txnrate = 0;
min_throughput = 999999999;
min_txnrate = 0;
all_throughputs = []
all_txnrates = []
#print "max_values length: " + "{0:d}".format(len(max_values))
for i in range(0, len(max_values)):
	#print "max_values[i] length: " + "{0:d}".format(len(max_values[i]))
	for j in range(0, len(max_values[i])):
		if i == 0:
			avg_values.append(0.0);
		if j == idx_throughput:
			if max_values[i][j] > max_throughput:
				max_throughput = max_values[i][j]
				max_txnrate = max_values[i][idx_txnrate]
			if max_values[i][j] < min_throughput:
				min_throughput = max_values[i][j]
				min_txnrate = max_values[i][idx_txnrate]
			all_throughputs.append(max_values[i][j])
			all_txnrates.append(max_values[i][idx_txnrate])
		avg_values[j] += max_values[i][j]

expfile.write("THROUGHPUT STATS\n")
for i in range(0, len(all_throughputs)):
	expfile.write("   " + "{0:d}".format(i+1) + ": " + "{0:.2f}".format(all_throughputs[i]) + "\n");

expfile.write("   MIN: " + "{0:.2f}".format(min_throughput) + " (" + "{0:.2f}".format(min_txnrate) + " submitted)\n");
expfile.write("   MAX: " + "{0:.2f}".format(max_throughput) + " (" + "{0:.2f}".format(max_txnrate) + " submitted)\n");
expfile.write("   AVG: " + "{0:.2f}".format(avg_values[idx_throughput]/numruns) + "\n")
expfile.write("   ---   \n")
for i in range(0, len(avg_values) - 1):
	if(i == idx_throughput):
		continue
	expfile.write(fieldsarray[i] + "(avg): " + "{0:.2f}".format(avg_values[i]/numruns) + "\n");

expfile.write("\n");
expfile.close()










##end script
