import sys, argparse
import os, datetime
import re
import math
from array import *

def generateReport(benchmark_result):
	anlyze_result = []

	#clearing to get pure json snippet
	strbegin = "<json>"
	strend = "</json>"
	output  = re.compile('<json>(.*?)</json>', re.DOTALL |  re.IGNORECASE).findall(benchmark_result)
	jsonsnippet = str(output[0])
	jsonsnippet = jsonsnippet.replace("\n","")
	jsonsnippet = jsonsnippet.replace("\"","")

	# get 	AVERAGESIZE
	output  = re.compile('AVERAGESIZE:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGESIZE = str(output[0])
	anlyze_result.append(AVERAGESIZE)
	# get 	MINSIZE
	output  = re.compile('MINSIZE:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINSIZE = str(output[0])
	anlyze_result.append(MINSIZE)
	# get MAXSIZE
	output  = re.compile('MAXSIZE:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXSIZE = str(output[0])
	anlyze_result.append(MAXSIZE)
	# get STDDEVSIZE
	output  = re.compile('STDDEVSIZE:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVSIZE = str(output[0])
	anlyze_result.append(STDDEVSIZE)

	# get AVERAGEBATCHTHROUPUT
	output  = re.compile('AVERAGEBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGEBATCHTHROUPUT = str(output[0])
	anlyze_result.append(AVERAGEBATCHTHROUPUT)
	# get MINBATCHTHROUPUT
	output  = re.compile('MINBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINBATCHTHROUPUT = str(output[0])
	anlyze_result.append(MINBATCHTHROUPUT)
	# get MAXBATCHTHROUPUT
	output  = re.compile('MAXBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXBATCHTHROUPUT = str(output[0])
	anlyze_result.append(MAXBATCHTHROUPUT)
	# get STDDEVBATCHTHROUPUT
	output  = re.compile('STDDEVBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVBATCHTHROUPUT = str(output[0])
	anlyze_result.append(STDDEVBATCHTHROUPUT)

	# get AVERAGECLIENTBATCHTHROUPUT
	output  = re.compile('AVERAGECLIENTBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGECLIENTBATCHTHROUPUT = str(output[0])
	anlyze_result.append(AVERAGECLIENTBATCHTHROUPUT)
	# get MINCLIENTBATCHTHROUPUT
	output  = re.compile('MINCLIENTBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINCLIENTBATCHTHROUPUT = str(output[0])
	anlyze_result.append(MINCLIENTBATCHTHROUPUT)
	# get MAXCLIENTBATCHTHROUPUT
	output  = re.compile('MAXCLIENTBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXCLIENTBATCHTHROUPUT = str(output[0])
	anlyze_result.append(MAXCLIENTBATCHTHROUPUT)
	# get STDDEVCLIENTBATCHTHROUPUT
	output  = re.compile('STDDEVCLIENTBATCHTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVCLIENTBATCHTHROUPUT = str(output[0])
	anlyze_result.append(STDDEVCLIENTBATCHTHROUPUT)
	
        # get AVERAGELATENCY 
	output  = re.compile('AVERAGELATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGELATENCY = str(output[0])
	anlyze_result.append(AVERAGELATENCY)
	# get 
	output  = re.compile('MINLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINLATENCY = str(output[0])
	anlyze_result.append(MINLATENCY)
	# get 
	output  = re.compile('MAXLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXLATENCY = str(output[0])
	anlyze_result.append(MAXLATENCY)
	# get STDDEVLATENCY
	output  = re.compile('STDDEVLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVLATENCY = str(output[0])
	anlyze_result.append(STDDEVLATENCY)

	# get AVERAGECLUSTERLATENCY 
	output  = re.compile('AVERAGECLUSTERLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGECLUSTERLATENCY = str(output[0])
	anlyze_result.append(AVERAGECLUSTERLATENCY)
	# get 
	output  = re.compile('MINCLUSTERLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINCLUSTERLATENCY = str(output[0])
	anlyze_result.append(MINCLUSTERLATENCY)
	# get 
	output  = re.compile('MAXCLUSTERLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXCLUSTERLATENCY = str(output[0])
	anlyze_result.append(MAXCLUSTERLATENCY)
	# get STDDEVCLUSTERLATENCY
	output  = re.compile('STDDEVCLUSTERLATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVCLUSTERLATENCY = str(output[0])
	anlyze_result.append(STDDEVCLUSTERLATENCY)

	# get AVERAGETHROUPUT
	output  = re.compile('AVERAGETHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGETHROUPUT = str(output[0])
	anlyze_result.append(AVERAGETHROUPUT)
	# get MINTHROUPUT
	output  = re.compile('MINTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINTHROUPUT = str(output[0])
	anlyze_result.append(MINTHROUPUT)
	# get MAXTHROUPUT
	output  = re.compile('MAXTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXTHROUPUT = str(output[0])
	anlyze_result.append(MAXTHROUPUT)
	# get STDDEVTHROUPUT
	output  = re.compile('STDDEVTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVTHROUPUT = str(output[0])
	anlyze_result.append(STDDEVTHROUPUT)

	# get AVERAGECLIENTTHROUPUT
	output  = re.compile('AVERAGECLIENTTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGECLIENTTHROUPUT = str(output[0])
	anlyze_result.append(AVERAGECLIENTTHROUPUT)
	# get MINCLIENTTHROUPUT
	output  = re.compile('MINCLIENTTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINCLIENTTHROUPUT = str(output[0])
	anlyze_result.append(MINCLIENTTHROUPUT)
	# get MAXCLIENTTHROUPUT
	output  = re.compile('MAXCLIENTTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXCLIENTTHROUPUT = str(output[0])
	anlyze_result.append(MAXCLIENTTHROUPUT)
	# get STDDEVCLIENTTHROUPUT
	output  = re.compile('STDDEVCLIENTTHROUPUT:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVCLIENTTHROUPUT = str(output[0])
	anlyze_result.append(STDDEVCLIENTTHROUPUT)
	
        # get AVERAGETUPLELATENCY
	output  = re.compile('AVERAGETUPLELATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	AVERAGETUPLELATENCY = str(output[0])
	anlyze_result.append(AVERAGETUPLELATENCY)
	# get MINTUPLELATENCY
	output  = re.compile('MINTUPLELATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MINTUPLELATENCY = str(output[0])
	anlyze_result.append(MINTUPLELATENCY)
	# get MAXTUPLELATENCY
	output  = re.compile('MAXTUPLELATENCY:(.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	MAXTUPLELATENCY = str(output[0])
	anlyze_result.append(MAXTUPLELATENCY)
	# get STDDEVTUPLELATENCY
	output  = re.compile('STDDEVTUPLELATENCY:(.*?)}', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
	STDDEVTUPLELATENCY = str(output[0])
	anlyze_result.append(STDDEVTUPLELATENCY)

	#analyze the content
	return anlyze_result
##enddef

def getReportFromList(list):
	report = ""
	for item in list:
		report += " " + item
	return report
##enddef


# get the args from command line
# set default values for parameters needed by script
timestamp = datetime.datetime.now()
t = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second
defaultoutput = 'inputclient-experiment' + '_'.join(str(i) for i in t)+ '.txt'
defaultinput = 'word.txt'
# get paramets from command line input

parser = argparse.ArgumentParser(description='This is a benchmark auto-run script, made by hawk.')
parser.add_argument('--host', help='host', default='localhost')
parser.add_argument('-p','--project', help='Benchmark name', default='tpcc')
parser.add_argument('-i','--inputfile', help='input file', default=defaultinput)
parser.add_argument('-o','--outputfile', help='output file', default=defaultoutput)
parser.add_argument('--rmin', help='min - rate', type=int, default=1000)
parser.add_argument('--rmax', help='max - rate', type=int, default=1000)
parser.add_argument('--rstep', help='step - rate', type=int, default=1000)
parser.add_argument('--imin', help='min - interval', type=int, default=1000)
parser.add_argument('--imax', help='max - interval', type=int, default=1000)
parser.add_argument('--istep', help='step - interval', type=int, default=1000)
parser.add_argument('--rndmin', help='min - round', type=int, default=10)
parser.add_argument('--rndmax', help='max - round', type=int, default=10)
parser.add_argument('--rndstep', help='step - round', type=int, default=10)

args = parser.parse_args()

projectname = args.project
host        = args.host
inputfile   = args.inputfile
resultfile  = args.outputfile
rmin	    = args.rmin
rmax	    = args.rmax
rstep       = args.rstep
imin	    = args.imin
imax	    = args.imax
istep       = args.istep
rndmin	    = args.rndmin
rndmax	    = args.rndmax
rndstep       = args.rndstep


print host, projectname, inputfile, resultfile, rmin, rmax, rstep, imin, imax, istep, rndmin, rndmax, rndstep

#exit(0)

file = open(resultfile, "w")

#print fields
fields  = "rate " + "interval " + "#round "
fields += "BATCH_SIZE_AVG " + "BATCH_SIZE_MIN " + "BATCH_SIZE_MAX " + "BATCH_SIZE_STDEV "
fields += "BATCH_THROUGHPUT_AVG " + "BATCH_THROUGHPUT_MIN " + "BATCH_THROUGHPUT_MAX " + "BATCH_THROUGHPUT_STDEV "
fields += "CLIENT_BATCH_THROUGHPUT_AVG " + "CLIENT_BATCH_THROUGHPUT_MIN " + "CLIENT_BATCH_THROUGHPUT_MAX " + "CLIENT_BATCH_THROUGHPUT_STDEV "
fields += "BATCH_LATENCY_AVG " + "BATCH_LATENCY_MIN " + "BATCH_LATENCY_MAX " + "BATCH_LATENCY_STDEV "
fields += "BATCH_CLUSTER_LATENCY_AVG " + "BATCH_CLUSTER_LATENCY_MIN " + "BATCH_CLUSTER_LATENCY_MAX " + "BATCH_CLUSTER_LATENCY_STDEV "
fields += "TUPLE_THROUGHPUT_AVG " + "TUPLE_THROUGHPUT_MIN " + "TUPLE_THROUGHPUT_MAX " + "TUPLE_THROUGHPUT_STDEV "
fields += "CLIENT_TUPLE_THROUGHPUT_AVG " + "CLIENT_TUPLE_THROUGHPUT_MIN " + "CLIENT_TUPLE_THROUGHPUT_MAX " + "CLIENT_TUPLE_THROUGHPUT_STDEV "
fields += "TUPLE_LATENCY_AVG " + "TUPLE_LATENCY_MIN " + "TUPLE_LATENCY_MAX " + "TUPLE_LATENCY_STDEV"
file.write(fields + "\n")

#  make command line to execute benchmark with the indicated configuration

resultlist =  list()

batch_round = rndmin;
while batch_round <= rndmax:
	source_rate = rmin
	while source_rate <= rmax:
		batch_interval	= imin
		while batch_interval <= imax:
			str_inputcmd 		 = "./inputclient"
                        str_host                 = " --host " + host
			str_project 		 = " " + projectname
			str_output_results_json  = " --json" + " true" + " --display true"
			str_source_inputfile	 = " --file " + inputfile
			str_source_rate		 = " --rate " + "{0:d}".format(source_rate)
			str_batch_interval	 = " --interval " + "{0:d}".format(batch_interval)
			str_batch_round		 = " --rounds " + "{0:d}".format(batch_round)
		
			runcmd = str_inputcmd + str_host + str_project + str_output_results_json + str_source_rate + str_batch_interval + str_batch_round + str_source_inputfile
		
			print "running inputclient with following configuration:"
			print runcmd
	
			# run the benchmark by calling the runcmd with os
			f = os.popen( runcmd )
		
			# get the benchmark running result, and print it on screen
			result = f.read()
                        f.close()
			#print "This benchmark result is :", resulti
                        basic = "{0:d}".format(source_rate) + " " + "{0:d}".format(batch_interval) + " " + "{0:d}".format(batch_round)
		        singlereport = generateReport(result)
			basic += getReportFromList(singlereport)
			file.write(basic+"\n")
	
			resultlist.append(singlereport)
		
			batch_interval += istep
			
		##endwhile

		source_rate += rstep
	##endwhile

	batch_round += rndstep;	
##endwhile

file.close()
##end script
