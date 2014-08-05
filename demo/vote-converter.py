import Queue
import socket
import sys, argparse
import time
from threading import Semaphore
from thread import *

votes = Queue.Queue()
results = []
numvotes = 0
batchId = 1000

def getvotes(filename, out):
	f = open(filename, 'r')
	global votes
	global batchId
	for line in f:
		line.replace("\n", "")
		line+=' ' + `batchId` + '\n'
		out.write(line)
		batchId+=1
	f.close()

parser = argparse.ArgumentParser(description='This script will automatically find the correct vote sequence.')
parser.add_argument('-f','--file', help='filename', default='votes-random-50000.txt')
parser.add_argument('-o','--out', help='output filename', default='vote-output.txt')

args = parser.parse_args()
FILE = args.file
OUTFILE = args.out

out = open(OUTFILE, 'w')
getvotes(FILE, out)
out.close()
