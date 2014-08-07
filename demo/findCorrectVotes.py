import Queue
import socket
import sys
import time
from threading import Semaphore
from thread import *

votes = Queue.Queue()
results = []
contestants = set(range(1,25))
numvotes = 0
phonenumbers

def getvotes(filename):
	f = open(filename, 'r')
	global votes
	for line in f:
		votes.put(line)
	f.close()

parser = argparse.ArgumentParser(description='This script will automatically find the correct vote sequence.')
parser.add_argument('-f','--file', help='filename', default='votes-random-50000.txt')
parser.add_argument('-o','--out', help='output filename', default='vote-output.txt')

args = parser.parse_args()
FILE = args.file
OUTFILE = args.out
getvotes(FILE)
out = open(OUTFILE, 'w')

for vote in votes:
