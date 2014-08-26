import socket
import sys, argparse
import time
import Queue
from threading import Semaphore 
from thread import *

c_lines = []
v_lines = []
c_index = 0
v_index = 0
CONTESTANTS_INFILE = '../logs/hstorecontestants-full.txt'
CONTESTANTS_OUTFILE = '../logs/hstorecontestants.txt'
VOTES_INFILE = '../logs/demohstoreout-full.txt'
VOTES_OUTFILE = '../logs/demohstorecurrent.txt'

def getlines(filename, lines):
	f = open(filename, 'r')
	for line in f:
		lines.append(line)
	f.close()

def printcontestants(filename):
	f = open(filename, 'w')
	global c_lines
	global c_index
	f.write(c_lines[c_index])
	c_index+=1
	while not c_lines[c_index].startswith('#'):
		f.write(c_lines[c_index])
		c_index+=1

def printvotes(filename):
	f = open(filename, 'w')
	global v_lines
	global v_index
	f.write(v_lines[v_index])
	v_index+=1
	while not v_lines[v_index].startswith('#'):
		f.write(v_lines[v_index])
		v_index+=1

getlines(CONTESTANTS_INFILE, c_lines)
getlines(VOTES_INFILE, v_lines)
print "waiting for S-Store to start"
time.sleep(15)
while v_index < len(v_lines) - 1:
	printcontestants(CONTESTANTS_OUTFILE)
	printvotes(VOTES_OUTFILE)
	time.sleep(1)