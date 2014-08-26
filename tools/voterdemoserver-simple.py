import socket
import sys, argparse
import time
import Queue
from threading import Semaphore 
from thread import *

hready = False
sready = False

HOST = ''
PORT = 9510
HSTORE_PORT = 9511
SSTORE_PORT = 9512
FILE = "../demo/demo-votes.txt"

h_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
h_lock = Semaphore(1)
s_lock = Semaphore(1)
votes = []
s_index = 0
h_index = 0
waittime = 0.001
#first_stop = True
print 'Socket created'

try:
	h_socket.bind((HOST,HSTORE_PORT))
	s_socket.bind((HOST,SSTORE_PORT))
except:
	print 'Bind failed.'
	sys.exit()

print 'Socket bind complete'

h_socket.listen(10)
s_socket.listen(10)
print 'Socket now listening'

def getvotes(filename):
	f = open(filename, 'r')
	global votes
	for line in f:
		votes.append(line)
	f.close()

def s_popvotes(conn, lock):
	global waittime
	global s_index
	global votes
	while True:
		data = conn.recv(1024)
		if data == 'closing':
			print "CLOSING"
			break
		lock.acquire()
		conn.sendall(votes[s_index])
		s_index+=1
		time.sleep(waittime)
		lock.release()
	s_index = 0
	conn.close()

def h_popvotes(conn, lock):
	global waittime
	global h_index
	global votes
	while True:
		data = conn.recv(1024)
		if data == 'closing':
			print "CLOSING"
			break
		lock.acquire()
		conn.sendall(votes[h_index])
		h_index+=1
		time.sleep(waittime)
		lock.release()
	h_index = 0
	conn.close()

def hthread():
	global h_socket
	global h_lock
	while True:
		conn, addr = h_socket.accept()
		print 'H-Store Votes connected with ' + addr[0] + ':' + str(addr[1])
		start_new_thread(h_popvotes, (conn,h_lock))
	h_socket.close()

def sthread():
	global s_socket
	global s_lock
	while True:
		conn, addr = s_socket.accept()
		print 'S-Store Votes connected with ' + addr[0] + ':' + str(addr[1])
		start_new_thread(s_popvotes, (conn,s_lock))
	s_socket.close()


parser = argparse.ArgumentParser(description='Starts running the vote feeder for h-store and/or s-store.')
parser.add_argument('-w','--wait', help='wait in between sending next vote (in seconds)', type=float, default=0.001)
parser.add_argument('-f','--file', help='filename to read', default="demo-votes.txt")

args = parser.parse_args()

waittime = args.wait
FILE = args.file
print(FILE)
print(waittime)
getvotes(FILE)
start_new_thread(hthread, ())
sthread()

