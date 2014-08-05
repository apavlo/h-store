import Queue
import socket
import sys, argparse
import time
from threading import Semaphore
from thread import *

hready = False
sready = False

HOST = ''
HSTORE_PORT = 9001
SSTORE_PORT = 9002
FILE = "votes-random-50000.txt"

h_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
h_lock = Semaphore(1)
s_lock = Semaphore(1)
h_votes = Queue.Queue()
s_votes = Queue.Queue()
print 'Socket created'

try:
	h_socket.bind((HOST,HSTORE_PORT))
	s_socket.bind((HOST,SSTORE_PORT))
except:
	print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	sys.exit()

print 'Socket bind complete'

h_socket.listen(10)
s_socket.listen(10)
print 'Socket now listening'

def clientthread(conn):
	global lock
	global votes
	while True:
		data = conn.recv(1024)
		lock.acquire()
		conn.sendall(votes.get())
		lock.release()
	
	conn.close()

def getvotes(filename):
	f = open(filename, 'r')
	global h_votes
	global s_votes
	for line in f:
		h_votes.put(line)
		s_votes.put(line)
	f.close()

def popvotes(conn, votes, lock):
	global waittime
	while True:
		data = conn.recv(1024)
		lock.acquire()
		conn.sendall(votes.get())
		time.sleep(waittime)
		lock.release()
	
	conn.close()

def hthread():
	global h_votes
	global h_socket
	while True:
		conn, addr = h_socket.accept()
		print 'H-Store Votes connected with ' + addr[0] + ':' + str(addr[1])
		start_new_thread(popvotes, (conn,h_votes,h_lock))
	h_socket.close()

def sthread():
	global s_votes
	global s_socket
	while True:
		conn, addr = s_socket.accept()
		print 'S-Store Votes connected with ' + addr[0] + ':' + str(addr[1])
		start_new_thread(popvotes, (conn,s_votes,s_lock))
	s_socket.close()

parser = argparse.ArgumentParser(description='Starts running the vote feeder for h-store and/or s-store.')
parser.add_argument('-w','--wait', help='wait in between sending next vote (in seconds)', default=0.001)

args = parser.parse_args()

waittime = args.wait

getvotes(FILE)
start_new_thread(hthread, ())
start_new_thread(sthread, ())

while True:
	time.sleep(1)




