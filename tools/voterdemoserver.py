import socket
import sys
import time
import Queue
from threading import Semaphore 
from thread import *

hready = False
sready = False

HOST = ''
PORT = 9000
HSTORE_PORT = 9001
SSTORE_PORT = 9002
FILE = "../demo/demo-votes.txt"

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
h_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
h_lock = Semaphore(1)
s_lock = Semaphore(1)
h_votes = Queue.Queue()
s_votes = Queue.Queue()
print 'Socket created'

try:
	s.bind((HOST,PORT))
	h_socket.bind((HOST,HSTORE_PORT))
	s_socket.bind((HOST,SSTORE_PORT))
except:
	print 'Bind failed.'
	sys.exit()

print 'Socket bind complete'

s.listen(2)
h_socket.listen(10)
s_socket.listen(10)
print 'Socket now listening'

def getvotes(filename):
	f = open(filename, 'r')
	global h_votes
	global s_votes
	for line in f:
		h_votes.put(line)
		s_votes.put(line)
	f.close()

def popvotes(conn, votes, lock):
	while True:
		data = conn.recv(1024)
		lock.acquire()
		conn.sendall(votes.get())
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

def bothConnected(conn, conn2):
	
	while True:
		data = conn.recv(1024)
		data2 = conn2.recv(1024)

		if (data == "h-store ready" and data2 == "s-store ready") or (data2 == "h-store ready" and data == "s-store ready"):
			print "READY"
			conn.sendall("READY\n")
			conn2.sendall("READY\n")
		else:
			print "ERROR: Unexpected message."
	
	

getvotes(FILE)
start_new_thread(hthread, ())
start_new_thread(sthread, ())

conn, addr = s.accept()
print 'Connected with ' + addr[0] + ':' + str(addr[1])
conn2, addr = s.accept()
print 'Connected with ' + addr[0] + ':' + str(addr[1])
bothConnected(conn, conn2)
conn.close()
conn2.close()
#start_new_thread(clientthread, (conn,))

s.close()




