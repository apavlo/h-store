import socket
import sys
import time
from thread import *

hready = False
sready = False

HOST = ''
PORT = 9000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

try:
	s.bind((HOST,PORT))
except:
	print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	sys.exit()

print 'Socket bind complete'

s.listen(2)
print 'Socket now listening'

def clientthread(conn):
	data = conn.recv(1024)
	global hready
	global sready
	print "data: ", data
	if not data:
		print "NO DATA"
		return
	if data == "h-store ready":
		hready = True
		print "H-STORE READY!!!"
		while not sready:
			#print "h thread: s ready? ", sready
			time.sleep(0.1)

	elif data == "s-store ready":
		sready = True
		print "S-STORE READY!!!"
		while not hready:
			#print "s thread: h ready? ", hready
			time.sleep(0.1)
		
	else:
		print "ERROR: data unknown - ", data
		return

	print "BOTH READY"
	conn.sendall("READY\n")
	hready = False
	sready = False

	conn.close()

def bothConnected(conn, conn2):
	data = conn.recv(1024)
	data2 = conn2.recv(1024)

	if (data == "h-store ready" and data2 == "s-store ready") or (data2 == "h-store ready" and data == "s-store ready"):
		print "READY"
		conn.sendall("READY\n")
		conn2.sendall("READY\n")
	else:
		print "ERROR: Unexpected message."
	
	conn.close()
	conn2.close()


while True:
	conn, addr = s.accept()
	print 'Connected with ' + addr[0] + ':' + str(addr[1])
	conn2, addr = s.accept()
	print 'Connected with ' + addr[0] + ':' + str(addr[1])
	bothConnected(conn, conn2)
	#start_new_thread(clientthread, (conn,))

s.close()




