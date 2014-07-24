import socket
import sys
import time
from thread import *

hready = False
sready = False

HOST = ''
PORT = 8888

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

try:
	s.bind((HOST,PORT))
except:
	print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	sys.exit()

print 'Socket bind complete'

s.listen(10)
print 'Socket now listening'

def clientthread(conn):
	data = conn.recv(1024)
	global hready
	global sready
	print "data: ", data
	if not data:
		print "NO DATA"
		continue
	if data == "h-store ready":
		hready = True
		print "H-STORE READY!!!"
		while not sready:
			print "h thread: s ready? ", sready
			time.sleep(1)

	elif data == "s-store ready":
		sready = True
		print "S-STORE READY!!!"
		while not hready:
			print "s thread: h ready? ", hready
			time.sleep(1)
		
	else:
		print "ERROR: data unknown - ", data
		return

	print "BOTH READY"
	conn.sendall("READY\n")
	time.sleep(2)
	hready = False
	sready = False

	conn.close()


while True:
	conn, addr = s.accept()
	print 'Connected with ' + addr[0] + ':' + str(addr[1])
	start_new_thread(clientthread, (conn,))

s.close()




