import socket
import sys
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
	while True:
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
		if data == "s-store ready":
			sready = True
			print "S-STORE READY!!!"
			#print "repr(data): ", repr(data)
		if hready and sready:
			conn.sendall("READY")
			#sleep 1 sec
			hready = False
			sready = False

	conn.close()


while True:
	conn, addr = s.accept()
	print 'Connected with ' + addr[0] + ':' + str(addr[1])
	start_new_thread(clientthread, (conn,))

s.close()




