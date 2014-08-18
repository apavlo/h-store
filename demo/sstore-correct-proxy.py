import socket

TCP_IP = '127.0.0.1'
TCP_PORT = 9510
BUFFER_SIZE = 1024
MESSAGE = "s-store ready"



while True:
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, TCP_PORT))
	s.send(MESSAGE)
	data = s.recv(BUFFER_SIZE)
	s.close
