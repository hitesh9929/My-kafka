import socket
import threading
import json
import sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# if len(sys.argv) != 3:
# 	print ("Correct usage: script, IP address, port number")
# 	exit()
# IP_address = str(sys.argv[1])
# Port = int(sys.argv[2])


IP_address = 'localhost'
Port = 9092
server.connect((IP_address, Port))

con_string = f"end::"
server.send(con_string.encode('utf-8'))

