import socket
import threading
import json
import sys
import argparse

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# if len(sys.argv) != 3:
# 	print ("Correct usage: script, IP address, port number")
# 	exit()
# IP_address = str(sys.argv[1])
# Port = int(sys.argv[2])
parser = argparse.ArgumentParser()
parser.add_argument('--topic', help='topic name')
args = parser.parse_args()
topic_name=args.topic
# topic_name = str(sys.argv[1])
# topic_name='test'

IP_address = 'localhost'
Port = 9092
server.connect((IP_address, Port))

con_string = f"producer:{topic_name}:"
server.send(con_string.encode('utf-8'))

# Listening to Server and Sending Nickname
def receive(client):
    while True:
        try:
            # Receive Message From Server
            message = client.recv(1024).decode('utf-8')
            print(message,1)
        except :
            # Close Connection When Error
            print("An error occured!")
            client.close()
            break

# Sending Messages To Server
def write(client):
    while True:
        try:
            message = f'{input("")}'
            client.send(message.encode('utf-8'))
        except:
            # Close Connection When Error
            print("An error occured!")
            client.close()
            break

# Starting Threads For Listening And Writing
receive_thread = threading.Thread(target=receive,args=(server,))
receive_thread.start()

write_thread = threading.Thread(target=write,args=(server,))
write_thread.start()