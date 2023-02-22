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

# topic_name = str(sys.argv[1])
# opts = sys.argv[2]
# topic_name='test'
# opts = ''

parser = argparse.ArgumentParser()
parser.add_argument('--frombeginning')
parser.add_argument('--topic', help='topic name')

args = parser.parse_args()

topic_name=args.topic
opts=args.frombeginning

IP_address = 'localhost'
Port = 9092
server.connect((IP_address, Port))

con_string = f"consumer:{topic_name}:{opts}"
server.send(con_string.encode('utf-8'))

# Listening to Server and Sending Nickname
def receive(client):
    while True:
        try:
            # Receive Message From Server
            message = client.recv(1024).decode('utf-8')
            print(message)
        except :
            # Close Connection When Error
            print("An error occured!")
            client.close()
            break

# Sending Messages To Server
def write(client):
    while True:
        try:
            message = f'{"client"}: {input("")}'
            # client.send(message.encode('utf-8'))
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