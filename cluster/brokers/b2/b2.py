import socket
import threading
from time import time

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


IP_address = 'localhost'
Port = 9092
server.connect((IP_address, Port))

con_string = "broker:b2:"
server.send(con_string.encode('utf-8'))

def write_to_partition(topic,msg,offset):
    # print('writing to partition')
    with open(f'./topics/{topic}/p{(int(offset)%3)+1}.txt','a') as f:
        f.write(f"{offset},{msg}\n")
    
    # write log to broker.log with timestamp
    with open('./broker.log','a') as f:
        f.write(f"{topic}\t{offset}\t{time()}\n")
    
    
# Listening to Server and Sending Nickname
def receive(client: socket.socket):
    while True:
        try:  
            # Receive Message From Server
            message = client.recv(1024).decode('utf-8')
            if message[0]=='2':
                client.send('2'.encode('utf-8'))
                continue
            print(message)
            topic,msg,offset = message[1:].split(',')
            write_to_partition(topic,msg,offset)
            if message[0]=='1':
                client.send(message[1:].encode('utf-8'))
        except Exception as e:
            # Close Connection When Error
            print("An error occured!",e)
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