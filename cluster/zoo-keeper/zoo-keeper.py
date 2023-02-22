import socket
import threading
from time import sleep

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


IP_address = 'localhost'
Port = 9092
server.connect((IP_address, Port))

con_string = "zoo-keeper::"
server.send(con_string.encode('utf-8'))

   

# Listening to Server and Sending Nickname
def receive(client: socket.socket):
    while True:
        try:
            client.send(f'2'.encode('utf-8'))
            message = client.recv(1024).decode('utf-8')
            if message[0]=='1':
                print('hb')
            elif message[0]=='0':
                print('dead')
            sleep(2)
                # client.send(f'3')
            # print(message)
        except Exception as e:
            # Close Connection When Error
            print("An error occured!",e)
            # client.send(f'3')
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
# for i in range(1,4):
receive_thread = threading.Thread(target=receive,args=(server,))
receive_thread.start()

write_thread = threading.Thread(target=write,args=(server,))
write_thread.start()