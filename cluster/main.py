import socket
import threading
import sys
import os
import shutil
from time import sleep


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


# if len(sys.argv) != 3:
# 	print ("Correct usage: script, IP address, port number")
# 	exit()

# IP_address = str(sys.argv[1])
# Port = int(sys.argv[2])

IP_address = 'localhost'
Port = 9092

server.bind((IP_address, Port))

server.listen(100)

client_index = 0

list_of_clients = []

zooKeeper = []

#  { "leader": [client,broker_alias], "followers": [[client,broker_alias], [client,broker_alias]] }
brokers = {"leader": None, "followers": []}

# 
broker_alias = { "b1": None, "b2": None, "b3": None}

# { "<topic>": [clients] }
producers = {}

# { "<topic>": [(<client>,<last_read_offset>), ...] }
consumers = {}

# { "topic": <last_write_offset> }
topic_write_offsets = {}

def remove(connection):
	if connection in list_of_clients:
		list_of_clients.remove(connection)


# Sending Messages To All Connected Clients
def broadcast(message, connection):
    for client in list_of_clients:
        if client!=connection:
            client.send(message)

def elect_new_leader():
    # elect new leader
    if len(brokers["followers"]) > 0:
        brokers["leader"] = brokers["followers"][0]
        brokers["followers"].remove(brokers["followers"][0])
    else:
        brokers["leader"] = None

def find_broker_and_remove(broker_name, client):
    if client in brokers["leader"]:
        brokers["leader"] = None
    else:
        for follower in brokers["followers"]:
            if client in follower:
                brokers["followers"].remove(follower)

def emit_from_beginning(client, topic_name):
    final_msg = ['']*(int(topic_write_offsets[topic_name])+1)
    # make the path dynamic
    with open(f'brokers/{brokers["leader"][1]}/topics/{topic_name}/p1.txt') as f1:
        with open(f'brokers/{brokers["leader"][1]}/topics/{topic_name}/p2.txt') as f2:
            with open(f'brokers/{brokers["leader"][1]}/topics/{topic_name}/p3.txt') as f3:
                for line in f1:
                    final_msg[int(line.strip().split(',')[0])] = line.strip().split(',')[1]
                for line in f2:
                    final_msg[int(line.strip().split(',')[0])] = line.strip().split(',')[1]
                for line in f3:
                    final_msg[int(line.strip().split(',')[0])] = line.strip().split(',')[1]
                
                for msg in final_msg:
                    client.send(msg.encode('utf-8'))
                    sleep(0.01)

def initial_cleanup():
    # delete all topics folder inside brokers
    shutil.rmtree(f"brokers/b1/topics", ignore_errors=True)
    shutil.rmtree(f"brokers/b2/topics", ignore_errors=True)
    shutil.rmtree(f"brokers/b3/topics", ignore_errors=True)

    # create topics folder inside brokers
    os.mkdir(f"brokers/b1/topics")
    os.mkdir(f"brokers/b2/topics")
    os.mkdir(f"brokers/b3/topics")


def createTopic(topic):
    # create topic directory in topics directory
    os.mkdir(f"brokers/b1/topics/{topic}")
    os.mkdir(f"brokers/b2/topics/{topic}")
    os.mkdir(f"brokers/b3/topics/{topic}")

    # create partition files in all above folders
    open(f"brokers/b1/topics/{topic}/p1.txt", "w+")
    open(f"brokers/b2/topics/{topic}/p1.txt", "w+")
    open(f"brokers/b3/topics/{topic}/p1.txt", "w+")

    open(f"brokers/b1/topics/{topic}/p2.txt", "w+")
    open(f"brokers/b2/topics/{topic}/p2.txt", "w+")
    open(f"brokers/b3/topics/{topic}/p2.txt", "w+")

    open(f"brokers/b1/topics/{topic}/p3.txt", "w+")
    open(f"brokers/b2/topics/{topic}/p3.txt", "w+")
    open(f"brokers/b3/topics/{topic}/p3.txt", "w+")

    topic_write_offsets[topic] = 0
    consumers[topic] = []
    producers[topic] = []



# Handling Messages From Clients
def handle(client,con_details):
    con_type = con_details[0]
    if con_type == 'zoo-keeper':
        while True:
            try:
                message = client.recv(1024).decode('utf-8')
                if message[0]=='3':
                    pass
                elif message[0]=='2':
                    if brokers["leader"] is None:
                        raise Exception("No leader available")
                    brokers["leader"][0].send('2'.encode('utf-8'))
            except Exception as e:
                print(e)
                break
    elif con_type == 'broker':
        broker_name=con_details[1]
        while True:
            try:
                message = client.recv(1024).decode('utf-8')
                if message=='':
                    broker_alias[broker_name]=None
                    find_broker_and_remove(broker_name,client)
                    # zooKeeper[0].send('0'.encode('utf-8'))
                    # print(1)
                    continue
                if message[0]=='2':
                    zooKeeper[0].send('1'.encode('utf-8'))
                    continue
                # print(message,'b')
                topic_name,msg,offset = message.split(',')
                # if t == 'test':
                # print(consumers)
                to_delete=[]
                for cons in consumers[topic_name]:
                    try:
                        cons[0].send(msg.encode('utf-8'))
                    except Exception as e:
                        to_delete.append(cons)
                
                for cons in to_delete:
                    consumers[topic_name].remove(cons)
            except Exception as e:
                print(e)
                broker_alias[broker_name]=None
                find_broker_and_remove(broker_name,client)
                elect_new_leader()
                zooKeeper[0].send(f'0'.encode('utf-8'))
                # print(2)
                break
            
    elif con_type == 'producer':
        topic_name = con_details[1]
        while True:
            try:
                # Broadcasting Messages
                message = client.recv(1024).decode('utf-8')
                # print(message,'p')                
                brokers["leader"][0].send(f"1{topic_name},{message},{topic_write_offsets[topic_name]}".encode('utf-8'))
                for follower in brokers["followers"]:
                    follower[0].send(f"0{topic_name},{message},{topic_write_offsets[topic_name]}".encode('utf-8'))

                topic_write_offsets[topic_name] += 1
                # broadcast(message.encode('utf-8'),client)
            except:
                # Removing And Closing Clients
                # clients.remove(client)
                client.close()
                break
    elif con_type == 'consumer':
        while True:
            try:
                # Broadcasting Messages
                message = client.recv(1024).decode('utf-8')
                # print(message)
                
                # broadcast(message.encode('utf-8'),client)
            except:
                # Removing And Closing Clients
                # clients.remove(client)
                client.close()
                break
    

# Receiving / Listening Function
def receive():
    
    while True:
        # Accept Connection
        client, address = server.accept()
        list_of_clients.append(client)

        # "<type>:<topic>:<args>"
        con_string = client.recv(1024).decode('utf-8')
        print("Connected with {}".format(con_string))

        con_type, topic, args = con_string.split(':')

        if con_type == 'zoo-keeper':
            zooKeeper.append(client)
        elif con_type == 'broker':
            if brokers["leader"] == None:
                brokers["leader"] = [client,topic]
            else:
                brokers["followers"].append([client,topic])
            broker_alias[topic] = client
        elif con_type == 'producer':
            if topic not in producers:
                createTopic(topic)
            producers[topic].append(client)
        elif con_type == 'consumer':
            if topic not in consumers:
                createTopic(topic)
            if args == '1':
                consumers[topic].append((client,0))
                emit_from_beginning(client, topic)
            else:
                consumers[topic].append((client,topic_write_offsets[topic]))
        elif con_type == 'end':
            sys.exit(0)
            server.close()
            break
        


        # broadcast("{7} joined!".encode('utf-8'), client)
        # client.send('Connected to Server!'.encode('utf-8'))

        # Start Handling Thread For Client
        thread = threading.Thread(target=handle, args=(client,con_string.split(':')))
        thread.start()
    
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

write_thread = threading.Thread(target=write,args=(server,))
write_thread.start()

initial_cleanup()
receive()
