import hashlib
import socket
import threading
from threading import Event, Thread
import datetime
import os
import csv


host = 'localhost'
port = 12345
BUFFER_SIZE = 0  # Send 2048 bytes each time step
# Create the log file name
name_log = str(datetime.datetime.now()).replace(' ', '').replace(':', '-').split('.')[0]
name_log += '-log.txt'
# Create the log file
log_file = open('./Logs/' + name_log, 'w')
print_lock = threading.Lock()

def serverOperation(filename, clientAddress, serverSocket, id, filesize, numClientes, writer):
    # Opening file reading binary
    file = open(filename, "rb")
    # Instantiate the MD5 algorithm for hashing the files
    md5 = hashlib.md5()
    
    package_n = 0
    start_time = datetime.datetime.now().timestamp()
    end_time = datetime.datetime.now().timestamp()
    while True:
        # Read the bytes from the file
        bytes_read = file.read(BUFFER_SIZE)
        # If bytes_read is null sending is done
        if not bytes_read:
            end_time = datetime.datetime.now().timestamp()
            break
        
        # Sends the byte package to client
        serverSocket.sendto(bytes_read, clientAddress)
        # Updates MD5 with the bytes_read
        md5.update(bytes_read)
        package_n += 1
    package_n += 1
    with print_lock:
        print('Client ' + str(clientAddress) + ': got sent ' + str(package_n) + ' packages with buffer size '+ str(BUFFER_SIZE)+' bytes\n')
        # Calculates the hash of the file sent
        calculated_hash = md5.hexdigest()
        # Send the hash with a separator
        serverSocket.sendto(('<SEP>'+calculated_hash).encode(), clientAddress)

        print('Client' + str(clientAddress) + ': Hash of the file sent: ' + str(calculated_hash) +'\n-------------------------------------')
    
    log_file.write('Client ' + str(clientAddress) + ' transmission has successfully ended.'+'\n')
    log_file.write('Transmission took ' + str(package_n) + ' packages with buffer size '+ str(BUFFER_SIZE)+' bytes\n')
    log_file.write('The file sent to Client ' + str(clientAddress) + 'took: ' + str((end_time - start_time)) + ' ms.'+'\n')
    #header = [numClientes,clientId,clientSocket_server,sentPackages,sendingTime,sentBytes,tasaTransferencia_servidor]
    serverData = [numClientes, int(id.decode()), clientAddress[1],BUFFER_SIZE, package_n, end_time - start_time,filesize/1000000,(filesize/1000000)/(end_time - start_time)]
    writer.writerow(serverData)

def MainServerThread(numArchivo, numClientes, writer):

    # The type of file we want to send
    if numArchivo == 1:
        filename = "100.txt"
    else:
        filename = "250.txt"
    filesize = os.path.getsize(filename)
    log_file.write('The file is ({0}) selected for the test and it is: {1}\n'.format(filename, filesize/1000000))
    print('The file selected for the test is: ' + filename)
    # Instantiate a socketΩ
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Autoconnect to the socket on the port
    serverSocket.bind((host, port))
    print('Server is running on the port: ' + str(port))

    
    print('Server is waiting for client connections\n-------------------------------------')
    clientesConectados = 0
    while clientesConectados < numClientes:
        id, clientAddress = serverSocket.recvfrom(BUFFER_SIZE)
        print('Recevied ping from client', clientAddress)
        clientesConectados += 1
        # Start a new thread and return its identifier
        serverHandler = Thread(target=serverOperation, args=(filename, clientAddress, serverSocket, id, filesize, numClientes, writer))
        log_file.write('Connected client: ' + str(clientAddress)+'\n')
        serverHandler.start()
        print('Client connected to server, data transfer starting')


if __name__ == '__main__':
    os.system('clear')
    numClientes = int(input("Type the number of clients you want to transfer data:\n"))
    os.system('clear')
    numArchivo = int(input("Select the size of the file you want to use:\n 1. 100 MB \n 2. 250 MB\n"))
    os.system('clear')
    BUFFER_SIZE = int(input("Input the buffer size to fragment datagrams: (Bytes)\n"))

    while (numArchivo != 1 and numArchivo != 2):
        os.system('clear')
        print('The option selected is invalid')
        numArchivo = int(input("Select the size of the file you want to use:\n 1. 100 MB \n 2. 250 MB\n"))
    os.system('clear')
    csvFile = open('pruebasDeCarga/pruebaServer.csv', 'a', encoding='UTF8', newline='')
    writer = csv.writer(csvFile)
    #header = ["clientId","clientSocket_server",'bufferSize','sentPackages','sendingTime','sentBytes','tasaTransferencia_servidor']
    writer.writerow([])
    server = Thread(target=MainServerThread, args=( numArchivo, numClientes, writer))
    server.start()
