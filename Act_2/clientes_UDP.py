import hashlib
import socket
from threading import Event, Thread
import datetime
import os
import csv

server_ip = '192.168.1.6'
port = 12345
BUFFER_SIZE = 0  # Send 2048 bytes each time step
# Create the log file name
name_log = str(datetime.datetime.now()).replace(' ', '-').replace(':', '-').split('.')[0]
name_log += '-log.txt'
# Create the log file
log_file = open('./Logs/' + name_log, 'w')




# Thread function
def clientOperation(clientSocket, id, numClientes, serverAddress, writer):
    received_hash = ''
    # Instantiate the MD5 algorithm for hashing the files
    md5 = hashlib.md5()
    filename ="Cliente" + str(id) + "-Prueba" + str(numClientes) + ".txt"
    filepath = "./ArchivosRecibidos/Cliente" + str(id) + "-Prueba" + str(numClientes) + '.txt'
    # Create the file that is going to be transferred
    file = open(filepath, "wb")
    # Send ping to server that is ready to receive the file
    clientSocket.sendto(str(id).encode(), serverAddress)
    print('Data transfer initiated for client:', clientSocket.getsockname(),'\n-------------------------------------')
    start_time = datetime.datetime.now().timestamp()
    end_time = datetime.datetime.now().timestamp()
    computedHash = ''
    integrity = False
    package_n = 0
    while True:
        # Read 2048 bytes from the socket (receive)
        bytes_read, addr = clientSocket.recvfrom(BUFFER_SIZE)
        package_n += 1
        # Asks if the separator is in the bytes read
        if '<SEP>'.encode() in bytes_read:
            bytes_with_hash = bytes_read.split("<SEP>".encode())
            file.write(bytes_with_hash[0])
            md5.update(bytes_with_hash[0])
            file.close()
            # Separates the hashcode from the bytes read
            end_time = datetime.datetime.now().timestamp()
            received_hash = bytes_with_hash[1].decode()
            computedHash = md5.hexdigest()
            break
        # Waits for the server to send the bytes
        if not bytes_read:
            continue
        # Update the MD5 with bytes read to complete hash
        md5.update(bytes_read)
        # Write to the file the bytes we just received
        file.write(bytes_read)
    integrity = received_hash == computedHash
    print("[CLIENT {0}]: Received file's calculated hash: {1}".format(clientSocket.getsockname(), computedHash))
    print('[CLIENT {0}]: Hash from server: {1}'.format(clientSocket.getsockname(), received_hash))
    print('[CLIENT {0}]: Integrity of data is:'.format(clientSocket.getsockname()), integrity,'\n-------------------------------------')
    filesize = os.path.getsize(filepath)
    log_file.write('The file is ({0}) selected for the test and it is: {1}\n'.format(filename, filesize/1000000))
    log_file.write('Connected client: ' + str(clientSocket.getsockname()) + '\n')
    log_file.write('Client ' + str(clientSocket.getsockname()) + ' transmission has successfully ended.' + '\n')
    log_file.write('The file received on Client ' + str(clientSocket.getsockname()) + 'took: ' + str((end_time - start_time)) + ' s.' + '\n')
    #header = [numClientes,clientId,clientSocket,success,bytesRecibidos,receivedPackages,receivingTime,tasaTransferencia_cliente]
    clientData = [numClientes, id, clientSocket.getsockname()[1] ,integrity, filesize/1000000, package_n, end_time - start_time, (filesize/1000000)/(end_time - start_time) ]
    writer.writerow(clientData)
    


def MainClientThread(numClientes, writer):
    # Instantiate N Threads for the number of clients selected
    for x in range(1, numClientes + 1):
        # Instantiate a socket
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        serverAddress = (server_ip, port)
        # Create the client
        cliente = Thread(name = x, target = clientOperation, args = (clientSocket, x, numClientes, serverAddress, writer))
        # Starts the client process
        cliente.start()


if __name__ == '__main__':
    # Asks for the number of clients required for the transfer
    os.system('clear')
    numClientes = int(input("Type the number of clients you want to connect to the server:\n"))
    os.system('clear')
    BUFFER_SIZE = int(input("Input the buffer size to fragment datagrams: (Bytes)\n"))
    os.system('clear')
    csvFile = open('pruebasDeCarga/pruebaClientes.csv', 'a', encoding='UTF8', newline='')
    writer = csv.writer(csvFile)
    #header = ["numClientes","clientId","clientSocket","success","bytesRecibidos","receivedPackages","receivingTime","tasaTransferencia_cliente"]
    writer.writerow([])
    # Instantiate a Superior Thread
    client = Thread(target=MainClientThread, args = (numClientes, writer))
    # Starts the thread to MainClientThread function
    client.start()