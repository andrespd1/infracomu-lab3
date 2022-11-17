import hashlib
import socket
import threading
from threading import Event, Thread
import datetime
import os
import sys

host = sys.argv[1]
port = 12346
BUFFER_SIZE = 2048  # Send 2048 bytes each time step
clientesListos = 0
# Create the log file name
name_log = str(datetime.datetime.now()).replace(' ', '').replace(':', '-').split('.')[0]
name_log += '-log.txt'
# Create the log file
log_file = open('./Logs/' + name_log, 'w')
print_lock = threading.Lock()

def serverOperation(connection, event, filename, addr):
    # Opening file reading binary
    file = open(filename, "rb")
    # Instantiate the MD5 algorithm for hashing the files
    md5 = hashlib.md5()
    # Receive confirmation of client for transfer initiation with the id of client
    confirmation = connection.recv(BUFFER_SIZE)
    # Deserialize client id
    print("Client", addr, "sent confirmation")
    # Adds 1 to check the number of clients connected
    global clientesListos
    clientesListos += 1
    # Wait for all the clients to be connected
    event.wait()
    package_n = 1
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
        connection.send(bytes_read)
        # Updates MD5 with the bytes_read
        md5.update(bytes_read)
        package_n += 1
    with print_lock:
        print('Client ' + str(addr) + ': received ' + str(package_n) + ' packages.')
        # Calculates the hash of the file sent
        calculated_hash = md5.hexdigest()
        # Send the hash with a separator
        connection.send('<SEP>'.encode())
        connection.send(calculated_hash.encode())
        connection.close()
        print('Client' + str(addr) + ': Hash of the file sent: ' + str(calculated_hash) +'\n-------------------------------------')
    package_n += 1
    log_file.write('Client ' + str(addr) + ' transmission has successfully ended.'+'\n')
    log_file.write('The file sent to Client ' + str(addr) + 'took: ' + str((end_time - start_time)) + ' ms.'+'\n')


def MainServerThread(event, numArchivo, numClientes):

    # The type of file we want to send
    if numArchivo == 1:
        filename = "100.txt"
    else:
        filename = "250.txt"
    filesize = os.path.getsize(filename)
    log_file.write('The file is ({0}) selected for the test and it is: {1}\n'.format(filename, filesize/1000000))
    print('The file selected for the test is: ' + filename)
    # Instantiate a socketΩ
    socketClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Autoconnect to the socket on the port
    socketClient.bind((host, port))
    print('Server is running on host: ' + str(host))
    print('Server is running on the port: ' + str(port))

    # Put the socket into listening mode
    socketClient.listen(5)
    print('Server is waiting for client connections\n-------------------------------------')
    clientesConectados = 0
    while clientesConectados < numClientes:
        connection, addr = socketClient.accept()
        clientesConectados += 1
        # Start a new thread and return its identifier
        serverHandler = Thread(target=serverOperation, args=(connection, event, filename, addr))
        log_file.write('Connected client: ' + str(addr)+'\n')
        serverHandler.start()
        print('Client connected to server, data transfer starting')
    while(clientesListos < numClientes):
        continue
    print('All clients are connected\n-------------------------------------')
    event.set()
    socketClient.close()


if __name__ == '__main__':
    cv = Event()
    numClientes = int(input("Type the number of clients you want to transfer data:\n"))
    os.system('clear')
    numArchivo = int(input("Select the size of the file you want to use:\n 1. 100 MB \n 2. 250 MB\n"))
    print(numArchivo)
    while (numArchivo != 1 and numArchivo != 2):
        os.system('clear')
        print('The option selected is invalid')
        numArchivo = int(input("Select the size of the file you want to use:\n 1. 100 MB \n 2. 250 MB\n"))
    os.system('clear')
    server = Thread(target=MainServerThread, args=(cv, numArchivo, numClientes))
    server.start()
