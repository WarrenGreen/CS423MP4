import socket
import struct
import pickle

def _write_data(socket, data):
    socket.sendall(struct.pack('!I', len(data)))
    socket.send(data)

def _read_data(socket):
    # helper
    def recvall(length):
        buf = ""
        while length:
            newdat = socket.recv(length)
            if not newdat:
                return None

            buf += newdat
            length -= len(newdat)

        return buf

    length_data = recvall(4)
    length, = struct.unpack('!I', length_data)

    return recvall(length)

class TransferManager:
    def __init__(self, host, port, slave=False):
        self.slave = slave
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if slave:
            self.socket.connect((host, port))
            print "Slave is connected"
        else:
            self.socket.bind((host, port))

            self.socket.listen(1)
            conn, addr = self.socket.accept()

            print "Master got a connection"
            self.socket = conn

    def write_array_of_jobs(self, jobs):
        _write_data(self.socket, str(len(jobs)))
        for job in jobs:
            _write_data(self.socket, pickle.dumps(job))

    def read_jobs(self):
        number_of_jobs = int(_read_data(self.socket))

        for i in range(number_of_jobs):
            yield pickle.loads(_read_data(self.socket))

    def shutdown(self):
        if not self.slave:
            self.socket.shutdown(socket.SHUT_RDWR)

        self.socket.close()
