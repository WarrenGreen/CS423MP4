import socket
import struct
import cPickle
import threading

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
    if length_data == None:
        return None

    length, = struct.unpack('!I', length_data)

    return recvall(length)

class MessageManager:
    def __init__(self, host, port, slave=False):
        self.slave = slave
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lock = threading.Lock()

        if slave:
            self.socket.connect((host, port))
            print "Slave is connected"
        else:
            self.socket.bind((host, port))

            self.socket.listen(1)
            conn, addr = self.socket.accept()

            print "Master got a connection"
            self.socket = conn

    def write_done(self):
        self.lock.acquire()
        message = {'type': 'done', 'payload': cPickle.dumps(1)}
        _write_data(self.socket, cPickle.dumps(message))
        self.lock.release()

    def write_alert(self, alert_value):
        self.lock.acquire()
        message = {
                'type': 'alert',
                'payload': cPickle.dumps(alert_value)
        }
        _write_data(self.socket, cPickle.dumps(message))
        self.lock.release()

    def write_array_of_jobs(self, jobs):
        self.lock.acquire()
        for job in jobs:
            message = {
                    'type': 'job',
                    'payload': cPickle.dumps(job)
            }
            _write_data(self.socket, cPickle.dumps(message))
        self.lock.release()

    def read_message(self):
        message = _read_data(self.socket)

        if message == None:
            return None

        # print message
        # print
        # print
        # print

        partial = cPickle.loads(message)

        return {
                'type': partial['type'],
                'payload': cPickle.loads(partial['payload'])
        }

    def shutdown(self):
        if not self.slave:
            self.socket.shutdown(socket.SHUT_RDWR)

        self.socket.close()
