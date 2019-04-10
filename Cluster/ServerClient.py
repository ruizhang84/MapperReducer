import socket

BUF_SIZE = 1024
HOST_PORT = 1026
WORKER_READY = "Ready"
WORKER_DONE = "Done"
WORKER_WAIT = "Pending"
MASTER_READY = "MasterReady"
MASTER_DONE = "MasterDone"
MASTER_WAIT = "MasterPending"
WORKER_CAN_CLOSE = "OK"
HOST = "localhost"


def pack_msg(msg, uid):
    return str(uid) + "\\" + str(len(str(msg))) + "\\" + str(msg)


def unpack_msg(msg):
    size = uid = ""
    for i in range(len(msg)):
        if msg[i] == "\\":
            uid = msg[:i]
            msg = msg[i+1:]
            break
    for i in range(len(msg)):
        if msg[i] == "\\":
            size = msg[:i]
            msg = msg[i+1:]
            break
    return uid, int(size), msg


def convert_array(input_files):
    array_str = input_files[1:-1]
    return [eval(s) for s in array_str.split(",")]


def recv_all(handler, buf_size=BUF_SIZE):
    msg = handler.recv(buf_size).decode()
    # print (msg)
    uid, size, msg = unpack_msg(msg)
    while len(msg) < size:
        msg += handler.recv(buf_size).decode()
    return uid, msg


def send_all(handler, data, uid="UNKNOWN"):
    msg = pack_msg(data, uid)
    handler.sendall(msg.encode())


class Socket:
    def __init__(self):
        self.handler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.handler.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# class Server(Socket):
#     """
#     The mapper and reducer will be as a client to connect master
#     """
#     def __init__(self):
#         Socket.__init__(self)
#
#     def test(self, server_addr=("localhost", 4090)):
#         self.handler.bind(server_addr)
#         self.handler.listen(5)
#         while True:
#             client, client_addr = self.handler.accept()
#             uid, data = recv_all(client)
#             # print (uid, data)
#             send_all(client, data)
#         # self.handler.close()
#
#
# class Client(Socket):
#     """
#     The master to listen mapper and reducer
#     """
#     def ___init__(self):
#         Socket.__init__(self)
#
#     def test(self, client_addr=("localhost", 4090), msg=str([(1, 2), (3, 4), (5, 6)])):
#         self.handler.connect(client_addr)
#         send_all(self.handler, msg)
#         data = recv_all(self.handler)
#         print(data)


# server = Server()
# Server.test()
# client = Client()
# client.test()

