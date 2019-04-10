from Cluster.ServerClient import *
from Cluster.Scheduler import *
import socketserver


def combine_result(result, output_location):
    with open(output_location, 'w') as out_file:
        for res in result:
            for file_name in res:
                with open(file_name, 'r') as f:
                    for line in f:
                        out_file.write(line)



class Master(socketserver.ThreadingTCPServer):
    """
    The rest are workers that are assigned work
    by the master. There are M map tasks and R reduce
    tasks to assign. The master picks idle workers and
    assigns each one a map task or a reduce task
    """
    def start(self, data_locations, mapper_ids, reducer_ids, output_location):
        self.mappers = mapper_ids[:]
        self.reducers = reducer_ids[:]
        self.mapper_scheduler = Scheduler()
        self.mapper_scheduler.add_all(data_locations)
        self.reducer_scheduler = Scheduler()
        self.mapper_progress = 0
        self.reducer_progress = 0
        self.result = []

        try:
            self.serve_forever()
        except KeyboardInterrupt:
            pass
        combine_result(self.result, output_location)
        self.server_close()

    def is_mapper_done(self):
        return self.mapper_progress == len(self.mappers)

    def is_reducer_done(self):
        return self.reducer_progress == len(self.reducers)


class MasterHandler(socketserver.BaseRequestHandler):
    """
    self.request  #client ,<socket.socket fd=7, family=AddressFamily.AF_INET,
                    type=SocketKind.SOCK_STREAM, proto=0, laddr=('127.0.0.1', 1024),
                    raddr=('127.0.0.1', 51542)>
    self.client_address   # ('127.0.0.1', 51542)
    self.server   # socketserver, <Master.Master.Master object at 0x10394c048>
    """
    def handle(self):
        """
        framework
        :param assign_work:
        :return:
        """
        # print(self.request)
        # print(self.server)
        # print(self.client_address)
        reply_msg = []
        send_msg = []
        while True:
            worker_id, msg = recv_all(self.request)
            # print (">>>>", msg)
            if worker_id in self.server.mappers:
                if msg == WORKER_READY:
                    reply_msg = self.server.mapper_scheduler.get_task()
                    if len(reply_msg) > 0:
                        send_all(self.request, MASTER_READY, "Master")
                    else:
                        send_all(self.request, WORKER_CAN_CLOSE, "Master")
                        break
                elif msg == WORKER_WAIT:
                    print(">>>>", msg)
                    send_all(self.request, reply_msg, "Master")

                elif msg == WORKER_DONE:
                    print(">>>>", msg)
                    # record intermediate results
                    send_all(self.request, MASTER_READY, "Master")
                    _, msg = recv_all(self.request)
                    self.server.reducer_scheduler.append(convert_array(msg))
                    self.server.mapper_progress += 1
                    send_all(self.request, MASTER_DONE, "Master")

            elif worker_id in self.server.reducers:
                # mapper done!
                if self.server.is_mapper_done():
                    if msg == WORKER_READY:
                        send_msg = self.server.reducer_scheduler.get_task()
                        if len(send_msg) > 0:
                            send_all(self.request, MASTER_READY, "Master")
                        else:
                            send_all(self.request, WORKER_CAN_CLOSE, "Master")
                            if self.server.is_reducer_done():
                                print(">>> All Done!!")
                                self.server.shutdown()
                            break

                    elif msg == WORKER_WAIT:
                        print(">>>>", msg)
                        send_all(self.request, send_msg, "Master")

                    elif msg == WORKER_DONE:
                        print(">>>>", msg)
                        send_all(self.request, MASTER_WAIT, "Master")
                        _, msg = recv_all(self.request)
                        self.server.result.append(convert_array(msg))
                        send_all(self.request, MASTER_DONE, "Master")
                        self.server.reducer_progress += 1
                else:
                    send_all(self.request, MASTER_WAIT, "Master")



