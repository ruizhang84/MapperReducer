from Cluster.ServerClient import *
import json


def mark_output(uid, work_cycle):
    return str(uid) + "-" + str(work_cycle) + ".mapred"


def init_output(uid, work_cycle):
    out_location = mark_output(uid, work_cycle)
    open(out_location, 'w+')
    return out_location


def write_output(out_location, output):
    """
    The output of the Reduce function is appended
    to a final output file for this reduce partition.
    """
    with open(out_location, 'a') as file:
        line = "\t".join(output) + "\n"
        file.write(line)


class Reducer(Socket):
    def __init__(self, uid):
        Socket.__init__(self)
        self.id = uid
        self.work_cycle = 0

    def reduce_start(self, reduce_fn, server_addr=(HOST, HOST_PORT)):
        # init
        self.reduce_fn = reduce_fn
        self.intermediate_data = []
        self.output_results = []

        # receive inputs
        self.handler.connect(server_addr)
        self.handler.setblocking(True)

        try:
            while True:
                send_all(self.handler, WORKER_READY, self.id)
                _, msg = recv_all(self.handler)
                # print (msg)
                if msg == MASTER_WAIT:
                    continue
                if msg == WORKER_CAN_CLOSE:
                    print("Reducer: " + str(self.id) + " work done!")
                    break

                # do work
                send_all(self.handler, WORKER_WAIT, self.id)
                _, msg = recv_all(self.handler)
                self.read(msg)
                self.sort()
                self.reduce()

                # worker done!
                send_all(self.handler, WORKER_DONE, self.id)
                # response work progress
                _, msg = recv_all(self.handler)
                send_all(self.handler, self.output_results, self.id)
                _, msg = recv_all(self.handler)
                # print(msg)
        except:
            print("Reducer: " + str(self.id) + " work done!")

        self.handler.close()


    def read(self, msg):
        """
        When a reduce worker is notified by the master
        about these locations, it uses remote procedure calls
        to read the buffered data from the local disks of the
        map workers
        :return:
        """
        for path in self._read_file(msg):
            with open(path, 'r') as fin:
                data = json.load(fin)
                self.intermediate_data.extend(data)

    def _read_file(self, msg):
        return convert_array(msg)

    def sort(self):
        """
        When a reduce worker has read all intermediate data,
        it sorts it by the intermediate keys
        so that all occurrences of the same key are grouped
        together.
        :return:
        """
        self.intermediate_data.sort()

    def reduce(self):
        """
        The reduce worker iterates over the sorted intermediate
        data and for each unique intermediate key encountered,
        it passes the key and the corresponding
        set of intermediate values to the user’s Reduce function
        :return:
        """
        container_key = ''
        container = []
        out_location = init_output(self.id, self.work_cycle)
        self.output_results.append(out_location)
        for k, v in self.intermediate_data:
            if k != container_key and len(container) > 0:
                output = self.reduce_fn(container_key, container)
                write_output(out_location, output)
                container_key = k
                container.clear()
            container.append(v)
        if len(container) > 0:
            output = self.reduce_fn(container_key, container)
            write_output(out_location, output)
        self.work_cycle += 1



# def merge(k, vs):
#     return k, str(sum(vs))
#
#
# reducer = Reducer(uid="0", reduce_fn=merge)
# for i in range(1):
#     loc = "mapper_0_0_"+str(i)+".JSON"
#     with open(loc, 'r') as f:
#         data = json.load(f)
#         reducer._read_file(data)
#
# reducer.sort()
# reducer.reduce()
# print (reducer.intermediate_data)



