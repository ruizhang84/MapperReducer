import json
import os
import math
from Cluster.ServerClient import *


def text_input_key_value_generator(input_files):
    """
    default function to parse key/value
    pairs out of the input data
    :param input_files: the file path
    :return: key-value pairs
    """
    pairs = []
    for file_path in convert_array(input_files):
        data = []
        with open(file_path, 'r') as fin:
            for line in fin:
                data.append(line)
        pairs.append((file_path, data))
    return pairs


def written_to_disk(data, dest):
    with open(dest, 'w') as outfile:
        json.dump(data, outfile)


def mark_location(uid, work_cycle, reducer_idx):
    return "mapper_" + "_".join([str(uid), str(work_cycle), str(reducer_idx)]) + '.JSON'


class Mapper(Socket):
    """
    A worker who is assigned a map task reads the contents
    of the corresponding input split.
    """
    def __init__(self, uid):
        Socket.__init__(self)
        self.id = uid
        self.work_cycle = 0

    def map_start(self, num_reducer, map_fn,
                     parse_fn=text_input_key_value_generator, server_addr=(HOST, HOST_PORT)):
        # init
        self.work_cycle = 0
        self.num_reducer = num_reducer
        self.map_fn = map_fn
        self.parse_fn = parse_fn
        self.buffered_pair = []
        self.locations = []

        # receive inputs
        self.handler.connect(server_addr)
        self.handler.setblocking(True)

        try:
            while True:
                # worker ready
                send_all(self.handler, WORKER_READY, self.id)
                _, msg = recv_all(self.handler)
                if msg == WORKER_CAN_CLOSE:
                    print("Mapper: " + str(self.id) + " work done!")
                    self.handler.close()
                    break

                # require input
                send_all(self.handler, WORKER_WAIT, self.id)
                _, msg = recv_all(self.handler)
                # do work
                self.map(msg)
                self.store()

                # work done!
                send_all(self.handler, WORKER_DONE, self.id)
                _, msg = recv_all(self.handler)
                data = str(self.locations)
                send_all(self.handler, data, self.id)
                _, msg = recv_all(self.handler)
                self.locations.clear()
        except:
            print("Mapper: " + str(self.id) + " work done!")
        self.handler.close()

    def map(self, input_split):
        """
        It parses key/value pairs out of the input data
        and passes each pair to the user-defined Map function
        The intermediate key/value pairs produced by the Map function
        are buffered in memory.
        :param input_split:
        :return:
        """
        for k, v in self.parse_fn(input_split):
            self.buffered_pair.extend(self.map_fn(k, v))

    def store(self):
        """
        Periodically, the buffered pairs are written to local
        disk, partitioned into R regions by the partitioning
        function. The locations of these buffered pairs on
        the local disk are passed back to the master, who
        is responsible for forwarding these locations to the
        reduce workers.
        :return:
        """
        self._partition()
        self.buffered_pair.clear()
        self.work_cycle += 1

    def _partition(self):
        # sort key
        self.buffered_pair.sort()
        # split key
        keys = set([k for k, v in self.buffered_pair])
        key_split_size = math.ceil(len(keys)/self.num_reducer)
        # write file
        self._write_split_to_file(key_split_size)

    def _write_split_to_file(self, key_split_size):
        def write_from_container(uid, cycle, idx):
            loc = mark_location(uid, cycle, idx)
            written_to_disk(container, loc)
            return idx+1

        container = []
        container_keys = set()
        reducer_idx = 0
        for k, v in self.buffered_pair:
            # write
            if len(container_keys) == key_split_size:
                loc = mark_location(self.id, self.work_cycle, reducer_idx)
                self.locations.append(os.path.abspath(loc))
                reducer_idx = write_from_container(self.id, self.work_cycle, reducer_idx)
                container.clear()
                container_keys.clear()
            else:
                container_keys.add(k)
                container.append((k, v))
        if len(container_keys) > 0:
            loc = mark_location(self.id, self.work_cycle, reducer_idx)
            self.locations.append(os.path.abspath(loc))
            write_from_container(self.id, self.work_cycle, reducer_idx)



# def count(k, v):
#     pairs = []
#     for s in v.split():
#         pairs.append((s, 1))
#     return pairs
# mapper = Mapper(uid="0", num_reducer=4, map_fn=count)
# f = open("/Users/zhang/Documents/Project/MapReducer/Examples/Alice.txt", 'r')
# mapper.map(f)
# # print (mapper.buffered_pair)
# mapper.store()


