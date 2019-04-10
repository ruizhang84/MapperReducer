from MapReducer.MapReduceWorker import *
import multiprocessing      # since call is blocking, multi-thread is impossible.
import random


def mapper_work(mapper, num_reducer, map_fn,
                parse_fn=text_input_key_value_generator, server_addr=(HOST, HOST_PORT)):
    mapper.map_start(num_reducer, map_fn, parse_fn, server_addr)


def count(k, v):
    pairs = []
    for line in v:
        for s in line.split():
            pairs.append((s, 1))
    return pairs


def random_kill_mapper(process):
    if bool(random.getrandbits(1)):
        process.terminate()


# mapper run
for i in range(5):
    mapper = MapReduceWorker(i)
    mapper_args = (mapper, 5, count)
    mapper_process = multiprocessing.Process(target=mapper_work, args=mapper_args)
    mapper_process.start()
    if i > 0:
        random_kill_mapper(mapper_process)

# print ("Mapper setup")


# from Cluster.ServerClient import *
#
# client = Client()
# client.test()
#
# from Mapper.MapWorker import *
# from MapReducer.MapReduceWorker import *
#
# def count(k, v):
#     pairs = []
#     for line in v:
#         for s in line.split():
#             pairs.append((s, 1))
#     return pairs
#
#
# mapper = MapReduceWorker("0")
# mapper.map_start(1, count)
# mapper = Mapper("0", 1, count)
# mapper.start()
