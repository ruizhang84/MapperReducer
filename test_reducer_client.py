from MapReducer.MapReduceWorker import *
import multiprocessing
import random


def reducer_work(reducer, reduce_fn, server_addr=(HOST, HOST_PORT)):
    reducer.reduce_start(reduce_fn, server_addr)


def merge(k, vs):
    return k, str(sum(vs))


def random_kill_reducer(process):
    if bool(random.getrandbits(1)):
        process.terminate()


for i in range(5, 10):
    reducer = MapReduceWorker(i)
    reducer_args = (reducer, merge)
    reducer_process = multiprocessing.Process(target=reducer_work, args=reducer_args)
    reducer_process.start()
    if i > 5:
        random_kill_reducer(reducer_process)

# from Reducer.ReduceWorker import *
# from MapReducer.MapReduceWorker import *
#
# def merge(k, vs):
#     return k, str(sum(vs))
#
# reducer = MapReduceWorker("1")
# reducer.reduce_start(reduce_fn=merge)
# reducer = Reducer(uid="1", reduce_fn=merge)
# reducer.start()
