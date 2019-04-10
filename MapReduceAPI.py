from Master.Master import *
from MapReducer.MapReduceWorker import *
import multiprocessing      # since call is blocking, multi-thread is impossible.
import uuid
import Util

CLUSTER_TABLE = {} # not good as global variable, but required since init_cluster returns int


def master_work(master, data_locations, mapper_ids, reducer_ids, output_location):
    master.start(data_locations, mapper_ids, reducer_ids, output_location)


def mapper_work(mapper, num_reducer, map_fn,
                parse_fn=text_input_key_value_generator, server_addr=(HOST, HOST_PORT)):
    mapper.map_start(num_reducer, map_fn, parse_fn, server_addr)


def reducer_work(reducer, reduce_fn, server_addr=(HOST, HOST_PORT)):
    reducer.reduce_start(reduce_fn, server_addr)


def init_cluster(ip_address=HOST, port=HOST_PORT):
    uid = str(uuid.uuid4())
    mapper_reducer = MapReduceWorker(uid)
    CLUSTER_TABLE[uid] = mapper_reducer
    return uid


def balance_mapper_reducer(clusters):
    mappers = clusters[:len(clusters) // 2]
    reducers = clusters[len(clusters) // 2:]
    return mappers, reducers


def run_mapred(input_files, map_fn, reduce_fn, output_location):
    # init master and works
    master = Master((HOST, HOST_PORT), MasterHandler)
    clusters = [uid for uid in CLUSTER_TABLE]

    if len(clusters) < 2:
        raise ValueError("Not enough Cluster!")
    # load balance
    mappers_uid, reducers_uid = balance_mapper_reducer(clusters)

    # split files
    input_data = []
    for file in input_files:
        splits = Util.split_file(file_path=file, num_split=len(mappers_uid))
        input_data.append(splits)
    #     print (splits)
    # print (input_data)

    # print ("start!")
    master_args = (master, input_data, mappers_uid, reducers_uid, output_location)
    master_process = multiprocessing.Process(target=master_work, args=master_args)
    master_process.start()
    # print ("Master Donez!")

    # mapper run
    for uid in mappers_uid:
        mapper = CLUSTER_TABLE[uid]
        mapper_args = (mapper, len(reducers_uid), map_fn)
        mapper_process = multiprocessing.Process(target=mapper_work, args=mapper_args)
        mapper_process.start()
    # print ("Mapper setup")

    # reducer run
    for uid in reducers_uid:
        reducer = CLUSTER_TABLE[uid]
        reducer_args = (reducer, reduce_fn)
        reducer_process = multiprocessing.Process(target=reducer_work, args=reducer_args)
        reducer_process.start()
    # print ("reducer setup")


def destroy_cluster(cluster_id):
    if cluster_id in CLUSTER_TABLE:
        del CLUSTER_TABLE[cluster_id]

