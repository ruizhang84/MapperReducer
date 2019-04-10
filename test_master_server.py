from Master.Master import *
import multiprocessing      # since call is blocking, multi-thread is impossible.
import Util


def master_work(master, data_locations, mapper_ids, reducer_ids, output_location):
    master.start(data_locations, mapper_ids, reducer_ids, output_location)


CLUSTER_TABLE = {}

data = ["Examples/simple_Alice.txt",
        "Examples/simple_Grimms.txt",
        "Examples/simple_Iliad.txt"]

# init master and workers
master = Master((HOST, HOST_PORT), MasterHandler)

mappers_uid = [str(i) for i in range(5)]
reducers_uid = [str(i) for i in range(5, 10)]

# split files
input_data = []
for file in data:
    splits = Util.split_file(file_path=file, num_split=len(mappers_uid))
    input_data.append(splits)


# print ("start!")
master_args = (master, input_data, mappers_uid, reducers_uid, "test_result.txt")
master_process = multiprocessing.Process(target=master_work, args=master_args)
master_process.start()
# print ("Master Donez!")


# from Master.Master import *
# from Reducer.ReduceWorker import *
# data = [['/Users/zhang/Documents/Project/MapReducer/Input_1868_Alice.txt',
#         '/Users/zhang/Documents/Project/MapReducer/Input_3735_Alice.txt']]
# data = [['/Users/zhang/Documents/Project/MapReducer/Input_Alice.txt']]
#
# s = Master((HOST, HOST_PORT), MasterHandler)
# s.start(data_locations=data, mapper_ids=["0"], reducer_ids=["1"])