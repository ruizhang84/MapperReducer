import threading
import copy


def pick_pool(pool):
    """
    pick a task from pool
    :param pool:
    :return:
    """
    task = []
    for idx in range(len(pool)):
        if len(pool[idx]) > 0:
            task.append(pool[idx].pop())
    return task


class Scheduler:
    """
    Assign tasks to worker from a pool
    """
    def __init__(self):
        self.lock = threading.Semaphore()
        self.pool = []

    def add_all(self, pool):
        self.pool = copy.deepcopy(pool)

    def append(self, data):
        self.pool.append(copy.deepcopy(data))

    def get_task(self):
        task = None
        self.lock.acquire()
        if len(self.pool) > 0:
            task = pick_pool(self.pool)
        self.lock.release()
        if len(task) > 0:
            print ("Scheduler schedule: " + str(task))
        return task


# data = [['/Users/zhang/Documents/Project/MapReducer/Input_1868_Alice.txt',
#         '/Users/zhang/Documents/Project/MapReducer/Input_1234_Alice.txt']]
#
#
# class MyThread(threading.Thread):
#     def run(self):
#         self.scheduler.get_task()
#
#
# scheduler = Scheduler(data)
# # scheduler.get_task()
# for i in range(10):
#     thread = MyThread()
#     thread.scheduler = scheduler
#     thread.run()
