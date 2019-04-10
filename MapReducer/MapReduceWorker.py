from Mapper.MapWorker import *
from Reducer.ReduceWorker import *


class MapReduceWorker(Mapper, Reducer):
    """
    a worker can map and reduce
    """
    def __init__(self, uid):
        Mapper.__init__(self, uid)
        Reducer.__init__(self, uid)
        self.id = uid
        self.work_cycle = 0

