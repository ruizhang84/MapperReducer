import MapReduceAPI


# test
def count(k, v):
    pairs = []
    for line in v:
        for s in line.split():
            pairs.append((s, 1))
    return pairs


def merge(k, vs):
    return k, str(sum(vs))


data = ["Examples/simple_Alice.txt",
        "Examples/simple_Grimms.txt",
        "Examples/simple_Iliad.txt"]
for i in range(4):
    print("create a cluster: " + MapReduceAPI.init_cluster())

MapReduceAPI.run_mapred(data, count, merge, "result.txt")
