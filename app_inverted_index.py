import MapReduceAPI
import os


# test
def label(k, v):
    pairs = []
    _, filename = os.path.split(k)
    prefix_name = filename.split("_")
    name = "_".join(prefix_name[2:])
    for line in v:
        for s in line.split():
            pairs.append((s, name))
    return pairs


def merge(k, vs):
    v = set()
    for d in vs:
        v.add(d)
    v = list(v)
    return k, ','.join(v)


def conver_string_id(input_location="result_temp.txt", output_location="result.txt"):
    output = []
    with open(input_location, 'r') as f_in:
        doc_id = {}
        for line in f_in:
            k, v = line.split("\t")
            vs = v.strip().split(',')
            out_vs = []
            for d in vs:
                if d not in doc_id:
                    doc_id[d] = len(doc_id)
                out_vs.append(doc_id[d])
            out_vs.sort()
            out_vs = [str(s) for s in out_vs]
            output.append(k + "\t" + ",".join(out_vs) + "\n")

    with open(output_location, 'w') as out_f:
        for line in output:
            out_f.write(line)


data = ["Examples/simple_Alice.txt",
        "Examples/simple_Grimms.txt",
        "Examples/simple_Iliad.txt"]

for i in range(4):
    print("create a cluster: " + MapReduceAPI.init_cluster())

MapReduceAPI.run_mapred(data, label, merge, "result_temp.txt")
# convert doc_string to doc_id
conver_string_id()


