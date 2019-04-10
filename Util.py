import math
import inspect, os


def count_line(file_name):
    line_number = 0
    with open(file_name, 'r') as f:
        for _ in f:
            line_number += 1
    return line_number


def write_file(file_path, i, write_out):
    _, file_name = os.path.split(file_path)
    out_name = "Input_" + str(i) + "_" + file_name
    with open(out_name, 'w') as f:
        f.write(write_out)
    return out_name


def split_file(file_path, num_split=2):
    """
    split a given file into smaller pieces
    :param file_path: path to file
    :param num_split: number of split
    :return:
    """
    num = count_line(file_path)
    split_size = math.ceil(num/num_split)
    file_locations = []

    write_out = ""
    line_num = 0
    with open(file_path, 'r') as fin:
        for i, line in enumerate(fin):
            line_num = i
            write_out += line
            if line_num % split_size == 0 and line_num != 0:
                out_file = write_file(file_path, line_num, write_out)
                file_locations.append(os.path.abspath(out_file))
                write_out = ""

        if len(write_out) > 0:
            out_file = write_file(file_path, line_num, write_out)
            file_locations.append(os.path.abspath(out_file))
    return file_locations


# print(split_file("/Users/zhang/Documents/Project/MapReducer/Examples/Alice.txt"))
# ['/Users/zhang/Documents/Project/MapReducer/Input_1868_Alice.txt',
# '/Users/zhang/Documents/Project/MapReducer/Input_3735_Alice.txt']

