def truncate(file_name):
    with open("simple_"+file_name, 'w') as out_file:
        with open(file_name, 'r') as fin:
            for i, l in enumerate(fin):
                if i == 100:
                    break
                out_file.write(l)


truncate("Alice.txt")
truncate("Grimms.txt")
truncate("Iliad.txt")
