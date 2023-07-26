import pandas as pd
import sys

filemode = "w"
if len(sys.argv) == 3:
    filemode = sys.argv[2]


with open("all_sources", "r") as f:
    lines = f.readlines()
sources = [line.strip() for line in lines]
files = dict()
for source in sources:
    files[source] = open("binary_train_data/" + source[0:len(source) if len(source) < 8 else 8], filemode)

with open('train_data_' + sys.argv[1] + '.csv') as file:
    lines = file.readlines()
    for line in lines:
        index = line.find("\t")
        id = line[0:index]
        for source in sources:
            if source == id:
                print("1", file = files[source], end = '')
            else:
                print("0", file = files[source], end = '')
            print(line[index:], file = files[source], end = '')
