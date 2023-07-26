import pandas as pd
import os

rootdir = '../0/log/'

services = set()

for dirpath, dirnames, filenames in os.walk(rootdir):
    for filename in filenames:
        filepath = os.path.join(dirpath, filename)
        df = pd.read_csv(filepath)
        for service in df['service']:
            services.add(service)
        #print("finished " + filename)

count = 0
print("service,number")
for service in services:
    print(service + "," + count)
