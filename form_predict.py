import pandas as pd
import sys

def get_servece_map():
    filepath = "service_encode.csv"
    df = pd.read_csv(filepath)
    serv_set = dict()
    for _, row in df.iterrows():
        serv_set[row["service"]] = row["number"]
    return serv_set

def get_keyword_map():
    filepath = "keyword_encode.csv"
    df = pd.read_csv(filepath)
    keyword_dict = dict()
    for _, row in df.iterrows():
        keyword_dict[row["keyword"]] = row["number"]
    return keyword_dict


services = get_servece_map()
keywords = get_keyword_map()


def get_log_feature(datapack, id):
    try:
        logfilename =  id + "_log.csv"
        input_dir = "../" + datapack + "/"
        input = input_dir + "log/" + logfilename
        
        data_frame = pd.read_csv(input)
        data_frame.sort_values("timestamp", inplace=True);
        
        #diff feature
        group = data_frame.groupby("service")
        data_frame["diff"] = group["timestamp"].diff()
        diff_group_mean = group["diff"].mean().reset_index()
        diff_group_std = group["diff"].std().reset_index()
        diff_mean_array = [0] * len(services)
        diff_std_array = [0] * len(services)
        for _, row in diff_group_mean.iterrows():
            diff_mean_array[services[row["service"]]] = row["diff"]
        
        for _, row in diff_group_std.iterrows():
            diff_std_array[services[row["service"]]] = row["diff"]
        
        #message feature
        data_frame["msg_len"] = data_frame["message"].apply(len)
        msg_len_mean = data_frame["msg_len"].mean()
        msg_len_std = data_frame["msg_len"].std()
        
        #service feature
        count = data_frame["service"].count()
        if count != 0:
            uni_no = 100 * data_frame["service"].nunique() / count
        else:
            uni_no = 0
        
        #count keyword
        matched = data_frame["message"].str.extractall("([A-Za-z_\\-][A-Za-z0-9_\\-]*)")
        matched.reset_index(drop = True, inplace = True)
        count_array = [0] * len(keywords)
        for str in matched[0]:
            if str in keywords:
                count_array[keywords[str]] += 1
        div = (count + 1) / 50
        for i in range(0, len(count_array)):
            count_array[i] = count_array[i] / div


        return (diff_mean_array, diff_std_array, count_array, uni_no, msg_len_mean, msg_len_std)
    except FileNotFoundError:
        diff_mean_array = [0] * len(services)
        diff_std_array = [0] * len(services)
        count_array = [0] * len(keywords)
        uni_no = 0
        msg_len_std = 0
        msg_len_mean = 0
        return (diff_mean_array, diff_std_array, count_array, uni_no, msg_len_mean, msg_len_std)

def get_trace_feature(datapack, id):
    try:
        tracefilename =  id + "_trace.csv"
        input_dir = "../" + datapack + "/"
        input = input_dir + "trace/" + tracefilename
        
        data_frame = pd.read_csv(input)
        data_frame.sort_values("timestamp", inplace=True);
        data_frame["duration"] = data_frame["end_time"] - data_frame["start_time"]
        
        #diff feature
        group = data_frame.groupby("service_name")
        data_frame["serv_diff"] = group["timestamp"].diff()
        serv_diff_group_mean = group["serv_diff"].mean().reset_index()
        serv_diff_group_std = group["serv_diff"].std().reset_index()
        serv_diff_mean_array = [0] * len(services)
        serv_diff_std_array = [0] * len(services)
        for _, row in serv_diff_group_mean.iterrows():
            serv_diff_mean_array[services[row["service_name"]]] = row["serv_diff"]
        
        for _, row in serv_diff_group_std.iterrows():
            serv_diff_std_array[services[row["service_name"]]] = row["serv_diff"]

        #service feature
        count = data_frame["service_name"].count()
        if count != 0:
            uni_no = 100 * data_frame["service_name"].nunique() / count
        else:
            uni_no = 0

        #duration feature
        if count != 0:
            div = count / 50
        else:
            div = 1
        duration_mean = data_frame["duration"].mean()
        duration_std = data_frame["duration"].std()
        duration_short = (data_frame["duration"] < 100).sum() / div
        duration_long = (data_frame["duration"] > 1000000).sum() / div
        duration_max = data_frame["duration"].max()

        return (serv_diff_mean_array, serv_diff_std_array, uni_no, duration_mean, duration_std, duration_short, duration_long, duration_max)
    except FileNotFoundError:
        diff_mean_array = [0] * len(services)
        diff_std_array = [0] * len(services)
        uni_no = 0
        duration_mean = 0
        duration_std = 0
        duration_short = 0
        duration_long = 0
        duration_max = 0
        return (diff_mean_array, diff_std_array, uni_no, duration_mean, duration_std, duration_short, duration_long, duration_max)

def get_id_list(datapack, task_no):
    with open('idlist_' + task_no + '_' + datapack + '.txt', 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    return lines


task_no = sys.argv[1]
datapack = sys.argv[2]

idlist = get_id_list(datapack, task_no)
total = len(idlist)
finish_count = 0
output = open("predict_data_" + task_no + "_" + datapack + ".csv", "w")
for id in idlist:
    diff_mean_array, diff_std_array, count_array, uni_no, msg_len_mean, msg_len_std = get_log_feature(datapack, id)
    print(f"{0}\t{msg_len_std}\t{msg_len_mean}\t{uni_no}\t", end = '', file = output)
    for count in count_array:
        print(count, end = "\t", file = output)

    for value in diff_mean_array:
        print(value, end ="\t", file = output)

    for value in diff_std_array:
        print(value, end ="\t", file = output)

    diff_mean_array, diff_std_array, uni_no, duration_mean, duration_std, duration_short, duration_long, duration_max = get_trace_feature(datapack, id)
    for value in diff_mean_array:
        print(value, end ="\t", file = output)

    for value in diff_std_array:
        print(value, end ="\t", file = output)

    print(f"{duration_std}\t{duration_mean}\t{uni_no}\t{duration_short}\t{duration_long}\t{duration_max}", end = '', file = output)

    print('\n', end = '', file = output)
    finish_count += 1
    print(f"[task_no={task_no}, datapack={datapack}] {id} finished, {100 * finish_count / total : .2f}% completed")
