import re
import os
import logging

logging.basicConfig(filename='C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\logs.txt', level=logging.INFO)
test_log_file = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs\\test_log.txt'
test_log_file_result = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs\\test_log_result.txt'
clearest_log_file = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs\\clearest_log_file.txt'
clear_logs_dir = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs'
file_for_dst = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs\\unique_entries.txt'


def main_func():
    os.chdir(clear_logs_dir)
    clear_logs_list = os.listdir()
    try:
        for log_file in clear_logs_list:
            with open(log_file, 'r') as file:
                split_str = []
                for line in file:
                    tmp_str = line.split(";")
                    if tmp_str[(len(tmp_str) - 1)] == "\n" or (int(tmp_str[(len(tmp_str) - 1)]) > 1024):
                        pass
                    else:
                        split_str.append(re.search('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\S*', line)[0])
            logging.info("splitting has been ended")
            with open(clearest_log_file, 'a+') as file:
                for line in split_str:
                    file.writelines(line + "\n")
            logging.info("file has been written")
    except BaseException as error:
        print(error)


def get_unique_entries():
    with open(clearest_log_file, 'r') as file:
        with open(file_for_dst, 'a+') as unique_file:
            count = 0
            dst_proto_service = []
            unique_entry = []
            for line in file:
                str_split = line.split(";")
                dst_proto_service.append([str_split[1], str_split[2], str_split[3]])
                if unique_entry in dst_proto_service[0]:
                    count += 1
                    logging.info("There are {0} repetitions".format(count))
                    pass
                else:
                    unique_entry.append(dst_proto_service[0])
                    unique_file.write("{0};{1};{2}".format(str_split[1], str_split[2], str_split[3]))
                dst_proto_service.clear()
    pass


# main_func()
get_unique_entries()
