import re
import sqlite3
import os
import logging

logging.basicConfig(filename='C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\logs.txt', level=logging.INFO)
raw_logs_dir = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\raw_logs'
clear_logs_dir = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\clear_logs'
# filename = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_01.txt'
filename_new = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_02.txt'


def main_func():
    raw_files_list = os.listdir(raw_logs_dir)
    for log_file in raw_files_list:
        os.chdir(raw_logs_dir)
        try:
            with open(log_file, 'r') as raw_file:
                os.chdir(clear_logs_dir)
                with open(log_file, 'a+') as clear_file:
                    for line in raw_file:
                        if re.search('migrate', line):
                            clear_file.write(line)
        except BaseException as error:
            logging.error("{0}".format(error))
            # print(error)


def sqlite_func():
    # create table log_database.db
    connection = sqlite3.connect('C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_database.db')
    connection_distinct = sqlite3.connect('C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_database_DISTINCT.db')
    # specify cursor to be able to execute SQL commands
    conn_executor = connection.cursor()
    conn_executor_distinct = connection_distinct.cursor()
    # conn_executor.execute("DROP TABLE IF EXISTS logs")
    conn_executor.execute('''CREATE TABLE IF NOT EXISTS logs
                            (src_ip, dst_ip, proto, service)''')
    conn_executor_distinct.execute('''CREATE TABLE IF NOT EXISTS logs
                            (src_ip, dst_ip, proto, service)''')
    # scheme for inserting data
    scheme_insert = """INSERT INTO logs ('src_ip','dst_ip','proto','service') VALUES (?, ?, ?, ?)"""
    # go through the file and save changes to DB

    count = 0
    list_into_table = []
    os.chdir(clear_logs_dir)
    clear_logs_list = os.listdir()
    try:
        for log_file in clear_logs_list:
            file = open(log_file, 'r')
            lines = file.readlines()
            for line in lines:
                try:
                    line_split = line.split(";")
                    splitted_values = [line_split[3], line_split[4], line_split[5], line_split[6]]
                    count += 1
                    list_into_table.append(splitted_values)
                    if count % 200000 == 0 or count % len(lines) == 0:
                        for i in list_into_table:
                            conn_executor.execute(scheme_insert, i)
                        list_into_table.clear()
                        print("The {0} log entries has been written into a table".format(count))
                        connection.commit()
                except BaseException as error:
                    logging.info("Current log file is {0} and line is {1}".format(log_file, line))
                    logging.error("{0}".format(error))
    except sqlite3.Error as e:
        print("Error occurred:", e.args[0])
    list_into_table.clear()
    print("Execute DISTINCT ...")
    conn_executor.execute('''SELECT DISTINCT src_ip, dst_ip, proto, service from logs''')
    print("Executing DISTINCT has been successfully preformed.")
    list_into_table = conn_executor.fetchall()
    conn_executor.close()

    # write the whole distinct instances to new DB
    count = 0
    print("Starting writing distinct entries into a table")
    for entry in list_into_table:
        count += 1
        conn_executor_distinct.execute(scheme_insert, entry)
        if count % 200000 == 0 or count % len(list_into_table) == 0:
            connection_distinct.commit()
    conn_executor_distinct.close()


# main_func()
sqlite_func()
