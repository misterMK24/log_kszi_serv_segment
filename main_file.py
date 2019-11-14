import re
import sqlite3

filename = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_01.txt'
filename_new = 'C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_01_edited.txt'


def main_func():
    with open(filename, 'r') as file:
        with open(filename_new, 'a+') as file_new:
            for line in file:
                if re.search('migrate', line):
                    file_new.write(line)


def sqlite_func():
    ''' creating SQL DB '''
    # value of the DB rows
    c_src = 'src_ip'
    c_dst = 'dst_ip'
    c_proto = 'proto'
    c_port = 'port'

    # create table log_database.db
    connection = sqlite3.connect('C:\\Трансгаз Казань\\_2019\\rules_kszi_editing\\log_database.db')

    # specify cursor to be able to execute SQL commands
    conn_executor = connection.cursor()
    conn_executor.execute("DROP TABLE IF EXISTS logs")
    conn_executor.execute('''CREATE TABLE logs
                            (src_ip, dst_ip, proto, service)''')

    # some schema for inserting data
    schema_insert = """INSERT INTO logs ('src_ip','dst_ip', 'proto', 'service') VALUES (?, ?, ?, ?)"""

    # go through the file and save changes to DB
    count = 0
    list_into_table = []
    try:
        file = open(filename_new, 'r')
        lines = file.readlines()
        for line in lines:
            line_split = line.split(";")
            splitted_values = [line_split[3], line_split[4], line_split[5], line_split[6]]
            count += 1
            list_into_table.append(splitted_values)
            if count % 100000 == 0 or count % len(lines) == 0:
                for i in list_into_table:
                    # print(i)
                    conn_executor.execute(schema_insert, i)
                list_into_table.clear()
                print("some data has been written into a table!")
            connection.commit()
            # pass
    except sqlite3.Error as e:
        print("Error occured:", e.args[0])
    conn_executor.close()

# main_func()
sqlite_func()
