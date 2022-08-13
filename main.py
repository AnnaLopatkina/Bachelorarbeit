import urllib.request
import duckdb
from pathlib import Path
import seaborn as sns
import pandas as pd
import psutil as psutil
import regex as re
import bz2
import time
import matplotlib.pyplot as plt
import os


def load_bz2_data():
    directory = './benchmark/'
    files = Path(directory).glob('*')
    for file in files:
        data_urls = str(file) + "/data-urls.txt"
        file1 = open(f"{data_urls}", 'r')
        lines = file1.readlines()
        for line in lines:
            print(f"str   {str(line)}   path {str(file)}")
            file_name = line.split("/")[-1]
            bz2_path = str(file).replace("\\", "/") + "/data/" + file_name.replace("\n", "")
            print(bz2_path)
            urllib.request.urlretrieve(f'{line}', bz2_path)


def decode_one_file_bz2_to_csv(file):
    file_name = str(file).replace(".csv.bz2", ".data.csv")
    print(f"{str(file_name)}")
    with bz2.open(str(file)) as f:
        content = f.read()
    open(f"{str(file_name)}", 'wb').write(content)


def create_one_table(folder_name):
    tables = f'./benchmark/{folder_name}/tables/'
    tables = Path(tables).glob('*')
    folder = f'./benchmark/{folder_name}'
    for table in tables:
        # execute_scripts_from_file(f"{table}")  # create table
        con.execute(get_sql(f"{table}"))
        copy_data_csv_into_table(table.name, folder)  # paste data


def copy_data_csv_into_table(table_name: str, folder):
    print(folder)
    file_name = table_name.replace("table.sql", "data.csv")
    file_prefix = file_name.replace(".data.csv", "")
    file_path = str(folder) + "/data/" + file_name
    print(file_path)
    con.execute(f"COPY {file_prefix} FROM '{file_path}' (AUTO_DETECT TRUE, NULL 'null')")
    sql_query = pd.read_sql_query(f"SELECT * FROM {file_prefix}", con)
    df = pd.DataFrame(sql_query)
    print("sucsses")
    df.to_csv(str(folder) + '/data/' + str(file_prefix) + '.csv', index=False)


def get_sql(filename):
    fd = open(filename, encoding="utf-8")
    sql_file = fd.read()
    fd.close()
    return sql_file


def execute_queries():
    directory = './benchmark/'
    files = Path(directory).glob('*')
    for file in files:
        queries = str(file) + "/queries/"
        queries = Path(queries).glob('*')
        for query in queries:
            print(f" queries:{query}")
            # execute_scripts_from_file(f"{query}")
            con.execute(get_sql(f"{query}"))


def find_word_with_start_index(folder_name, sql_file):
    index = sql_file.find(folder_name)
    result = ''
    while True:
        if sql_file[index] is '\"':
            break
        result += sql_file[index]
        index = index + 1
    return result


def heat_map(folder_name: str):
    file_name = ''
    lst = []
    directory = './benchmark/' + folder_name + '/query_with_join/'
    queries = Path(directory).glob('*')
    data_formats = ['virtual_table', 'parquet', 'df', 'csv', 'csv.gz']
    for query in queries:
        sql_file = get_sql(query)
        file_name = find_word_with_start_index(folder_name, sql_file)
        table_path = f'./benchmark/{folder_name}/tables/{file_name}.table.sql'
        file_path = f'./benchmark/{folder_name}/data/{file_name}.csv'
        con.execute(get_sql(table_path))
        con.execute(f"COPY {file_name} FROM '{file_path}' (AUTO_DETECT TRUE, NULL 'null')")
    for data_format in data_formats:
        if data_format == 'virtual_table':
            path = file_name
        elif data_format == 'df':
            path = convert_csv_to_df('./benchmark/' + folder_name + "/data/" + file_name + '.csv')
        else:
            path = './benchmark/' + folder_name + "/data/" + file_name + "." + data_format
        data_formats, execution_time = change_queries_of_one_folder_with_join(folder_name, path)
        lst.append(execution_time)
    df = pd.DataFrame(lst, index=data_formats, columns=data_formats, dtype=float)
    ax = sns.heatmap(df, annot=True)
    plt.show()


def change_queries_of_one_folder_with_join(folder_name: str, path=None):
    directory = './benchmark/' + folder_name + '/query_with_join/'
    queries = Path(directory).glob('*')
    execution_time, memory_usage = [], []
    data_formats = ['virtual_table', 'parquet', 'df', 'csv', 'csv.gz']
    for query in queries:
        for data_format in data_formats:
            if data_format == 'virtual_table':
                sql_file = get_sql(query)
                new_sql = sql_file
            else:
                new_sql, file_name, sql_file = change_table_name_to_path(folder_name, data_format, query)
            if sql_file.__contains__("INNER JOIN"):
                new_sql = re.sub('(INNER JOIN) "[A-Za-z]+\d*_\d+"[^.]', f'INNER JOIN \"{path}\" AS {folder_name} ',
                                 new_sql)
                print(new_sql)
            execution_time, memory_usage = time_memory_usage(new_sql, execution_time, memory_usage)
    plot_data(data_formats, execution_time, 'execution time diagram', 'formats', 'execution time', 30, 18)
    plot_data(data_formats, memory_usage, 'memory usage diagram', 'formats', 'memory usage', 30, 18)
    return data_formats, execution_time


def change_queries_of_one_folder(folder_name: str):
    directory = './benchmark/' + folder_name + '/queries/'
    queries = Path(directory).glob('*')
    execution_time, memory_usage = [], []
    data_formats = ['virtual_table', 'parquet', 'df', 'csv', 'csv.gz']
    for query in queries:
        for data_format in data_formats:
            if data_format == 'virtual_table':
                sql_file = get_sql(query)
                file_name = find_word_with_start_index(folder_name, sql_file)
                table_path = f'./benchmark/{folder_name}/tables/{file_name}.table.sql'
                file_path = f'./benchmark/{folder_name}/data/{file_name}.csv'
                con.execute(get_sql(table_path))
                con.execute(f"COPY {file_name} FROM '{file_path}' (AUTO_DETECT TRUE, NULL 'null')")
                new_sql = sql_file
            else:
                new_sql, file_name, sql_file = change_table_name_to_path(folder_name, data_format, query)
                print(new_sql)
            execution_time, memory_usage = time_memory_usage(new_sql, execution_time, memory_usage)
    plot_data(data_formats, execution_time, 'execution time diagram', 'formats', 'execution time', 25, 13)
    plot_data(data_formats, memory_usage, 'memory usage diagram', 'formats', 'memory usage', 25, 13)
    return data_formats, execution_time


def change_table_name_to_path(folder_name: str, data_format, query):
    print(data_format)
    print(f" queries:{query}")
    sql_file = get_sql(query)
    file_name = find_word_with_start_index(folder_name, sql_file)
    if data_format == 'df':
        reference = convert_csv_to_df('./benchmark/' + folder_name + "/data/" + file_name + '.csv')
    else:
        reference = './benchmark/' + folder_name + "/data/" + file_name + '.' + data_format
    print(f"file path {reference}")
    return re.sub('(FROM) "[A-Za-z]+\d*_\d+"[^.]', f'FROM \"{reference}\" AS \"{file_name}\" ',
                  sql_file), file_name, sql_file


def time_memory_usage(new_sql, execution_time, memory_usage=None):
    ex_time, mem_use = 0, 0
    for i in range(0, 5):
        old = psutil.Process(os.getpid()).memory_info()[0] / 1024 ** 2
        start_time = time.time()
        con.execute(new_sql)
        elapsed_time = time.time() - start_time
        memory_use = psutil.Process(os.getpid()).memory_info()[0] / 1024 ** 2 - old
        ex_time += elapsed_time
        mem_use += memory_use
    execution_time.append(ex_time / 5.0)
    if memory_usage is not None:
        memory_usage.append(mem_use / 5.0)
    return execution_time, memory_usage


def convert_csv_to_df(data_csv):
    df = pd.read_csv(data_csv, sep=',')
    con.register('test_df_view', df)
    return 'test_df_view'


def convert_csv_to_parquet(data_csv):
    df = pd.read_csv(data_csv)
    new_folder = str(data_csv).replace(".csv", ".parquet")
    return df.to_parquet(new_folder, engine='fastparquet')


def compress_csv_to_gz(data_csv):
    df = pd.read_csv(data_csv)
    new_folder = str(data_csv).replace(".csv", ".csv.gz")
    return df.to_csv(new_folder, compression='gzip')


def compress_csv_to_bz2(data_csv):
    df = pd.read_csv(data_csv)
    new_folder = str(data_csv).replace(".csv", ".bz2")
    return df.to_csv(new_folder, compression='bz2')


def plot_data(data_format: list, exec_memory: list, title: str, x_label: str, y_label: str, x=None, y=None):
    if x is not None and y is not None:
        plt.figure(figsize=(x, y))
    plt.subplot(131)
    plt.xticks(rotation=45, ha='right', fontsize=22)
    plt.title(title, fontsize=22)
    plt.xlabel(x_label, fontsize=22)
    plt.ylabel(y_label, fontsize=22)
    plt.bar(data_format, exec_memory)
    plt.rcParams.update({'font.size': 22})
    for index, value in enumerate(exec_memory):
        if value >= 0:
            plt.text(index - 0.35, value + 0.1, str(round(value, 2)))
        else:
            plt.text(index - 0.35, value - 0.5, str(round(value, 2)))
    plt.show()


def plot_data_size(folders):
    data_format, data_size = [], []
    for path, dirs, files in os.walk(folders):
        for f in files:
            fp = os.path.join(path, f)
            print(fp)
            data_size.append(round(os.path.getsize(fp) / (1024 * 1024), 2))
            data_format.append(f)
    for i in data_size, data_format:
        print(i)
    plot_data(Arade_data_size, data_size, 'data size', 'formats', 'size in MB', 25, 13)


def change_operator_in_query(query, old_operator, new_operator):
    new_query = re.sub(old_operator, new_operator, query)
    ex_time, mem_use = 0, 0
    for i in range(0, 5):
        start_time = time.time()
        con.execute(new_query)
        elapsed_time = time.time() - start_time
        print(elapsed_time)
        ex_time += elapsed_time
    print('whole: ', ex_time, mem_use)
    return ex_time / 5.0


def filter_function(query_filter_path: str):
    execution_memory_for_filter_and_groupby(query_filter_path, 9888775)


def group_by(query_filter_path: str):
    execution_memory_for_filter_and_groupby(query_filter_path, 4347702)


def execution_memory_for_filter_and_groupby(query_path: str, tuples: int):
    execution_time, memory_time = [], []
    data_formats = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    for data_format in data_formats:
        if data_format == 'virtual_table':
            sql_file = get_sql(f'benchmark/Arade/{query_path}')
            file_name = find_word_with_start_index('Arade', sql_file)
            table_path = f'./benchmark/Arade/tables/{file_name}.table.sql'
            file_path = f'./benchmark/Arade/data/{file_name}.csv'
            con.execute(get_sql(table_path))
            con.execute(f"COPY {file_name} FROM '{file_path}' (AUTO_DETECT TRUE, NULL 'null')")
            new_sql = sql_file
        else:
            new_sql, file_name, sql_file = change_table_name_to_path('Arade', data_format,
                                                                     f'./benchmark/Arade/{query_path}')
        execution_time, memory_time = time_memory_usage(new_sql, execution_time, memory_time)
    i = 0
    for z in con.fetchall():
        i = i + 1
    title = f'{i}, {round(i / tuples * 100)}%'
    for y in execution_time:
        print('dict', y)
    print(memory_time)
    plot_data(data_formats, execution_time, title, '', 'execution time', 30, 18)
    plot_data(data_formats, memory_time, title, '', 'memory usage in MB', 30, 18)


Arade_formats = ["Arade.parquet", "Arade_df", "Arade.csv", "Arade.gz", "Arade_vt"]
Arade_data_size = ["Arade.csv", "Arade_roh.bz2", "Arade.gz", "Arade_roh.csv", "Arade.parquet"]

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:', read_only=False)

