import json
import os

FILE_FOLDER_PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'
SERVER_METADATA_PATH = FILE_FOLDER_PATH + 'metadata'

def read_dict_from_file(filepath):
    file = None
    try:
        file = open(filepath, 'r')
    except FileNotFoundError:
        return None
    return json.loads(file.read())

def write_dict_to_file(filepath, data_dict):
    file = open(filepath, 'w+')
    file.write(json.dumps(data_dict))

def read_server_metadata():
    return read_dict_from_file(SERVER_METADATA_PATH)

def write_server_metadata(data_dict):
    write_dict_to_file(SERVER_METADATA_PATH, data_dict)

def read_dict_from_table_file(table_name, file_name):
    return read_dict_from_file('{}{}/{}'.format(FILE_FOLDER_PATH, table_name, file_name))

def write_dict_to_table_file(table_name, file_name, data_dict):
    write_dict_to_file('{}{}/{}'.format(FILE_FOLDER_PATH, table_name, file_name), data_dict)

def write_table_wal(table_name, entry_dict):
    wal_path = '{}{}/wal'.format(FILE_FOLDER_PATH, table_name)
    wal_list = read_dict_from_file(wal_path)
    wal_list.append(entry_dict)
    write_dict_to_file(wal_path, wal_list)
