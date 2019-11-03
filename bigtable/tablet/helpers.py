import json
import os

FILE_FOLDER_PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

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

def read_server_metadata(server_metadata_path):
    return read_dict_from_file(server_metadata_path)

def write_server_metadata(server_metadata_path, data_dict):
    write_dict_to_file(server_metadata_path, data_dict)

def read_dict_from_table_file(server_folder_path, table_name, tablet_name, file_name):
    return read_dict_from_file('{}/{}/{}/{}'.format(server_folder_path, table_name, tablet_name, file_name))

def write_dict_to_table_file(server_folder_path, table_name, tablet_name, file_name, data_dict):
    write_dict_to_file('{}/{}/{}/{}'.format(server_folder_path, table_name, tablet_name, file_name), data_dict)

def write_table_wal(server_folder_path, table_name, tablet_name, entry_dict):
    wal_path = '{}/{}/{}/wal'.format(server_folder_path, table_name, tablet_name)
    wal_list = read_dict_from_file(wal_path)
    wal_list.append(entry_dict)
    write_dict_to_file(wal_path, wal_list)
