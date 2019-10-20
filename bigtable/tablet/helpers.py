import json
import os
import shutil

folder_path = './files/'
server_metadata_path = folder_path + 'metadata'

def read_dict_from_file(filepath):
    file = open(filepath, 'r')
    return None if file == None else json.loads(file.read())

def write_dict_to_file(filepath, data_dict):
    file = open(filepath, 'w')
    file.write(json.dumps(data_dict))

def read_server_metadata():
    return read_dict_from_file(server_metadata_path)

def write_server_metadata(data_dict):
    write_dict_to_file(server_metadata_path, data_dict)

def read_table_metadata(table_name):
    return read_dict_from_file('{}{}/metadata'.format(folder_path, table_name))

def write_table_metadata(table_name, data_dict):
    write_dict_to_file('{}{}/metadata'.format(folder_path, table_name), data_dict)