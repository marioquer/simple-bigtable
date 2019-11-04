import json
import os
import requests
import requests.exceptions
import shutil

SHARDING_ROW_THRESHOLD = 1000
FILE_FOLDER_PATH = os.path.abspath(os.path.join(__file__, '../../tablet/files'))

def request_create_table(args_dict, hostname, port):
    '''
        Send request of creating a new table to the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables"
    return requests.post(url, json=args_dict)
    

def request_delete_table(table_name, hostname, port):
    '''
        Send request of destroy the table from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    return requests.delete(url) 

def request_get_table_info(table_name, hostname, port):
    '''
        Send request of get table info from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tablets/" + table_name
    return requests.get(url)

def check_single_tablet_server_status_helper(hostname, port):
    '''
        Send request to check single tablet server status 
    '''
    url = "http://" + hostname + ":" + port + "/api/tabletstatus"
    try:
        response = requests.get(url, verify=False)
    except requests.exceptions.ConnectionError as e:
        print(e)
        return 403
    except requests.exceptions.Timeout as e:
        print(e)
        return 408
    else:
        return 200

def restore_data_on_new_server(
        old_tablet_server_id, target_tablet_server_id, hostname, port
    ):
    '''
        Restore data from old tablet server on the target one
    '''
    old_server_folder_path = '{}/{}'.format(FILE_FOLDER_PATH, 'server' + str(old_tablet_server_id))
    target_server_folder_path = '{}/{}'.format(FILE_FOLDER_PATH, 'server' + str(target_tablet_server_id))
    
    # copy all files under src to dst
    copy_dir(old_server_folder_path, target_server_folder_path, True)

    print("copy dir finished...")

    # send request of restoring data on the new server
    response = request_restore_tablet_server(hostname, port)

    print("restore finished...")
    if response.status_code is 200:
        # delete files on old_tablet_server_id
        print("rmtree called")
        shutil.rmtree(old_server_folder_path)

def copy_dir(src, dst, is_server_metadata):
    '''
        Copy all files under src to dst
    '''
    # make target_server_folder_path dir
    if os.path.isdir(src) and not os.path.exists(dst):
        os.makedirs(dst)
    
    # get files under old_server_folder_path
    if os.path.isdir(src):
        files = os.listdir(src)
    
        # recursively copy files to target_server_folder_path
        for f in files:
            if f == 'metadata' and is_server_metadata is True:
                merge_two_servers_metadata(src, dst)
            else:
                s = src + '/' + f
                d = dst + '/' + f
                if os.path.isdir(s):
                    copy_dir(s, d, False)
                else:
                    shutil.copy2(str(s), str(d))
    else:
        shutil.copy2(str(s), str(d))

def merge_two_servers_metadata(src, dst):
    '''
        Merge two tablet servers' metadata
    '''
    old_filepath = src + '/metadata'
    target_filepath = dst + '/metadata'
    
    old_metadata = read_dict_from_file(old_filepath)
    target_metadata = read_dict_from_file(target_filepath)

    # copy tables info in the old metadata to the new metadata
    for table_name in old_metadata['tables']:
        target_metadata['tables'][table_name] = old_metadata['tables'][table_name]
    
    write_server_metadata(target_filepath, target_metadata)

def write_server_metadata(server_metadata_path, data_dict):
    write_dict_to_file(server_metadata_path, data_dict)

def write_dict_to_file(filepath, data_dict):
    file = open(filepath, 'w+')
    file.write(json.dumps(data_dict))

def read_dict_from_file(filepath):
    file = None
    try:
        file = open(filepath, 'r')
    except FileNotFoundError:
        return None
    return json.loads(file.read())

def request_restore_tablet_server(hostname, port):
    '''
        Send request of restoring data on the new server
    '''
    url = "http://" + hostname + ":" + port + "/api/restore"
    return requests.get(url)
