import json
import requests

SHARDING_ROW_THRESHOLD = 1000

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
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    return requests.get(url) 

def check_single_tablet_server_status_helper(hostname, port):
    '''
        Send request to check single tablet server status 
    '''
    url = "http://" + hostname + ":" + port + "/api/tabletstatus"
    return requests.get(url)