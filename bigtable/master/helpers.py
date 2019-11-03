import json
import requests

SHARDING_ROW_THRESHOLD = 1000

def request_create_table(args_dict, hostname, port):
    '''
        Send request of creating a new table to the tablet server #id
    '''

    url = "http://" + hostname + ":" + port + "/api/tables"
    print('---------------------------')
    print(url)
    print(args_dict)
    response = requests.post(url, json=args_dict)
    print('---------------------------')
    return response
    

def request_delete_table(table_name, hostname, port):
    '''
        Send request of destroy the table from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    response = requests.delete(url) 

    return response

def request_get_table_info(table_name, hostname, port):
    '''
        Send request of get table info from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    response = requests.get(url) 

    return response 