import json
import requests

TABLET_SERVER_COUNT = 2


def request_create_table(args, hostname, port):
    '''
        Send request of creating a new table to the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables"
    response = requests.post(url = url, params = args) 

    return response.json() 
    

def request_delete_table(table_name, hostname, port):
    '''
        Send request of destroy the table from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    response = requests.delete(url = url) 

    return response.json() 

def request_get_table_info(table_name, hostname, port):
    '''
        Send request of get table info from the tablet server #id
    '''
    url = "http://" + hostname + ":" + port + "/api/tables/" + table_name
    response = requests.get(url = url) 

    return response.json() 