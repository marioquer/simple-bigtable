import os
import sys
import threading
import time
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '../../..')))
from bigtable.master.helpers import *
import logging

class MasterServer:
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('MasterServer')
        self.logger.setLevel(logging.DEBUG)
        self.open_tables = {}  # {'table_name': ['client_id']}
        self.table_locations = {}  # {'table_name': [tablet_server_id]}
        self.current_tablet_server_id = 0 # round robin: next server
        self.tablet_server_count = 0 # updated when a new tablet server integrated

        # tablet servers info: {tablet_server_id: {'hostname': '', 'port': ''} }
        self.tablet_servers = {}

    def check_single_tablet_server_status(self, tablet_server_id, hostname, port):
        response = check_single_tablet_server_status_helper(hostname, port)
        while response.status_code is 200:
            time.sleep(2)
            response = check_single_tablet_server_status_helper(hostname, port)

        # When the server is down, do recovery
        self.recovery_for_server_down(tablet_server_id)

    def recovery_for_server_down(self, tablet_server_id):
        pass

    def register_tablet_server(self, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        hostname = args_dict['hostname']
        port = str(args_dict['port'])

        # update tablet server info
        tablet_server_id = self.tablet_server_count
        self.tablet_servers[tablet_server_id] = {
            'hostname': hostname,
            'port': port
        }

        # try:
        #     threading.Thread(target=self.check_single_tablet_server_status, args=(tablet_server_id, hostname, port, )
        #     )
        # except:
        #     self.logger.error(
        #         "Error to start a new thread for monitoring hostname " + str(hostname)
        #     )
        # update tablet_server_count
        self.tablet_server_count += 1
        
        return {
            'tablet_server_id': tablet_server_id
        }, 200

    def list_tables(self):
        return {
                'tables': list(self.table_locations.keys())
            }, 200

    def create_table(self, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        table_name = args_dict['name']

        # table already exists
        if table_name in self.table_locations.keys():
            return '', 409

        if self.current_tablet_server_id > self.tablet_server_count or \
            self.current_tablet_server_id == self.tablet_server_count:
            self.current_tablet_server_id = 0
        
        hostname = self.tablet_servers[self.current_tablet_server_id]['hostname']
        port = self.tablet_servers[self.current_tablet_server_id]['port']

        # send request to tablet server for creating a new table
        response = request_create_table(
            args_dict, 
            hostname,
            port
        )

        # check response
        if response.status_code is 400:
            self.logger.debug(
                'create_table Failure - 400 returned from tablet server ' + str(self.current_tablet_server_id)
            )
            return '', 400
        elif response.status_code is 409:
            self.logger.debug(
                'create_table Failure - 409 returned from tablet server ' + str(self.current_tablet_server_id)
            )
            return '', 409
        else:
            self.table_locations[table_name] = [self.current_tablet_server_id]
            # update current_tablet_server_id
            self.current_tablet_server_id = (self.current_tablet_server_id + 1) % self.tablet_server_count

            return {
                        'hostname': hostname,
                        'port': port
                }, 200
    
    def delete_table(self, table_name):
        # table does not exist
        if table_name not in self.table_locations.keys():
            return '', 404
        
        # table in use
        if table_name in self.open_tables.keys():
            return '', 409
        
        has_404 = False
        has_409 = False

        # Success - table not in use
        for target_tablet_server_id in self.table_locations[table_name]:
            # send request to tablet server for deleting a table
            response = request_delete_table(
                table_name, 
                self.tablet_servers[target_tablet_server_id]['hostname'],
                self.tablet_servers[target_tablet_server_id]['port']
            )
            if response.status_code is 404:
                has_404 = True
                self.logger.debug(
                    'delete_table Failure - 404 returned from tablet server ' + str(target_tablet_server_id)
                )
                
            elif response.status_code is 409:
                has_409 = True
                self.logger.debug(
                    'delete_table Failure - 409 returned from tablet server ' + str(target_tablet_server_id)
                )

        if has_404 is True:
            return '', 404
        elif has_409 is True:
            return '', 409
        else:
            self.table_locations.pop(table_name, None)
            return '', 200
    
    def get_table_info(self, table_name):
        # table does not exist
        if not table_name in self.table_locations.keys():
            return '', 404

        tablets = []
        for target_tablet_server_id in self.table_locations[table_name]:
            hostname = self.tablet_servers[target_tablet_server_id]['hostname']
            port = self.tablet_servers[target_tablet_server_id]['port']

            # send request to tablet server for getting table info
            response = request_get_table_info(table_name, hostname, port)

            # not found on this tablet server
            if response.status_code is 404:
                continue
            # found on this tablet server
            else:
                response_dict = response.json()
                # row_from = response_dict['row_from']
                # row_to = response_dict['row_to']
                row_from = 'a'
                row_to = 'z'
                tablets.append(
                    {
                        "hostname": hostname,
                        "port": port,
                        "row_from": row_from,
                        "row_to": row_to
                    }
                )

        return {
                    'name': table_name, 
                    'tablets': tablets
                }, 200

    def open_table(self, table_name, args):
        args_dict = json.loads(args)
       
        # Table does not exist.
        if table_name not in self.table_locations.keys():
            return '', 404

        # Client already opened the table.
        if table_name in self.open_tables.keys() and \
            args_dict['client_id'] in self.open_tables[table_name]:
            return '', 400

        # Open success
        if table_name not in self.open_tables.keys():
            self.open_tables[table_name] = [args_dict['client_id']]
        else:
            self.open_tables[table_name].append(args_dict['client_id'])

        return '', 200

    def close_table(self, table_name, args):
        args_dict = json.loads(args)
        
        # Table does not exist.
        if table_name not in self.table_locations.keys():
            return '', 404

        # Client not opened the table.
        if table_name not in self.open_tables.keys():
            return '', 400

        if table_name in self.open_tables.keys() and \
            args_dict['client_id'] not in self.open_tables[table_name]:
            return '', 400
    
        # Close success
        self.open_tables[table_name].remove(args_dict['client_id'])

        if len(self.open_tables[table_name]) is 0:
            self.open_tables.pop(table_name, None)

        return '', 200
    
    def sharding_tables(self):
        pass
