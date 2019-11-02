from helpers import *
import logging

class MasterServer:

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('MasterServer')
        self.logger.setLevel(logging.DEBUG)
        self.open_tables = {}  # {'table_name': 'client_id'}
        self.table_locations = {}  # {'table_name': tablet_server_id}
        self.tablet_servers = {} # {tablet_server_id: {'hostname': '', 'port': ''} }
        self.current_tablet_server_id = 0 # round robin next server

        # TODO: create_connection


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

        # exist
        if table_name in self.table_locations.keys():
            return '', 409

        hostname = self.tablet_servers[self.current_tablet_server_id]['hostname']
        port = self.tablet_servers[self.current_tablet_server_id]['port']

        # TODO: send request to tablet server
        create_status = request_create_table(
            args, 
            hostname,
            port
        )
       
        self.table_locations[table_name] = self.current_tablet_server_id
        # update current_tablet_server_id
        self.current_tablet_server_id = (self.current_tablet_server_id + 1) % TABLET_SERVER_COUNT

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
        
        # Success - table not in use
        target_tablet_server_id = table_location[table_name]

        # TODO: send request to tablet server
        destroy_status = request_delete_table(
            table_name, 
            self.tablet_servers[target_tablet_server_id]['hostname'],
            self.tablet_servers[target_tablet_server_id]['port']
        )

        if destroy_status is False:
            self.logger.debug(
                'delete_table Failure - destroy_status false from tablet server ' + str(target_tablet_server_id)
            )

        return '', 200
    
    def get_table_info(self, table_name):
        # table does not exist
        if not table_name in self.table_locations.keys:
            return '', 404

        target_tablet_server_id = self.table_locations[table_name]
        hostname = self.tablet_servers[target_tablet_server_id]['hostname']
        port = self.tablet_servers[target_tablet_server_id]['port']

        # TODO: send request to tablet server to get table info
        row_from, row_to = request_get_table_info(table_name, hostname, port)

        return {
                    'name': table_name, 
                    'tablets': [
                        {
                            "hostname": hostname,
                            "port": port,
                            "row_from": row_from,
                            "row_to": row_to
                        }
                    ]
                }, 200

    def open_table(self, table_name, args):
        # Table does not exist.
        if table.name not in self.table_locations.keys():
            return '', 404

        # Client already opened the table.
        if table_name in self.open_tables.keys():
            return '', 400

        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        # Open success
        self.open_tables[table_name] = args_dict['client_id']
        return '', 200

    def close_table(self, table_name, args):
        # Table does not exist.
        if table.name not in self.table_locations.keys():
            return '', 404

        # Client not opened the table.
        if table_name not in self.open_tables.keys():
            return '', 400

        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        if self.open_tables[table_name] is not args_dict['client_id']:
            return '', 400

        # Open success
        self.open_tables[table_name].pop(table_name, None)
        return '', 200