from helpers import *
import logging

class MasterServer:
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('MasterServer')
        self.logger.setLevel(logging.DEBUG)
        self.open_tables = {}  # {'table_name': 'client_id'}
        self.table_locations = {}  # {'table_name': [tablet_server_id]}
        self.current_tablet_server_id = 0 # round robin: next server
        self.tablet_server_count = 0 # updated when a new tablet server integrated

        # tablet servers info: {tablet_server_id: {'hostname': '', 'port': ''} }
        self.tablet_servers = {}

    def get_tablet_server_info(self, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        hostname = args_dict['hostname']
        port = args_dict['port']

        # update tablet server info
        self.tablet_servers[self.tablet_server_count] = {
            'hostname': hostname,
            'port': port
        }

        # update tablet_server_count
        self.tablet_server_count += 1
        
        return '', 200

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

        hostname = self.tablet_servers[self.current_tablet_server_id]['hostname']
        port = self.tablet_servers[self.current_tablet_server_id]['port']

        # send request to tablet server for creating a new table
        response = request_create_table(
            args, 
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

        # send request to tablet server for deleting a table
        response = request_delete_table(
            table_name, 
            self.tablet_servers[target_tablet_server_id]['hostname'],
            self.tablet_servers[target_tablet_server_id]['port']
        )

        if response.status_code is 404:
            self.logger.debug(
                'delete_table Failure - 404 returned from tablet server ' + str(target_tablet_server_id)
            )
            return '', 404
        elif response.status_code is 409:
            self.logger.debug(
                'delete_table Failure - 409 returned from tablet server ' + str(target_tablet_server_id)
            )
            return '', 409
        else:
            return '', 200
    
    def get_table_info(self, table_name):
        # table does not exist
        if not table_name in self.table_locations.keys:
            return '', 404

        for target_tablet_server_id in self.table_locations[table_name]:
            hostname = self.tablet_servers[target_tablet_server_id]['hostname']
            port = self.tablet_servers[target_tablet_server_id]['port']

            # send request to tablet server for getting table info
            response = request_get_table_info(table_name, hostname, port)

            # not found on this tablet server
            if reponse.status_code is 404:
                continue
            # found on this tablet server
            else:
                response_dict = response.json()
                break

        # TODO: modify table info in TabletServer
        row_from = response_dict['row_from']
        row_to = response_dict['row_to']

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
    
    def sharding_tables(self):
        pass
