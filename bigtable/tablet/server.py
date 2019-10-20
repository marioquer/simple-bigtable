# from helpers import *
from bigtable.tablet.helpers import *

class TabletServer:
    # metadata should be stored as file
    metadata = None
    max_mem_row = 100
    table_objs = {}

    def __init__(self, *args, **kwargs):
        # do recovery
        if not os.path.isfile(server_metadata_path):
            self.metadata = {
                'max_mem_row': 100,
                'tables': {}
            }
            write_server_metadata(self.metadata)
        else:
            self.metadata = read_server_metadata()

    def list_tables(self):
        return {
                'tables': list(self.metadata['tables'].keys())
            }, 200

    def create_table(self, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        table_name = args_dict['name']
        # exist
        if table_name in self.metadata['tables']:
            return '', 409

        # TODO: create new Table obj and add to map

        # add to dict and save to disk
        self.metadata['tables'][table_name] = args_dict
        write_server_metadata(self.metadata)
        try:
            os.mkdir(folder_path + table_name)
        except OSError:
            print ("Creation of the directory %s failed" % path)        
        
        return '', 200

    def delete_table(self, table_name):
        # table not found
        if not table_name in self.metadata['tables']:
            return '', 404

        # delete from metadata
        self.metadata['tables'].pop(table_name, None)
        write_server_metadata(self.metadata)
        # TODO: delete from Table obj list

        # delete folder
        shutil.rmtree(folder_path + table_name) 
        return '', 200

        # 409 for checkpoint2

    def get_table_info(self, table_name):
        if not table_name in self.metadata['tables']:
            return '', 404
        return self.metadata['tables'][table_name], 200
        

    def retreive_row(self, table_name, row):
        pass

    def insert_cell(self, table_name, args):
        pass
    
    def retreive_cell(self, table_name, args):
        pass

    def retreive_cells(self, table_name, args):
        pass

    def set_memtable_max(self, max_value):
        pass


class Table:
    memtable = None
    memindex = None
    metadata = None
    name = None

    # metadata store commit log postion, sstable next name?

    def __init__(self, table_name):
        # restore memtable and memindex
        self.name = table_name
        pass