# from helpers import *
from bigtable.tablet.helpers import *

class TabletServer:
    # metadata should be stored as file
    metadata = None
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
            # restore all table objs
            for key in self.metadata['tables'].keys():
                self.table_objs[key] = Table(key)

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

        # add to dict and save to disk
        self.metadata['tables'][table_name] = args_dict
        write_server_metadata(self.metadata)
        try:
            os.mkdir(folder_path + table_name)
        except OSError:
            print ("Creation of the directory %s failed" % path)        
        
        # create new Table obj and add to map
        self.table_objs[table_name] = Table(table_name)

        return '', 200

    def delete_table(self, table_name):
        # table not found
        if not table_name in self.metadata['tables']:
            return '', 404

        # delete from metadata
        self.metadata['tables'].pop(table_name, None)
        write_server_metadata(self.metadata)
        # delete from Table obj list
        self.table_objs.pop(table_name, None)

        # delete folder
        shutil.rmtree(folder_path + table_name) 
        return '', 200

        # 409 for checkpoint2

    def get_table_info(self, table_name):
        if not table_name in self.metadata['tables']:
            return '', 404
        return self.metadata['tables'][table_name], 200

    def insert_cell(self, table_name, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        column_family = args_dict['column_family']
        column = args_dict['column']
        row = args_dict['row']
        data = args_dict['data']

        # check existence of table
        if not table_name in self.table_objs:
            return '', 404
        
        table = self.table_objs[table_name]

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                table.write_cell(args_dict, self.metadata['max_mem_row'])
                return '', 200

        # column family or column not found
        return '', 400
    
    def retreive_cell(self, table_name, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        column_family = args_dict['column_family']
        column = args_dict['column']
        row = args_dict['row']

        # check existence of table
        if not table_name in self.table_objs:
            return '', 404

        table = self.table_objs[table_name]

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                return table.get_a_cell(row, column_family, column), 200

        # column family or column not found
        return '', 400


    def retreive_cells(self, table_name, args):
        try:
            args_dict = json.loads(args)
        except ValueError:
            return '', 400

        # check existence of table
        if not table_name in self.table_objs:
            return '', 404

        column_family = args_dict['column_family']
        column = args_dict['column']
        row_from = args_dict['row_from']
        row_to = args_dict['row_to']

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                return table.get_cells(row_from, row_to, column_family, column), 200

        # column family or column not found
        return '', 400
        
        

    def retreive_row(self, table_name, row):
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
        # TODO: restore memtable and memindex
        self.name = table_name
        
        # new
        self.metadata = {
            'next_sstable_index': 0,
            'uncommited_wal_line': 0
        }
        self.memtable = []
        self.memindex = {}
        write_dict_to_table_file(table_name, 'metadata', self.metadata)
        write_dict_to_table_file(table_name, 'ssindex', {})
        write_dict_to_table_file(table_name, 'wal', [])


    def write_cell(self, args_dict, max_mem_row):
        # do insert
        # 1. write wal
        # 2. append memtable and even do spilling
        write_table_wal(self.name, args_dict)
        heappush(self.memtable, (args_dict['row'], args_dict))
        self.do_memtable_spill(max_mem_row)


    def get_a_cell(self, rowkey, column_family, column):
        sorted_memtable = nsmallest(len(self.memtable), self.memtable)

        rowlist = []
        # binary search in memtable
        mem_start = self.binary_search_first_index(sorted_memtable, rowkey)
        mem_end = self.binary_search_last_index(sorted_memtable, rowkey)
        if mem_start != -1 and mem_end != -1:
            rowlist.extend(sorted_memtable[mem_start, mem_end + 1])

        # binary search in sstable
        cell_sstables = self.memindex.get(rowkey, [])
        for s in cell_sstables:
            ssdata = read_dict_from_table_file(self.name, s)
            ss_start = self.binary_search_first_index(ssdata, rowkey)
            ss_end = self.binary_search_last_index(ssdata, rowkey)
            rowlist.extend(ssdata[ss_start, ss_end + 1])

        # construct result list
        resultset = []
        for row in rowlist:
            row_data = row[1]
            if row_data['column_family'] == column_family and row_data['column'] == column:
                for val in row_data['data']:
                    heappush(resultset, (val['time'], val))
        result = []
        for t in nlargest(5, resultset):
            result.append(t[1])
        
        return result


    def get_cells(self, row_from, row_to, column_family, column):
        sorted_memtable = nsmallest(len(self.memtable), self.memtable)

        


    def do_memtable_spill(self, max_mem_row):
        memtable_len = len(self.memtable)
        if memtable_len > max_mem_row:
            # split
            sstable_data = self.memtable[0: max_mem_row]
            self.memtable = self.memtable[max_mem_row:]
            # store sstable with sorted key
            sstable_name = 'sstable{}'.format(self.metadata['next_sstable_index'])
            write_dict_to_table_file(self.name, sstable_name, nsmallest(max_mem_row, sstable_data))
            self.metadata['next_sstable_index'] += 1
            # construct in-memory index
            for t in sstable_data:
                key_list = self.memindex.get(t[0], [])
                key_list.append(sstable_name)
                self.memindex[t[0]] = key_list
            # store ssindex
            write_dict_to_table_file(self.name, 'ssindex', self.memindex)
            # mark committed in wal
            self.metadata['uncommited_wal_line'] += max_mem_row


    def binary_search_first_index(self, input_list, rowkey):
        start = 0
        end = len(input_list) - 1
        mid = (start + end) / 2
        index = -1

        while (start <= end):
            if rowkey < input_list[mid][0]:
                end = mid - 1
            elif rowkey == input_list[mid][0]:
                end = mid - 1
                index = mid
            else:
                start = mid + 1
            mid = (start + end) / 2

        return index


    def binary_search_last_index(self, input_list, rowkey):
        start = 0
        end = len(input_list) - 1
        mid = (start + end) / 2
        index = -1

        while (start <= end):
            if rowkey < input_list[mid][0]:
                end = mid - 1
            elif rowkey == input_list[mid][0]:
                end = mid + 1
                index = mid
            else:
                start = mid + 1
            mid = (start + end) / 2

        return index