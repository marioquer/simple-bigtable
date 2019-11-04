import sys
import os
import heapq
import requests
import shutil
import logging
import json

sys.path.insert(0, os.path.abspath(os.path.join(__file__, '../../..')))
from bigtable.tablet import helpers

class TabletServer:
    # metadata should be stored as file
    metadata = None
    table_objs = {}
    tablet_server_id = None
    server_folder_path = ''
    server_metadata_path = ''

    def __init__(self, cmd_argv):
        # send tablet server info to master
        self.tablet_hostname = cmd_argv[1]
        self.tablet_port = cmd_argv[2]
        self.master_hostname = cmd_argv[3]
        self.master_port = cmd_argv[4]

        self.server_info = {
            'master_hostname': self.master_hostname,
            'master_port': self.master_port,
            'tablet_server_id': self.tablet_server_id
        }

        params = {
                'hostname': self.tablet_hostname, 
                'port': self.tablet_port
            }
            
        url = "http://" + self.master_hostname + ":" + self.master_port + "/api/tabletservers"
        response = requests.post(url, json=params)

        self.tablet_server_id = response.json()['tablet_server_id']
        self.server_folder_path = '{}{}'.format(helpers.FILE_FOLDER_PATH, 'server' + str(self.tablet_server_id))
        self.server_metadata_path = '{}/{}'.format(self.server_folder_path, 'metadata')

        # do recovery
        if not os.path.isfile(self.server_metadata_path):
            self.metadata = {
                'max_mem_row': 100,
                'tables': {}
            }
            try:
                os.makedirs(self.server_folder_path)
            except OSError:
                print ("Creation of the directory %s failed" % self.server_folder_path)
            helpers.write_server_metadata(self.server_metadata_path, self.metadata)
        else:
            self.restore_tablet_server()


    def restore_tablet_server(self):
        self.metadata = helpers.read_server_metadata(self.server_metadata_path)
        for key in self.metadata['tables'].keys():
            self.table_objs[key] = Tablet(self.server_folder_path, key, key + '0', self.master_info)
        return '', 200

    def check_tablet_server_status(self):
        print('tablet_server: check tablet server status')
        return '', 200

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
        helpers.write_server_metadata(self.server_metadata_path, self.metadata)
        try:
            tablet_dir_path = '{}/{}/{}'.format(self.server_folder_path, table_name, table_name + '0')
            os.makedirs(tablet_dir_path)
        except OSError:
            print ("Creation of the directory %s failed" % tablet_dir_path)        
        
        # create new Table obj and add to map TODO: tablet obj update
        self.table_objs[table_name] = Tablet(self.server_folder_path, table_name, table_name + '0', self.master_info)

        return '', 200

    def delete_table(self, table_name):
        # table not found
        if not table_name in self.metadata['tables']:
            return '', 404

        # delete from metadata
        self.metadata['tables'].pop(table_name, None)
        helpers.write_server_metadata(self.server_metadata_path, self.metadata)
        # delete from Table obj list
        self.table_objs.pop(table_name, None)

        # delete folder
        shutil.rmtree('{}/{}'.format(self.server_folder_path, table_name))
        return '', 200

        # 409 for checkpoint2

    def get_table_info(self, table_name):
        if not table_name in self.metadata['tables']:
            return '', 404

        return self.metadata['tables'][table_name], 200


    def get_tablets_info(self, table_name):
        if not table_name in self.metadata['tables']:
            return '', 404
        tablet = self.table_objs[table_name]
        return {
            'row_from': tablet.metadata['row_from'],
            'row_to': tablet.metadata['row_to']
        }, 200
    

    def insert_cell(self, table_name, args):
        # check existence of table
        if not table_name in self.table_objs.keys():
            return '', 404

        # check json format
        args_dict = {}
        if args != b'':
            try:
                args_dict = json.loads(args)
            except ValueError:
                return '', 400

        column_family = args_dict['column_family']
        column = args_dict['column']
        row = args_dict['row']
        data = args_dict['data']
        
        table = self.table_objs[table_name]

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                table.write_cell(args_dict, self.metadata['max_mem_row'])
                return '', 200

        # column family or column not found
        return '', 400
    
    def retrieve_cell(self, table_name, args):
        # check existence of table
        if not table_name in self.table_objs.keys():
            return '', 404

        args_dict = {}
        if args != b'':
            try:
                args_dict = json.loads(args)
            except ValueError:
                return '', 400

        column_family = args_dict['column_family']
        column = args_dict['column']
        row = args_dict['row']

        table = self.table_objs[table_name]

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                return table.get_a_cell(row, column_family, column), 200

        # column family or column not found
        return '', 400


    def retrieve_cells(self, table_name, args):
         # check existence of table
        if not table_name in self.table_objs.keys():
            return '', 404

        args_dict = {}
        if args != b'':
            try:
                args_dict = json.loads(args)
            except ValueError:
                return '', 400


        column_family = args_dict['column_family']
        column = args_dict['column']
        row_from = args_dict['row_from']
        row_to = args_dict['row_to']

        table = self.table_objs[table_name]

        # check existence of column family and column
        table_structure = self.metadata['tables'][table_name]
        for cf in table_structure['column_families']:
            if cf['column_family_key'] == column_family and column in cf['columns']:
                return table.get_cells(row_from, row_to, column_family, column), 200

        # column family or column not found
        return '', 400
        
        
    def retrieve_row(self, table_name, row):
        pass

    def set_memtable_max(self, args):
        
        args_dict = {}
        if args != b'':
            try:
                args_dict = json.loads(args)
            except ValueError:
                return '', 400

        new_max_value = args_dict['memtable_max']

        if not isinstance(new_max_value, int):
            return '', 400

        # update max_mem_row of metadata
        self.metadata['max_mem_row'] = new_max_value
        # update server metadata in the disk
        helpers.write_server_metadata(self.server_metadata_path, self.metadata)

        # spill memtable
        for table in self.metadata['tables'].keys():
            self.table_objs[table].do_memtable_spill(new_max_value)

        return '', 200

    def receive_shard(self, args):
        table_name = args['table_name']
        # TODO: update table_obj
        # TODO: save sstable, restore ssindex and memindex, update metadata
        return '', 200



class Tablet:
    memtable = None
    memindex = None
    mem_rowkey_set = None
    metadata = None
    table_name = ''
    tablet_name = ''
    mem_unique_id = 0
    tablet_max_limit = 1000
    tablet_folder_path = ''
    server_folder_path = ''
    

    def __init__(self, server_folder_path, table_name, tablet_name, server_info):
        self.mem_rowkey_set = set()
        self.table_name = table_name
        self.tablet_name = tablet_name
        self.server_folder_path = server_folder_path
        self.tablet_folder_path = '{}/{}/{}'.format(server_folder_path, table_name, tablet_name)
        self.server_info = server_info
        
        wal = helpers.read_dict_from_table_file(server_folder_path, table_name, tablet_name, 'wal')
        if wal != None:
            # restore
            self.metadata = helpers.read_dict_from_table_file(server_folder_path, table_name, tablet_name, 'metadata')
            start_index = self.metadata['uncommited_wal_line']
            self.memtable = []
            for data in wal[start_index: ]:
                self.mem_rowkey_set.add(data['row'])
                heapq.heappush(self.memtable, (data['row'], self.mem_unique_id, data))
                self.mem_unique_id += 1
            self.memindex = helpers.read_dict_from_table_file(server_folder_path, table_name, tablet_name, 'ssindex')
        else:
            # new
            self.metadata = {
                'next_sstable_index': 0,
                'uncommited_wal_line': 0,
                'rowkey_set': [],
                'row_from': '',
                'row_to': ''
            }
            self.memtable = []
            self.memindex = {}
            helpers.write_dict_to_table_file(server_folder_path, table_name, tablet_name, 'metadata', self.metadata)
            helpers.write_dict_to_table_file(server_folder_path, table_name, tablet_name, 'ssindex', {})
            helpers.write_dict_to_table_file(server_folder_path, table_name, tablet_name, 'wal', [])


    def write_cell(self, args_dict, max_mem_row):
        # do insert
        # 1. write wal
        # 2. append memtable and even do spilling
        row_key = args_dict['row']
        helpers.write_table_wal(self.server_folder_path, self.table_name, self.tablet_name, args_dict)
        heapq.heappush(self.memtable, (row_key, self.mem_unique_id, args_dict))
        self.mem_unique_id += 1
        self.mem_rowkey_set.add(row_key)
        self.metadata['rowkey_set'].append(row_key)
        row_from = self.metadata['row_from']
        row_to = self.metadata['row_to']
        if row_from == '' or row_to == '':
            row_from = row_key
            row_to = row_from
        else:
            if row_key > row_to:
                row_to = row_key
            elif row_key < row_from:
                row_from = row_key
        self.metadata['row_from'] = row_from
        self.metadata['row_to'] = row_to
        helpers.write_dict_to_table_file(self.server_folder_path, self.table_name, self.tablet_name, 'metadata', self.metadata)
        self.do_memtable_spill(max_mem_row)
        self.do_sharding()


    def get_a_cell(self, rowkey, column_family, column):
        sorted_memtable = heapq.nsmallest(len(self.memtable), self.memtable)

        rowlist = []
        # binary search in memtable
        mem_start = self.binary_search_first_index(sorted_memtable, rowkey)
        mem_end = self.binary_search_last_index(sorted_memtable, rowkey)
        if mem_start != -1 and mem_end != -1:
            rowlist.extend(sorted_memtable[mem_start: mem_end + 1])

        # binary search in sstable
        cell_sstables = self.memindex.get(rowkey, [])
        for s in cell_sstables:
            ssdata = helpers.read_dict_from_table_file(self.server_folder_path, self.table_name, self.tablet_name, s)
            ss_start = self.binary_search_first_index(ssdata, rowkey)
            ss_end = self.binary_search_last_index(ssdata, rowkey)
            rowlist.extend(ssdata[ss_start: ss_end + 1])

        # construct result set without timestamp duplication
        resultset = set()
        for row in rowlist:
            row_data = row[2]
            if row_data['column_family'] == column_family and row_data['column'] == column:
                for val in row_data['data']:
                    resultset.add((val['time'], val['value']))
        
        # get latest 5 versions
        resultheap = []
        for val in resultset:
            heapq.heappush(resultheap, (val[0], {'time': val[0], 'value': val[1]}))
        result = []
        for t in heapq.nlargest(5, resultheap):
            result.append(t[1])
        
        return {
            'row': rowkey,
            'data': result
        }

    def get_cells(self, row_from, row_to, column_family, column):
        sorted_memtable = heapq.nsmallest(len(self.memtable), self.memtable)

        rowlist = []
        # binary search in memtable
        mem_start = self.binary_search_first_index(sorted_memtable, row_from, True)
        mem_end = self.binary_search_last_index(sorted_memtable, row_to, True)
        if mem_start != -1 and mem_end != -1:
            rowlist.extend(sorted_memtable[mem_start: mem_end + 1])

        # binary search in sstable
        for i in range(self.metadata['next_sstable_index']):
            ssdata = helpers.read_dict_from_table_file(self.server_folder_path, self.table_name, self.tablet_name, 'sstable{}'.format(i))
            ss_start = self.binary_search_first_index(ssdata, row_from, True)
            ss_end = self.binary_search_last_index(ssdata, row_to, True)
            if ss_start != -1 and ss_end != -1:
                rowlist.extend(ssdata[ss_start: ss_end + 1])

        # get all needed cells
        resultset_map = {}
        for row in rowlist:
            row_data = row[2]
            if row_data['column_family'] == column_family and row_data['column'] == column:
                for val in row_data['data']:
                    temp = resultset_map.get(row_data['row'], set()) 
                    temp.add((val['time'], val['value']))
                    # heappush(temp, (val['time'], val))
                    resultset_map[row_data['row']] = temp

        # shuffle into right row bucket and return
        result_row_list = []
        for key in resultset_map.keys():
            # get lastest 5 versions of each cell
            resultheap = []
            for val in resultset_map[key]:
                heapq.heappush(resultheap, (val[0], {'time': val[0], 'value': val[1]}))

            data_list = []
            for t in heapq.nlargest(5, resultheap):
                data_list.append(t[1])
            result_row_list.append({
                'row': key,
                'data': data_list
            })

        return {
            'rows': result_row_list
        }


    def do_memtable_spill(self, max_mem_row):
        mem_rowkey_set_len = len(self.mem_rowkey_set)
        if mem_rowkey_set_len > max_mem_row:
            # split
            memtable_len = len(self.memtable)
            sstable_data = self.memtable[0: -1]
            self.memtable = [self.memtable[-1]]
            # clear rowkey set
            self.mem_rowkey_set.clear()
            self.mem_rowkey_set.add(self.memtable[0][0])
            # store sstable with sorted key
            sstable_name = 'sstable{}'.format(self.metadata['next_sstable_index'])
            helpers.write_dict_to_table_file(self.server_folder_path, self.table_name, self.tablet_name, sstable_name, heapq.nsmallest(len(sstable_data), sstable_data))
            self.metadata['next_sstable_index'] += 1
            # construct in-memory index
            for t in sstable_data:
                key_list = self.memindex.get(t[0], [])
                key_list.append(sstable_name)
                self.memindex[t[0]] = key_list
            # store ssindex
            helpers.write_dict_to_table_file(self.server_folder_path, self.table_name, self.tablet_name, 'ssindex', self.memindex)
            # mark committed in wal
            self.metadata['uncommited_wal_line'] += (memtable_len - 1)
            helpers.write_dict_to_table_file(self.server_folder_path, self.table_name, self.tablet_name, 'metadata', self.metadata)


    def do_sharding(self):
        row_set = set(self.metadata['rowkey_set'])
        row_set_len = len(row_set)
        if row_set_len == self.tablet_max_limit:
            # TODO: do sharding
            request_sharding_url = "http://{}:{}/api/sharding".format(self.server_info['master_hostname'], self.server_info['master_port'])
            data = {
                'table_name': self.table_name,
                'server_id': self.server_info['tablet_server_id']
            }
            response_dict = requests.post(request_sharding_url, json=data).json()

            # TODO: get data from sstable
            ssdata = {}
            sharding_url = "http://{}:{}/api/sendshard".format(response_dict['target_host'], response_dict['target_port'])
            response = requests.post(sharding_url, json=ssdata)
            # TODO: update metadata
            



    def binary_search_first_index(self, input_list, rowkey, larger=False):
        start = 0
        end = len(input_list) - 1
        mid = (start + end) // 2
        index = -1

        while (start <= end):
            if rowkey < input_list[mid][0]:
                if larger:
                    index = mid
                end = mid - 1
            elif rowkey == input_list[mid][0]:
                end = mid - 1
                index = mid
            else:
                start = mid + 1
            mid = (start + end) // 2
        return index


    def binary_search_last_index(self, input_list, rowkey, smaller=False):
        start = 0
        end = len(input_list) - 1
        mid = (start + end) // 2
        index = -1

        while (start <= end):
            if rowkey < input_list[mid][0]:
                end = mid - 1
            elif rowkey == input_list[mid][0]:
                start = mid + 1
                index = mid
            else:
                if smaller:
                    index = mid
                start = mid + 1
            mid = (start + end) // 2
        return index