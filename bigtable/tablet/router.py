from flask import Flask, escape, request
from bigtable.tablet.server import *

app = Flask(__name__)
tablet_server = TabletServer()

'''
Table Operations
'''
@app.route('/api/tables', methods=['GET', 'POST', 'DELETE'])
def tables_ops():
    print(tablet_server.name)
    if request.method == 'POST':
        # Create Table
        return tablet_server.create_table(request.form)
    elif request.method == 'DELETE':
        # Delete Table
        return tablet_server.delete_table(request.args['pk'])
    else:
        # List Tables
        return tablet_server.list_tables()

'''
Get Table Info
'''
@app.route('/api/tables/<string:pk>', methods=['GET'])
def tables_info(pk):
    return {}



'''
Row Operations
'''
@app.route('/api/table/<string:pk>/row', methods=['GET'])
def row_retreive(pk):
    # Retrieve a row
    print('get')
    pass
    return {}


'''
Cell Opreations
'''
@app.route('/api/table/<string:pk>/cell', methods=['GET', 'POST'])
def cell_ops(pk):
    if request.method == 'POST':
        # Insert a cell
        print(request.form)
        pass
    else:
        # Retrieve a cell
        print(request.args)
        pass
    return {}


'''
Retrieve cells
'''
@app.route('/api/table/<string:pk>/cells', methods=['GET'])
def cells_retrieve(pk):
    print(request.args)
    return {}


'''
Set MemTable Max Entries
'''
@app.route('/api/memtable', methods=['POST'])
def max_entries():
    print(request.form)
    return {}