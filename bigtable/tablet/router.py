from flask import Flask, escape, request
# from server import *
from bigtable.tablet.server import *


app = Flask(__name__)
tablet_server = TabletServer()

'''
Table Operations
'''
@app.route('/api/tables', methods=['GET', 'POST'])
def tables_ops():
    if request.method == 'POST':
        # Create Table
        return tablet_server.create_table(request.data)
    else:
        # List Tables
        return tablet_server.list_tables()

'''
Table destroy
'''
@app.route('/api/tables/<string:pk>', methods=['DELETE'])
def tables_destroy(pk):
    return tablet_server.delete_table(pk)


'''
Get Table Info
'''
@app.route('/api/tables/<string:pk>', methods=['GET'])
def tables_info(pk):
    return tablet_server.get_table_info(pk)


'''
Row Operations
'''
@app.route('/api/table/<string:pk>/row', methods=['GET'])
def row_retreive(pk):
    # Retrieve a row
    return tablet_server.retreive_row(pk, request.form['row'])


'''
Cell Opreations
'''
@app.route('/api/table/<string:pk>/cell', methods=['GET', 'POST'])
def cell_ops(pk):
    if request.method == 'POST':
        # Insert a cell
        return tablet_server.insert_cell(pk, request.data)
    else:
        # Retrieve a cell
        return tablet_server.retreive_cell(pk, request.data)


'''
Retrieve cells
'''
@app.route('/api/table/<string:pk>/cells', methods=['GET'])
def cells_retrieve(pk):
    return tablet_server.retreive_cells(pk, request.form)


'''
Set MemTable Max Entries
'''
@app.route('/api/memtable', methods=['POST'])
def max_entries():
    return tablet_server.set_memtable_max(request.form['memtable_max'])


if __name__ == "__main__":
    app.run(host='localhost', port='8000', threaded=False)