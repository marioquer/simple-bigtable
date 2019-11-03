from flask import Flask, escape, request
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(__file__, '../../..')))
from bigtable.tablet import server


app = Flask(__name__)
tablet_server = server.TabletServer(sys.argv)

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
def row_retrieve(pk):
    # Retrieve a row
    return tablet_server.retrieve_row(pk, request.form['row'])


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
        return tablet_server.retrieve_cell(pk, request.data)


'''
Retrieve cells
'''
@app.route('/api/table/<string:pk>/cells', methods=['GET'])
def cells_retrieve(pk):
    return tablet_server.retrieve_cells(pk, request.data)


'''
Set MemTable Max Entries
'''
@app.route('/api/memtable', methods=['POST'])
def max_entries():
    return tablet_server.set_memtable_max(request.data)


if __name__ == "__main__":
    app.run(host=sys.argv[1], port=sys.argv[2], threaded=False)