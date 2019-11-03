from flask import Flask, escape, request
from server import *
import sys

app = Flask(__name__)
master_server = MasterServer()


'''
Table Operations
'''
@app.route('/api/tabletservers', methods=['POST'])
def get_tablet_server_info():
    # register tablet server
    return master_server.register_tablet_server(request.data)


'''
Table Operations
'''
@app.route('/api/tables', methods=['GET', 'POST'])
def tables_ops():
    if request.method == 'POST':
        # Create Table
        return master_server.create_table(request.data)
    else:
        # List Tables
        return master_server.list_tables()

'''
Table destroy
'''
@app.route('/api/tables/<string:pk>', methods=['DELETE'])
def tables_destroy(pk):
    return master_server.delete_table(pk)

'''
Get Table Info
'''
@app.route('/api/tables/<string:pk>', methods=['GET'])
def tables_info(pk):
    return master_server.get_table_info(pk)

'''
Allow a client to begin using an exisiting table
'''
@app.route('/api/lock/<string:pk>', methods=['POST'])
def open_table(pk):
    return master_server.open_table(pk, request.data)

'''
Relinquishes a table
'''
@app.route('/api/lock/<string:pk>', methods=['DELETE'])
def close_table(pk):
    return master_server.close_table(pk, request.data)

if __name__ == "__main__":
    app.run(host=sys.argv[1], port=sys.argv[2], threaded=False)