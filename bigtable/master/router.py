from flask import Flask, escape, request
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '../../..')))
print(sys.path)
from bigtable.master.server import *

app = Flask(__name__)
master_server = MasterServer()


'''
Register Tablet Server
'''
@app.route('/api/tabletservers', methods=['POST'])
def register_tablet_server():
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

'''
Decide sharding
'''
@app.route('/api/sharding', method=['POST'])
def sharding():
    # {'from': {'table_name': table_name, 'server_id': id}}
    # give a target server for sharding and record it
    target_host, target_port = master_server.get_sharding_target_server(request.data)
    return {
        'target_host': target_host,
        'target_port': target_port,
    }, 200


if __name__ == "__main__":
    app.run(host=sys.argv[1], port=sys.argv[2], threaded=False)