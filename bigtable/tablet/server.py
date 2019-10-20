from bigtable.tablet.helpers import *

class TabletServer:
    def __init__(self, *args, **kwargs):
        self.name = 'mario'

    def list_tables(self):
        return {}, 200

    def create_table(self, args):
        return {'a': 'd'}, 200

    def delete_table(self, pk):
        return {}, 200