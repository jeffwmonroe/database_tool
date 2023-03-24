from sqlalchemy import select, func
from utilities import good_log_cols, good_column


class Action:
    def __init__(self, thing, action, table):
        self.thing = thing
        self.action = action
        self.table = table

        self.num_rows = 0
        self.num_cols = 0

        self.good_name = False

        self.good_log = False
        
        self.good_ext = False

    # ToDo list all validations
    #     number of rows
    #     number of columns: this is for data / no data
    #     4 log columns: name and type

    # ToDo MAP Validations
    #     One to one / one to many   -   only for maps
    #     id name (id)
    #     thing_id name
    #     ext_id existence and type
    def validate(self, engine, metadata):
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount
        self.good_name = good_column(self.table.c, [self.thing + '_name', self.thing], ["TEXT", "VARCHAR(200)"])
        self.num_cols = len(self.table.c.keys())
        if self.action == 'map' or self.action == 'fuzzymatch':
            self.good_log = good_log_cols(self.table.c)

        self.good_ext = good_column(self.table.c, ['ext'], ["TEXT", "VARCHAR(200)"])


class VendorAction(Action):
    def __init__(self, thing, action, vendor, table):
        super().__init__(thing, action, table)
        self.vendor = vendor

    def validate(self, engine, metadata):
        super().validate(engine, metadata)
        pass
