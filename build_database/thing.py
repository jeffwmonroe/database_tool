from collections import namedtuple
from action import Action, VendorAction
from sqlalchemy import select
from utilities import good_column, good_log_cols

ActionKey = namedtuple("ThingAction", "vendor action")


class Thing:

    def __init__(self, thing, table):
        self.thing = thing
        self.table = table
        self.actions = {}
        self.vendor_actions = {}

        self.num_rows = 0
        self.num_cols = 0
        self.good_id = False
        self.good_log = False
        self.good_name = False

    def add_action(self, action, table):
        self.actions[action] = Action(action, table)
        # print(f'   action added: {self.thing} - {action}')

    def add_vendor_action(self, vendor, action, table):
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(action, vendor, table)
        # print(f'    vendor action added: {self.thing} - {key}')

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
        # print(validate: {self.thing}')
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount
            # print(f'    rows = {self.num_rows}')

        self.good_id = good_column(self.table.c, [self.thing + '_id'], ["INTEGER"])
        self.good_name = good_column(self.table.c, [self.thing + '_name', self.thing], ["TEXT", "VARCHAR(200)"])

        self.good_log = good_log_cols(self.table.c)

        self.num_cols = len(self.table.c.keys())
        # if 'created_ts' in self.table.c.keys
        # print(f'    num_cols = {self.num_cols}')
        # print(f'    good id = {self.good_id}')
        # print(f'    good log = {self.good_log}')
        # print(f'    good id name = {self.good_id_name}')
        # self.print_validation()
        for action in self.actions:
            self.actions[action].validate(engine, metadata)
        for vendor_action in self.vendor_actions:
            self.vendor_actions[vendor_action].validate(engine, metadata)

    def print_validation(self):
        print(f'Table: {self.thing}', end=' ')
        print(f'<id={self.good_id}, name={self.good_name}, log={self.good_log}>', end=" ")
        print(f'columns = {self.num_cols}, rows = {self.num_rows}')
