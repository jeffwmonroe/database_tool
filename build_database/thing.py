from collections import namedtuple
from action import Action, VendorAction
import sqlalchemy as sqla
from sqlalchemy import select, func

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
        print(f'   action added: {self.thing} - {action}')

    def add_vendor_action(self, vendor, action, table):
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(action, vendor, table)
        print(f'    vendor action added: {self.thing} - {key}')

    # ToDo list all validations
    #     number of rows
    #     number of columns: this is for data / no data
    #     4 log columns: name and type

    # ToDo MAP Validations
    #     One to one / one to many   -   only for maps
    #     id name (id)
    #     thing_id name
    #     ext_id existence and type

    def good_column(self, labels, types):
        # I realize that this is horrifying ...
        for label in labels:
            if label in self.table.c.keys():
                col = self.table.c[label]
                # print(f'    column: {label}, {col.type}')
                if str(col.type) in types:
                    return True
                # else:
                #     print(f'    not found!! {col.type} in {types}')
                #     print(str(col.type))

        return False

    def good_log_cols(self):
        return (self.good_column(['created_ts'], ["TIMESTAMP"]) and
                self.good_column(['updated_ts'], ["TIMESTAMP"]) and
                self.good_column(['created_by'], ["TEXT", "VARCHAR(200)"]) and
                self.good_column(['updated_by'], ["TEXT", "VARCHAR(200)"]))

    def validate(self, engine, metadata):
        # print(f'validate: {self.thing}')
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount
            # print(f'    rows = {self.num_rows}')

        self.good_id = self.good_column([self.thing + '_id'], ["INTEGER"])
        self.good_name = self.good_column([self.thing + '_name', self.thing], ["TEXT", "VARCHAR(200)"])

        self.good_log = self.good_log_cols()

        self.num_cols = len(self.table.c.keys())
        # if 'created_ts' in self.table.c.keys
        # print(f'    num_cols = {self.num_cols}')
        # print(f'    good id = {self.good_id}')
        # print(f'    good log = {self.good_log}')
        # print(f'    good id name = {self.good_id_name}')
        for col in self.table.c.keys():
            # print(f'    col = {col} - {self.table.c[col]}')
            # print(type(col))
            pass
        # self.print_validation()

    def print_validation(self):
        print(f'Table: {self.thing}', end=' ')
        print(f'<id={self.good_id}, name={self.good_name}, log={self.good_log}>', end=" ")
        print(f'columns = {self.num_cols}, rows = {self.num_rows}')

