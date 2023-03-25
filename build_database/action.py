from utilities import good_column
from table_base import TableBase


class Action(TableBase):
    def __init__(self, thing, action, table):
        super().__init__(thing, table)
        self.action = action

        self.good_ext = False

    def has_name(self):
        return True

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
        super().validate(engine, metadata)

        if self.action == 'map' or self.action == 'fuzzymatch':
            self.good_ext = good_column(self.table.c, ['ext'], ["TEXT", "VARCHAR(200)"])
        else:
            self.good_ext = None

    def table_name(self):
        return self.action + "_" + self.thing


class VendorAction(Action):
    def __init__(self, thing, action, vendor, table):
        super().__init__(thing, action, table)
        self.vendor = vendor

    def has_log(self):
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_id(self):
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_name(self):
        return self.action == 'map' or self.action == 'fuzzymatch'

    def table_name(self):
        return self.action + "_" + self.thing + "_" + self.vendor
