from utilities import good_column, one_to_one_data
from table_base import TableBase


class Action(TableBase):
    def __init__(self, thing, action, table):
        super().__init__(thing, table)
        self.action = action

        self.good_ext = False

    def has_name(self):
        return True

    def validate(self, engine, metadata):
        super().validate(engine, metadata)

        if self.action == 'map' or self.action == 'fuzzymatch':
            self.good_ext = good_column(self.table.c, ['ext'], ["TEXT", "VARCHAR(200)"])
        else:
            self.good_ext = None

    def table_name(self):
        return self.action + "_" + self.thing

    def get_action(self):
        return self.action


class VendorAction(Action):
    def __init__(self, thing, action, vendor, table):
        super().__init__(thing, action, table)
        self.vendor = vendor
        self.duplicate_ids = 0
        self.duplicate_ext_ids = 0

    def has_log(self):
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_id(self):
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_name(self):
        return self.action == 'import'

    def table_name(self):
        return self.action + "_" + self.thing + "_" + self.vendor

    def get_vendor(self):
        return self.vendor

    def get_duplicate_ids(self):
        return self.duplicate_ids

    def get_duplicate_ext_ids(self):
        return self.duplicate_ext_ids

    def validate(self, engine, metadata):
        super().validate(engine, metadata)
        if self.action == 'map' or self.action == 'fuzzymatch':
            self.duplicate_ids, self.duplicate_ext_ids = one_to_one_data(engine,
                                                                         self.table,
                                                                         self.thing + '_id',
                                                                         'ext_id')
