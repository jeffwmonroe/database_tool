from sqlalchemy import select
from utilities import good_log_cols, good_column


class TableBase:
    def __init__(self, thing, table):
        self.thing = thing
        self.table = table

        self.num_rows = 0
        self.num_cols = 0

        self.good_id = False
        self.good_name = False
        self.good_log = False

    def has_log(self) -> bool:
        return False

    def has_id(self) -> bool:
        return False

    def has_name(self) -> bool:
        return True

    def validate(self, engine, metadata) -> None:
        # print(validate: {self.thing}')
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount
            # print(f'    rows = {self.num_rows}')
        if self.has_name():
            self.good_name = good_column(self.table.c, [self.thing + '_name', self.thing], ["TEXT", "VARCHAR(200)"])
        else:
            self.good_name = None
        if self.has_id():
            self.good_id = good_column(self.table.c, [self.thing + '_id'], ["INTEGER", "BIGINT"])
        else:
            self.good_id = None
        if self.has_log():
            self.good_log = good_log_cols(self.table.c)
        else:
            self.good_log = None
        self.num_cols = len(self.table.c.keys())

    def table_name(self) -> str:
        return "none"

    def get_thing(self):
        return self.thing

    def get_action(self):
        return None

    def get_vendor(self):
        return None

    def get_duplicate_ids(self) -> int:
        return 0

    def get_duplicate_ext_ids(self) -> int:
        return 0

    def validation_data(self):
        return [{
            'table_name': self.table_name(),
            'thing': self.get_thing(),
            'action': self.get_action(),
            'vendor': self.get_vendor(),
            'good_id': self.good_id,
            'good_name': self.good_name,
            'good_log': self.good_log,
            'columns': self.num_cols,
            'rows': self.num_rows,
            'dup id': self.get_duplicate_ids(),
            'dup ext_id': self.get_duplicate_ext_ids(),
        }]

    def get_id_column(self):
        name = self.thing + "_id"
        if name in self.table.c.keys():
            # print(f'found it: {name}')
            return self.table.c[name]
        raise ValueError('No name found in get_id_column', name)

    def get_name_column(self):
        name = self.thing + "_name"
        if name in self.table.c.keys():
            return self.table.c[name]
        if self.thing in self.table.c.keys():
            return self.table.c[self.thing]
        raise ValueError('No name found in get_name_column', name)
