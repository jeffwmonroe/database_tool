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

    def has_log(self):
        return False

    def has_id(self):
        return False

    def has_name(self):
        return True

    def validate(self, engine, metadata):
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
            self.good_id = good_column(self.table.c, [self.thing + '_id'], ["INTEGER"])
        else:
            self.good_id = None

        if self.has_log():
            self.good_log = good_log_cols(self.table.c)
        else:
            self.good_log = None

        self.num_cols = len(self.table.c.keys())

    def table_name(self):
        return "none"

    def print_validation(self):
        name = self.table_name()
        print(f'Table: {name}', end=' ')
        print(f'<id={self.good_id}, name={self.good_name}, log={self.good_log}>', end=" ")
        print(f'columns = {self.num_cols}, rows = {self.num_rows}')
