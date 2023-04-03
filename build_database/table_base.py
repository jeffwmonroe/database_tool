from sqlalchemy import select, func
import sqlalchemy as sqla

ColumnCollection = sqla.sql.expression.ColumnCollection


def duplicate_row_query(engine: sqla.Engine, column: sqla.Column) -> int:
    subquery = select(column,
                      func.count(column).label("count")
                      ).group_by(column).subquery()
    stmt = select(subquery).filter(subquery.c.count > 1)
    with engine.connect() as connection:
        result = connection.execute(stmt)
        num_rows = result.rowcount
    return num_rows


def good_column(table: ColumnCollection, labels: list[str], types: list[str]) -> bool:
    # I realize that this is horrifying ...
    for label in labels:
        if label in table.keys():
            col = table[label]
            if str(col.type) in types:
                return True

    return False


def good_log_cols(table: ColumnCollection) -> bool:
    return (good_column(table, ['created_ts'], ["TIMESTAMP"]) and
            good_column(table, ['updated_ts'], ["TIMESTAMP"]) and
            good_column(table, ['created_by'], ["TEXT", "VARCHAR(200)"]) and
            good_column(table, ['updated_by'], ["TEXT", "VARCHAR(200)"]))


def one_to_one_data(engine: sqla.Engine,
                    map_table: sqla.Table,
                    id_label: str,
                    ext_id_label: str
                    ) -> tuple[int | None, int | None]:
    if id_label in map_table.c.keys():
        id_column = map_table.c[id_label]
    else:
        return None, None

    if ext_id_label in map_table.c.keys():
        ext_id_column = map_table.c[ext_id_label]
    else:
        return None, None

    duplicate_ext_ids = duplicate_row_query(engine, ext_id_column)
    duplicate_ids = duplicate_row_query(engine, id_column)

    return duplicate_ids, duplicate_ext_ids


class TableBase:
    def __init__(self, thing: str, table: sqla.Table):
        self.thing: str = thing
        self.table: sqla.Table = table

        self.num_rows: int = 0
        self.num_cols: int = 0

        self.good_id: bool = False
        self.good_name: bool = False
        self.good_log: bool = False

    def has_log(self) -> bool:
        return False

    def has_id(self) -> bool:
        return False

    def has_name(self) -> bool:
        return True

    def validate(self, engine: sqla.Engine, metadata: sqla.MetaData) -> None:
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

    def get_thing(self) -> str:
        return self.thing

    def get_action(self) -> str | None:
        return None

    def get_vendor(self) -> str | None:
        return None

    def get_duplicate_ids(self) -> int:
        return 0

    def get_duplicate_ext_ids(self) -> int:
        return 0

    def validation_data(self) -> list[dict[str, str | bool | int | None]]:
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

    def get_id_column(self) -> sqla.Table:
        name = self.thing + "_id"
        if name in self.table.c.keys():
            # print(f'found it: {name}')
            return self.table.c[name]
        raise ValueError('No name found in get_id_column', name)

    def get_name_column(self) -> sqla.Table:
        name = self.thing + "_name"
        if name in self.table.c.keys():
            return self.table.c[name]
        if self.thing in self.table.c.keys():
            return self.table.c[self.thing]
        raise ValueError('No name found in get_name_column', name)
