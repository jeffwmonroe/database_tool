import sqlalchemy as sqla
from database_tools.jason_schema.json_schema import JsonDataTable, JsonColumn


class OntologyColumn:
    def __init__(self, column: JsonColumn) -> None:
        self.name: str = column.name
        self.data_type: str = column.data_type

    # foreign_table: str
    # foreign_column: str


def get_column(sql_table: sqla.Table, name_list: list[str]) -> sqla.Column:
    for name in name_list:
        if name in sql_table.c.keys():
            return sql_table.c[name]
    print(f'list of column names: {sql_table.c.keys()}')
    raise ValueError(f'column not found in in _get_column', name_list)


def get_id_column(sql_table: sqla.Table) -> sqla.Column:
    return get_column(sql_table, [sql_table.name])


class OntologyTable:
    def __init__(self, table: JsonDataTable, sql_table: sqla.Table) -> None:
        self.sql_table = sql_table

        self.name: str = table.name
        self.vendors: list[str] = table.vendors

        self.columns: list[OntologyColumn] = [OntologyColumn(col) for col in table.columns]

    def get_id_column(self) -> sqla.Column:
        return get_column(self.sql_table, [self.name + "_id"])

    def get_name_column(self) -> sqla.Column:
        return get_column(self.sql_table, [self.name + "_name", self.name])

    def pprint(self) -> None:
        print(f'build table: {self.name}')
        for column in self.columns:
            print(f'    column: ({column.name}, {column.data_type})')
