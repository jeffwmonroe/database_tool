import sqlalchemy as sqla

from database_tools.json_schema.json_schema import JsonColumn, JsonDataTable
from database_tools.database_connection.utilities import (
    build_data_table,
    build_sql_column,
)


class OntologyColumn:
    def __init__(self, column: JsonColumn) -> None:
        self.name: str = column.name
        self.data_type: str = column.data_type
        self.foreign_table: str | None = column.foreign_table

        self.sql_column: sqla.Column = build_sql_column(column.name, column.data_type)

    def validate(self, old_column: sqla.column) -> bool:
        if self.name == old_column.name:
            # ToDo perform data type checks
            # print(f'          type ={type(old_column.type)}')
            # old_type = old_column.type
            # nt = old_column.quoted_name
            # print(f'         old type = {old_column.type}')
            # print(f'         new type = {self.data_type}')
            return True
        return False


def get_column(sql_table: sqla.Table, name_list: list[str]) -> sqla.Column:
    for name in name_list:
        if name in sql_table.c.keys():
            return sql_table.c[name]
    print(f"list of column names: {sql_table.c.keys()}")
    raise ValueError(f"column not found in in _get_column", name_list)


class OntologyTable:
    def __init__(self, metadata_obj: sqla.MetaData, table: JsonDataTable) -> None:

        self.columns: list[OntologyColumn] = []
        for column in table.columns:
            self.columns.append(OntologyColumn(column))

        columns = [column.sql_column for column in self.columns]
        self.sql_table = build_data_table(
            metadata_obj, table.name, use_name=True, extra_data_columns=columns
        )

        self.name: str = table.name
        self.vendors: list[str] = table.vendors

    def get_id_column(self) -> sqla.Column:
        return get_column(self.sql_table, [self.name + "_id"])

    def get_name_column(self) -> sqla.Column:
        return get_column(self.sql_table, [self.name + "_name", self.name])

    def pprint(self) -> None:
        print(f"build table: {self.name}")
        for column in self.columns:
            print(f"    column: ({column.name}, {column.data_type})")

    def extra_columns(
            self, old_table: sqla.Table
    ) -> tuple[list[str], list[sqla.Column]]:
        column_names: list[str] = []
        columns: list[sqla.Column] = []
        for column in self.columns:
            if column.name in old_table.c.keys():
                # print(f'    extra: {column.name}')
                column_names.append(column.name)
                columns.append(old_table.c[column.name])
        return column_names, columns

    def validate_column(self, old_column: sqla.Column) -> bool:
        for column in self.columns:
            if column.validate(old_column):
                return True
        return False
