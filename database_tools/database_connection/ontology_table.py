import sqlalchemy as sqla

from database_tools.database_connection.utilities import (build_data_table,
                                                          build_sql_column)
from database_tools.json_schema.json_schema import JsonColumn, JsonDataTable


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


def get_bridge_keys(column, metadata_obj, engine):
    bridge_table = metadata_obj.tables["old_new_bridge"]
    bridge_query1 = sqla.select(bridge_table).filter(
        bridge_table.c.thing == column.foreign_table
    )
    with engine.connect() as connection:
        result = connection.execute(bridge_query1)
        old_keys = [row[0] for row in result]
        connection.commit()
    return old_keys


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

    def null_bad_foreign_keys(
        self,
        column,
        metadata_obj: sqla.MetaData,
        engine: sqla.Engine,
    ):
        old_keys = get_bridge_keys(column, metadata_obj, engine)

        null_update = (
            sqla.update(self.sql_table)
            .where(self.sql_table.c[column.name].not_in(old_keys))
            .values({self.sql_table.c[column.name]: 666})
        )
        # ToDo fix hardwired value. What is NULL?
        with engine.connect() as connection:
            connection.execute(null_update)
            connection.commit()

    def connect_foreign_keys(
        self,
        metadata_obj: sqla.MetaData,
        engine: sqla.Engine,
    ) -> None:
        # print(f'   table: {self.name}')
        for column in self.columns:
            if column.foreign_table is None:
                continue
            print(f"      found foreign key: {column.name}")
            if column.foreign_table not in metadata_obj.tables.keys():
                print(f"         ERROR foreign table {column.foreign_table} not found")
                break
            bridge_table = metadata_obj.tables["old_new_bridge"]

            self.null_bad_foreign_keys(column, metadata_obj, engine)

            bridge_query = (
                sqla.select(bridge_table)
                .filter(bridge_table.c.thing == column.foreign_table)
                .subquery()
            )
            join_query = (
                sqla.select(
                    self.sql_table.c.log_id,
                    self.sql_table.c.n_id,
                    bridge_query.c.old_id,
                    bridge_query.c.n_id.label("answer"),
                )
                .join(
                    bridge_query, self.sql_table.c[column.name] == bridge_query.c.old_id
                )
                .subquery()
            )

            update_query = (
                sqla.update(self.sql_table)
                .where(self.sql_table.c.log_id == join_query.c.log_id)
                .values({self.sql_table.c[column.name]: join_query.c["answer"]})
            )

            with engine.connect() as connection:
                connection.execute(update_query)
                connection.commit()

    def standard_table_exceptions(self) -> list[str]:
        table_exceptions = [
            f"import_{self.name}",
            f"export_{self.name}",
            f"history_{self.name}",
        ]
        for vendor in self.vendors:
            col_exceptions = [
                f"exp_map_{self.name}_{vendor}",
                f"fuzzymatch_{self.name}_{vendor}",
                f"history_map_{self.name}_{vendor}",
                f"log_map_{self.name}_{vendor}",
                f"map_{self.name}_{vendor}",
                f"import_{self.name}_{vendor}",
            ]
            table_exceptions = table_exceptions + col_exceptions
        return table_exceptions
