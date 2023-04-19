import time
from typing import Any, Callable

import sqlalchemy as sqla
from sqlalchemy import func, insert, select

import database_tools.database_connection.enums as db_enum
from database_tools.database_connection.database_connection import DatabaseConnection
from database_tools.database_connection.old_schema import OntologySchema
from database_tools.database_connection.ontology_table import OntologyTable
from database_tools.database_connection.utilities import (
    build_data_table,
    join_thing_and_map,
)
from database_tools.json_schema.json_schema import JsonSchema, read_schema_json
from database_tools.transfer_table.thing import Action, Thing


# ToDo move this to a configuration
def db_url() -> str:
    # ToDo build these into environment variables or pass them as parameters
    dialect = "postgresql"
    driver = "psycopg2"
    user = "postgres"
    password = "aqua666"
    host = "localhost"
    port = "5432"
    name = "test"
    url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    return url


class NewDatabaseSchema(DatabaseConnection):
    def __init__(self):
        super().__init__(db_url())
        # thing_table is the central table that holds all the primary keys for things
        self.thing_table: sqla.Table | None = None
        # vendor_table is the central table that holds all the primary keys for vendors
        self.vendor_table: sqla.Table | None = None
        # name_map_table is the centralized table that holds all the mapping information for all
        # the thing tables
        self.name_map_table: sqla.Table | None = None
        # bridge_table holds a mapping from primary key in the old database to primary key
        # in the new database
        self.bridge_table: sqla.Table | None = None

        # thing / data tables go here
        self.data_tables: dict[str, OntologyTable] = {}
        # self.vendors: dict[str, list[str]] = {}
        self.schema: JsonSchema | None = None

    def add_vendor(self, vendor: str) -> int:
        stmt = select(self.vendor_table).filter(
            self.vendor_table.c.vendor == vendor, self.vendor_table.c.database == "main"
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            pk_list = [row.v_id for row in result]
            conn.commit()
        if len(pk_list) > 0:
            return pk_list[0]
        stmt = insert(self.vendor_table).values(vendor=vendor, database="main")
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            pk = result.inserted_primary_key[0]
            conn.commit()
        return pk

    def add_things(self, things):
        stmt = insert(self.thing_table).values(things)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            pk = result.inserted_primary_key[0]
            conn.commit()
        print(f"pk = {pk}")

    def build_bridge_table(self) -> sqla.Table:
        return sqla.Table(
            "old_new_bridge",
            self.metadata_obj,
            sqla.Column("old_id", sqla.Integer, primary_key=True),
            sqla.Column("thing", sqla.String(30), nullable=False, primary_key=True),
            sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
            sqla.PrimaryKeyConstraint("old_id", "thing", name="primary_key"),
        )

    def build_tables_from_json(self) -> None:
        # ToDo this is a horrible hard code
        schema = read_schema_json("data/tables.json")
        self.data_tables = {}
        for table in schema.tables:
            self.data_tables[table.name] = OntologyTable(self.metadata_obj, table)

    def connect_tables(self, commit=False) -> None:
        self.bridge_table = self.build_bridge_table()
        self.thing_table = sqla.Table(
            "thing",
            self.metadata_obj,
            sqla.Column("n_id", sqla.Integer, primary_key=True),
            sqla.Column("thing", sqla.String(30)),
            # sqla.Column("type", sqla.String(30), unique=True),
        )

        self.vendor_table = sqla.Table(
            "vendor",
            self.metadata_obj,
            sqla.Column("v_id", sqla.Integer, primary_key=True),
            sqla.Column("vendor", sqla.String(30)),
            sqla.Column("database", sqla.String(30)),
        )

        # self.data_tables = read_table_info(self.metadata_obj)
        # self.vendors = read_vendor_info()
        self.build_tables_from_json()

        self.name_map_table = build_data_table(
            self.metadata_obj,
            "name_map",
            use_vid=True,
            extra_data_columns=[
                sqla.Column("ext_id", sqla.String(200)),
                sqla.Column("map_type", sqla.String(30)),
                sqla.Column("confidence", sqla.REAL),
            ],
        )

        if commit:
            self.metadata_obj.create_all(self.engine)

    def fill_tables(self, old_database: OntologySchema, short_load: bool) -> None:
        start_time = time.time()
        pk = 1000
        print(f"--------------------------------------")
        print("Iteration Test")
        for key in old_database.things.keys():
            print(f"key = {key}")
            thing_table = old_database.things[key]
            if key in self.data_tables.keys():
                print("    found in data_tables")
                new_thing_table = self.data_tables[key]
                print(f"    vendors = {new_thing_table.vendors}")
                # print(f'    thing.thing = {thing_table.thing}')
                extra_columns_names, extra_columns = new_thing_table.extra_columns(
                    thing_table.table
                )
                # print(f'    extra columns = {extra_columns}')
                data_table: sqla.Table = self.data_tables[key].sql_table
                bridge, pk = self.fill_thing_table(
                    data_table,
                    old_database.engine,
                    thing_table,
                    pk,
                    extra_columns,
                    extra_columns_names,
                    short_load,
                )
                for action in thing_table:
                    print(f"    action.vendor = {action.get_vendor()}")
                    if action.vendor in new_thing_table.vendors:
                        self.fill_name_map_table(
                            old_database.engine,
                            thing_table,
                            action,
                            action.vendor,
                            bridge,
                            short_load,
                        )
        duration = time.time() - start_time
        print(f"Total duration: {duration}")

    def fill_thing_table(
            self,
            new_data_table: sqla.Table,
            old_engine: sqla.Engine,
            old_thing: Thing,
            thing_pk_start: int,
            extra_columns: list[sqla.Column],
            extra_column_names: list[str],
            short_load: bool,
    ) -> tuple[dict[int, int], int]:
        print(f"    fill_thing_table:  {old_thing.thing}")
        standard_columns: list[sqla.Column] = [
            old_thing.get_id_column().label("old_id"),
            old_thing.get_name_column().label("name"),
            old_thing.table.c.updated_ts.label("t_updated_ts"),
            old_thing.table.c.updated_by.label("t_updated_by"),
            func.rank().over(order_by=old_thing.get_id_column()).label("rank"),
        ]
        columns = standard_columns + extra_columns
        subquery1 = select(*columns).subquery()
        if short_load:
            subquery2 = select(subquery1).filter(subquery1.c.rank < 11)
        else:
            subquery2 = select(subquery1)
        stmt = subquery2
        # ToDo clean up the thing_add type definition
        thing_add: list[dict[str, Any]] = []
        # Bridge_add is a list of dicts old_id, thing, new_id. It is formed so that it can
        # be dumped to the bridge table in the new database. This is not strictly necessary but
        # is good for validation and debugging.
        bridge_add: list[dict[str, str | int | Any]] = []
        # bridge is a dictionary mapping old_pks to new_pks. It is used in memory for this execution
        bridge: dict[int, int] = {}
        index = 0
        with old_engine.connect() as connection:
            result = connection.execute(stmt)
            row_num: int | Callable[[], int] = result.rowcount
            for row in result:
                time_stamp = row.t_updated_ts
                # dt.tz_convert
                row_dict = {
                    "n_id": thing_pk_start + index,
                    "name": row.name,
                    "action": db_enum.Action.create,
                    "created_ts": time_stamp,
                    "created_by": row.t_updated_by,
                    "status": db_enum.Status.draft,
                }
                for column_name in extra_column_names:
                    if column_name in row._fields:
                        row_dict[column_name] = row._asdict()[column_name]
                thing_add.append(row_dict)
                bridge_add.append(
                    {
                        "old_id": row.old_id,
                        "thing": old_thing.thing,
                        "n_id": thing_pk_start + index,
                    }
                )
                bridge[row.old_id] = thing_pk_start + index
                index += 1
        # values is used for the type table
        values: list[dict[str, int | str]] = [
            {"n_id": thing_pk_start + index, "thing": old_thing.thing}
            for index in range(row_num)
        ]

        with self.engine.connect() as new_connection:
            # working
            insert_type = self.thing_table.insert()
            new_connection.execute(insert_type, values)

            insert_thing = new_data_table.insert()
            new_connection.execute(insert_thing, thing_add)

            # working
            bridge_ins = self.bridge_table.insert()
            new_connection.execute(bridge_ins, bridge_add)

            new_connection.commit()
        return bridge, thing_pk_start + index

    def fill_name_map_table(
            self,
            engine: sqla.Engine,
            old_thing: Thing,
            old_action: Action,
            vendor: str,
            bridge: dict[int, int],
            short_load: bool,
    ) -> None:
        vendor_pk = self.add_vendor(vendor)
        stmt = join_thing_and_map(old_thing, old_action, short_load)
        map_add = []

        start_time = time.time()
        index = 0
        with engine.connect() as connection:
            result = connection.execute(stmt)
            # row_num = result.rowcount
            for row in result:
                old_pk = row.old_id
                new_pk = bridge[old_pk]
                # print(f'old-new:  {old_pk} : {new_pk}')
                map_add.append(
                    {
                        "n_id": new_pk,
                        "v_id": vendor_pk,
                        "ext_id": row.ext_id,
                        "map_type": "person",
                        "confidence": 1,
                        "action": db_enum.Action.create,
                        "created_ts": row.m_updated_ts,
                        "created_by": row.m_updated_by,
                        "status": db_enum.Status.draft,
                    }
                )
                index += 1
        # print('map add len')
        # print(len(map_add))
        if len(map_add) > 0:
            with self.engine.connect() as new_connection:
                insert_map = self.name_map_table.insert()
                new_connection.execute(insert_map, map_add)

                new_connection.commit()
        duration = time.time() - start_time
        print(f"         Duration: {duration}")
