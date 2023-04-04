import sqlalchemy as sqla
from sqlalchemy import insert, select
from database_tools.database_connection.database_connection import DatabaseConnection
from database_tools.database_connection.utilities import build_data_table
from database_tools.database_connection.excel_reader import read_table_info, read_vendor_info

# ToDo move this to a .env vile
table_list = "./data/table_list.xlsx"


def db_url() -> str:
    # ToDo build these into environment variables or pass them as parameters
    dialect = 'postgresql'
    driver = 'psycopg2'
    user = 'postgres'
    password = 'aqua666'
    host = 'localhost'
    port = '5432'
    name = 'test'
    url = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}'
    return url


class NewDatabaseSchema(DatabaseConnection):
    def __init__(self):
        super().__init__(db_url())
        # thing_table is the central table that holds all the primary keys for things
        self.thing_table: sqla.Table | None = None
        # vendor_table is the central table that holds all the primary keys for vendors
        self.vendor_table: sqla.Table | None = None
        # thing / data tables go here
        self.data_tables: dict[str, sqla.Table] = {}
        self.vendors: dict[str, list[str]] = {}
        # name_map_table is the centralized table that holds all the mapping information for all
        # the thing tables
        self.name_map_table: sqla.Table | None = None
        # bridge_table holds a mapping from primary key in the old database to primary key
        # in the new database
        self.bridge_table: sqla.Table | None = None

    def add_vendor(self, vendor: str) -> int:
        stmt = select(self.vendor_table).filter(self.vendor_table.c.vendor == vendor,
                                                self.vendor_table.c.database == 'main')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            rows_found: int = result.rowcount
            pk_list = [row.v_id for row in result]
            conn.commit()
        if rows_found > 0:
            # print(f"PK found: {pk_list}")
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
        print(f'pk = {pk}')

    def build_bridge_table(self) -> sqla.Table:
        return sqla.Table("old_new_bridge",
                          self.metadata_obj,
                          sqla.Column("old_id", sqla.Integer, primary_key=True),
                          sqla.Column("thing", sqla.String(30), nullable=False, primary_key=True),
                          sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
                          sqla.PrimaryKeyConstraint("old_id", "thing", name="primary_key"))

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

        self.data_tables = read_table_info(self.metadata_obj)
        self.vendors = read_vendor_info()

        self.name_map_table = build_data_table(self.metadata_obj,
                                               "name_map",
                                               use_vid=True,
                                               extra_data_columns=[sqla.Column("ext_id", sqla.String(200)),
                                                                   sqla.Column("map_type", sqla.String(30)),
                                                                   sqla.Column("confidence", sqla.REAL),
                                                                   ])

        if commit:
            self.metadata_obj.create_all(self.engine)
