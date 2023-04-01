import sqlalchemy as sqla
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from database_connection import DatabaseConnection
import enum
from sqlalchemy import Enum
import pandas as pd
import math

# ToDo move this to a .env vile
table_list = "../data/table_list.xlsx"
class Status(enum.Enum):
    draft = 1
    stage = 2
    production = 3


class Action(enum.Enum):
    create = 1
    modify = 2
    delete = 3


def db_url():
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


def build_sql_column(col_name, col_type):
    match col_type:
        case 'string30':
            return sqla.Column(col_name, sqla.String(30))
        case 'string200':
            return sqla.Column(col_name, sqla.String(200))
        case 'varchar':
            return sqla.Column(col_name, sqla.VARCHAR)
        case 'int':
            return sqla.Column(col_name, sqla.Integer)
        case 'double':
            return sqla.Column(col_name, sqla.DOUBLE_PRECISION)
    raise ValueError('Column type not found in build_sql_column', col_type)


def build_data_table(metadata_obj, name, use_vid=False, use_name=False, data_cols=None):
    if data_cols is None:
        data_cols = []
    keys = [sqla.Column("log_id", sqla.Integer, primary_key=True),
            sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
            ]
    if use_vid:
        keys.append(sqla.Column("v_id", sqla.ForeignKey("vendor.v_id"), nullable=False))

    if use_name:
        data_cols.insert(0, sqla.Column("name", sqla.String(200)))

    logs = [sqla.Column("action", Enum(Action)),
            sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            # ToDo make this a user ID (authenticated)
            sqla.Column("created_by", sqla.String(30)),
            sqla.Column("status", Enum(Status)),
            ]
    args = keys + logs + data_cols
    table = sqla.Table(name,
                       metadata_obj,
                       *args
                       )
    return table


class NewDatabaseSchema(DatabaseConnection):
    def __init__(self):
        super().__init__(db_url())
        self.thing_table = None
        self.vendor_table = None
        self.data_tables = {}
        self.vendors = {}
        self.name_map_table = None
        self.bridge_table = None

    def add_vendor(self, vendor):
        stmt = select(self.vendor_table).filter(self.vendor_table.c.vendor == vendor,
                                                self.vendor_table.c.database == 'main')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            rows_found = result.rowcount
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

    def build_bridge_table(self):
        return sqla.Table("old_new_bridge",
                          self.metadata_obj,
                          sqla.Column("old_id", sqla.Integer, primary_key=True),
                          sqla.Column("thing", sqla.String(30), nullable=False, primary_key=True),
                          sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
                          sqla.PrimaryKeyConstraint("old_id", "thing", name="primary_key"))

    def read_table_info(self):
        print('---------------------------------------')
        print('new_schema: read_table_info')
        df = pd.read_excel(table_list,
                           sheet_name='tables',
                           keep_default_na=False)
        for row_index, row in df.iterrows():
            row_list = list(row.values)
            table_name = row_list[0]
            row_list = row_list[1:]
            print(f'   table name: {table_name}')
            data_columns = []
            while len(row_list) > 1:
                col_name = row_list[0]
                col_type = row_list[1]
                row_list = row_list[2:]
                if col_name == '' or col_type == '':
                    break
                data_columns.append(build_sql_column(col_name, col_type))
            self.data_tables[table_name] = build_data_table(self.metadata_obj,
                                                            table_name,
                                                            use_name=True,
                                                            data_cols=data_columns)
            self.vendors[table_name] = []

    def read_vendor_info(self):
        print('---------------------------------------')
        print('read_vendor_info') \
            # ToDo fix the file name
        df = pd.read_excel(table_list,
                           sheet_name='vendors',
                           keep_default_na=False)
        for row_index, row in df.iterrows():
            row_list = list(row.values)
            table_name = row_list[0]
            print(f'   table name: {table_name}')
            vendors = []
            for val in row_list[1:]:
                if val != '':
                    vendors.append(val)
            self.vendors[table_name] = vendors

    def connect_tables(self, commit=False):
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

        self.read_table_info()
        self.read_vendor_info()

        self.name_map_table = build_data_table(self.metadata_obj,
                                               "name_map",
                                               use_vid=True,
                                               data_cols=[sqla.Column("ext_id", sqla.String(200)),
                                                          sqla.Column("map_type", sqla.String(30)),
                                                          sqla.Column("confidence", sqla.REAL),
                                                          ])

        if commit:
            self.metadata_obj.create_all(self.engine)

    def insert_types(self):
        # ToDo complete this list. Probably move it into data or at least as a global
        type_list = ['app', 'artist', 'brand', 'category', 'venue']
        stmt_list = []
        for type_name in type_list:
            stmt = insert(self.thing_table).values(thing=type_name)
            print(f'stmt = {stmt}')
            compiled = stmt.compile()
            stmt_list.append(stmt)
            print(f'compiled = {compiled.params}')

        for stmt in stmt_list:
            with self.engine.connect() as conn:
                try:
                    conn.execute(stmt)
                    conn.commit()
                except IntegrityError:
                    print(f'type already exists')

    # ToDo move this to base class
