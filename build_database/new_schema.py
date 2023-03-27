import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from database_connection import DatabaseConnection
import enum
from sqlalchemy import Enum


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


def build_data_table(metadata_obj, name, use_vid=False, use_name=False, data_cols=[]):
    keys = [sqla.Column("log_id", sqla.Integer, primary_key=True),
            sqla.Column("n_id", sqla.ForeignKey("type.n_id"), nullable=False),
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
    args = keys + data_cols + logs
    table = sqla.Table(name,
                       metadata_obj,
                       *args
                       )
    return table


class NewDatabaseSchema(DatabaseConnection):
    def __init__(self):
        super().__init__(db_url())
        self.type_table = None
        self.vendor_table = None
        self.artist_table = None
        self.name_map_table = None

    def add_vendor(self, vendor):
        # ToDo add code to detect if vendor already exists
        stmt = select(self.vendor_table).filter(self.vendor_table.c.vendor == vendor,
                                                self.vendor_table.c.database == 'main')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            rows_found = result.rowcount
            conn.commit()
        if rows_found > 0:
            return
        stmt = insert(self.vendor_table).values(vendor=vendor, database="main")
        pk = None
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            pk = result.inserted_primary_key
            conn.commit()
        return pk

    def add_things(self,
                   things):
        stmt = insert(self.type_table).values(things)
        pk = None
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            pk = result.inserted_primary_key
            conn.commit()
        print(f'pk = {pk}')

    def add_thing_no_transaction(self,
                                 connection,
                                 thing,
                                 vendor,
                                 old_pk,
                                 name,
                                 thing_updated_by,
                                 thing_updated_ts,
                                 ext_id,
                                 map_updated_by,
                                 map_updated_ts):
        stmt1 = insert(self.type_table).values(type=thing)
        pk = None
        result = connection.execute(stmt1)
        pk = result.inserted_primary_key[0]
    # print(f'pk = {pk}')

        stmt2 = insert(self.artist_table).values(
            n_id=pk,
            name=name,
            action=Action.create,
            created_ts=thing_updated_ts,
            created_by=thing_updated_by,
            status=Status.draft
        )
        # print(f'   pk = {pk}')
        thing_pk = None
        result = connection.execute(stmt2)
        thing_pk = result.inserted_primary_key

    def add_thing(self,
                  thing,
                  vendor,
                  old_pk,
                  name,
                  thing_updated_by,
                  thing_updated_ts,
                  ext_id,
                  map_updated_by,
                  map_updated_ts):
        stmt1 = insert(self.type_table).values(type=thing)
        pk = None
        with self.engine.connect() as conn:
            result = conn.execute(stmt1)
            pk = result.inserted_primary_key[0]
            conn.commit()
        # print(f'pk = {pk}')

        stmt = insert(self.artist_table).values(
            n_id=pk,
            name=name,
            action=Action.create,
            created_ts=thing_updated_ts,
            created_by=thing_updated_by,
            status=Status.draft
        )
        # print(f'   pk = {pk}')
        thing_pk = None
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            thing_pk = result.inserted_primary_key
            conn.commit()

    def connect_tables(self, commit=False):

        self.type_table = sqla.Table(
            "type",
            self.metadata_obj,
            sqla.Column("n_id", sqla.Integer, primary_key=True),
            sqla.Column("type", sqla.String(30)),
            # sqla.Column("type", sqla.String(30), unique=True),
        )

        self.vendor_table = sqla.Table(
            "vendor",
            self.metadata_obj,
            sqla.Column("v_id", sqla.Integer, primary_key=True),
            sqla.Column("vendor", sqla.String(30)),
            sqla.Column("database", sqla.String(30)),
        )

        self.artist_table = build_data_table(self.metadata_obj, "artist", use_name=True)

        self.name_map_table = build_data_table(self.metadata_obj,
                                               "name_map",
                                               use_vid=True,
                                               data_cols=[sqla.Column("ext_id", sqla.String(30)),
                                                          sqla.Column("map_type", sqla.String(30)),
                                                          sqla.Column("confidence", sqla.REAL),
                                                          ])

        # ToDo have the create_all be optional
        if commit:
            self.metadata_obj.create_all(self.engine)

    def insert_types(self):
        # ToDo complete this list. Probably move it into data or at least as a global
        type_list = ['app', 'artist', 'brand', 'category', 'venue']
        stmt_list = []
        for type_name in type_list:
            stmt = insert(self.type_table).values(type=type_name)
            print(f'stmt = {stmt}')
            compiled = stmt.compile()
            stmt_list.append(stmt)
            print(f'compiled = {compiled.params}')

        for stmt in stmt_list:
            with self.engine.connect() as conn:
                try:
                    result = conn.execute(stmt)
                    conn.commit()
                except IntegrityError:
                    print(f'type already exists')

    # ToDo move this to base class
