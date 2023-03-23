import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf
from sqlalchemy import insert, select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from database_connection import DatabaseConnection


def ontology_url():
    # ToDo build these into environment variables or pass them as parameters
    dialect = 'postgresql'
    driver = 'psycopg2'
    user = 'postgres'
    password = 'aqua666'
    host = 'localhost'
    port = '5432'
    name = 'ontology'
    url = f'{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}'
    return url


def log_info():
    return [sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            sqla.Column('updated_ts', sqla.DateTime(timezone=True)),
            sqla.Column("created_by", sqla.String(200)),
            sqla.Column("updated_by", sqla.String(200)),
            ]


def type_table_no_data(metadata_obj, name):
    keys = [sqla.Column(name + "_id", sqla.Integer, primary_key=True),
            sqla.Column(name + "_name", sqla.String(200)),
            ]

    log = log_info()

    args = keys + log
    return sqla.Table(name,
                      metadata_obj,
                      *args
                      )


def map_table(metadata_obj, name, vendor):
    keys = [sqla.Column("id", sqla.Integer, primary_key=True), sqla.Column("ext_id", sqla.BIGINT)]
    log = log_info()
    data = [sqla.Column(name + "_id", sqla.ForeignKey(name + "." + name + "_id"), nullable=False),
            ]
    args = keys + log + data
    return sqla.Table("map_" + name + "_" + vendor,
                      metadata_obj,
                      *args
                      )

class OntologySchema(DatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['schema'] = 'ontology'
        super().__init__(ontology_url(), *args, **kwargs)
        self.artist_table = None
        self.map_table = None

    def connect_tables(self, commit=False):
        self.artist_table = type_table_no_data(self.metadata_obj, "artist")
        self.map_table = map_table(self.metadata_obj, "artist", "cheetah")

    def drop_database(self):
        """
        This will NOT drop the Ontology database
        This is overriding the base case to protect myself from mistakes
        """
        print('-------------------------------')
        print('NOT Dropping the database')
        print(f'database = {self.url}')


    def loop_over(self):

        stmt = select(self.artist_table)
        print(f'stmt = {stmt}')

        print('artist table')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

            i = 0
            for row in result:
                created = row.created_ts
                updated = row.updated_ts
                delta = (updated - created).total_seconds()
                if delta > 1:
                    print(
                        f"   id: {row.artist_id}  name: {row.artist_name} created_ts: {row.created_ts} delta: {delta}")
                if i < 10:
                    print(
                        f"   id: {row.artist_id}  name: {row.artist_name} created_ts: {row.created_ts} delta: {delta}")

                i += 1
                # if i == 10:
                #     break
        stmt = select(self.map_table)
        print(f'stmt = {stmt}')
        print('map table')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

            i = 0
            print(f"Results = {result.rowcount}")
            for row in result:
                created = row.created_ts
                updated = row.updated_ts
                delta = (updated - created).total_seconds()
                if delta > 1:
                    print(
                        f"   Large Delta!!! id: {row.artist_id}  ext_id: {row.ext_id} created_ts: {row.created_ts} delta: {delta}")
                if i < 10:
                    print(
                        f"   id: {row.artist_id}  ext_id: {row.ext_id} created_ts: {row.created_ts} delta: {delta}")

                i += 1
        count_fn = func.count(self.map_table.c.artist_id)
        print(count_fn)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            print(f"Results from count= {result.rowcount}")

        stmt = select(self.map_table).order_by(self.map_table.c.artist_id)
        print(f'stmt = {stmt}')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
