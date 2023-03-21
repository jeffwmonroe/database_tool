import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf
from sqlalchemy import insert, select
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


def type_table_no_data (metadata_obj, name):
    return sqla.Table(name,
                      metadata_obj,
                      sqla.Column(name+"_id", sqla.Integer, primary_key=True),
                      sqla.Column(name+"_name", sqla.String(200)),
                      sqla.Column('created_ts', sqla.DateTime(timezone=True)),
                      sqla.Column('updated_ts', sqla.DateTime(timezone=True)),
                      sqla.Column("created_by", sqla.String(200)),
                      sqla.Column("updated_by", sqla.String(200)),
                      )


class OntologySchema(DatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['schema'] = 'ontology'
        super().__init__(ontology_url(), *args, **kwargs)
        self.artist_table = None

    def build_tables(self):
        self.artist_table = sqla.Table(
            "artist",
            self.metadata_obj,
            sqla.Column("artist_id", sqla.Integer, primary_key=True),
            sqla.Column("artist_name", sqla.String(200)),
            sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            sqla.Column('updated_ts', sqla.DateTime(timezone=True)),
            sqla.Column("created_by", sqla.String(200)),
            sqla.Column("updated_by", sqla.String(200)),
        )

    def loop_over(self):

        stmt = select(self.artist_table)
        print(f'stmt = {stmt}')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

            print('artist table')
            for row in result:
                created = row.created_ts
                updated = row.updated_ts
                delta = (updated - created).total_seconds()
                if delta > 1:
                    print(
                        f"   id: {row.artist_id}  name: {row.artist_name} created_ts: {row.created_ts} delta: {delta}")
                # i += 1
                # if i == 10:
                #     break
