import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from database_connection import DatabaseConnection


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


class NewDatabaseSchema(DatabaseConnection):
    def __init__(self):
        super().__init__(db_url())
        self.type_table = None
        self.vendor_table = None
        self.names_table = None

    def build_tables(self):

        self.type_table = sqla.Table(
            "type",
            self.metadata_obj,
            sqla.Column("n_id", sqla.Integer, primary_key=True),
            sqla.Column("type", sqla.String(30), unique=True),
        )

        self.vendor_table = sqla.Table(
            "vendor",
            self.metadata_obj,
            sqla.Column("v_id", sqla.Integer, primary_key=True),
            sqla.Column("vendor", sqla.String(30)),
            sqla.Column("database", sqla.String(30)),
        )

        self.names_table = sqla.Table(
            "artist",
            self.metadata_obj,
            sqla.Column("log_id", sqla.Integer, primary_key=True),
            sqla.Column("n_id", sqla.ForeignKey("type.n_id"), nullable=False),
            sqla.Column("name", sqla.String(30)),
            # ToDo make action into an enum
            sqla.Column("action", sqla.String(30)),
            sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            # ToDo make this a user ID (authenticated)
            sqla.Column("created_by", sqla.String(30)),
            # ToDo make this an enum
            sqla.Column("status", sqla.String(30)),
        )
        # ToDo have the create_all be optional
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
    def drop_database(self):
        """
        This will drop the test database
        This method really belongs in the base class. I am putting it here
        so that I don't accidentally delete the ontology database.
        """
        url = self.url
        print('Dropping the database')
        sqlf.drop_database(url)
