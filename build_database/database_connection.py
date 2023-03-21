import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text


class DatabaseConnection:
    def __init__(self, url, schema=None):
        self.url = url
        self.engine = None
        self.metadata_obj = None
        self.schema = schema

    def connect_engine(self):
        self.engine = sqla.create_engine(self.url)
        print(f'schema = {self.schema}')
        with self.engine.connect() as conn:
            conn.execute(sqla.text("select 'hello world'"))
            print('Successful login. Database exists. Connection good...')
        if self.schema is None:
            self.metadata_obj = sqla.MetaData()
        else:
            self.metadata_obj = sqla.MetaData(schema=self.schema)

    def create_database(self):
        """
        This will create the new database if it does not exist
        """

        if not sqlf.database_exists(self.url):
            print("no database found ... building database ...")
            sqlf.create_database(self.url)
        else:
            print('database already exists')

    def build_tables(self):
        pass
