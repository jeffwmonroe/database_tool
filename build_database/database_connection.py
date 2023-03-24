import sqlalchemy as sqla
import sqlalchemy_utils.functions as sqlf


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
        print("metadata has been set")

    def reflect_tables(self):
        self.metadata_obj.reflect(bind=self.engine)
        # print(self.metadata_obj.tables)
        # print(len(self.metadata_obj.tables.keys()))
        # print(self.metadata_obj.tables['ontology.artist'])

        # Loop over tables
        # for key in self.metadata_obj.tables.keys():
        #     print(f'   key = {key}')
        table_list = list(self.metadata_obj.tables.keys())
        # print("number of tables = " + str(len(table_list)))
        # vtable = self.metadata_obj.tables['ontology.venue']
        # print(f'vtable = {vtable}')
        # print(type(vtable))
        # Loop over columns
        # for col in vtable.c:
        #     print(f'   col = {col.name}, {col.type}')
        return table_list

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
        """
        This is behaving like an abstract method. Python does not have abstract
        methods; however, this is a reminder that this method needs to be implemented
        in child classes.
        :return:
        """
        self.connect_tables(commit=True)

    def connect_tables(self, commit=False):
        pass

    def drop_database(self):
        """
        This will drop the test database
        This method really belongs in the base class. I am putting it here
        so that I don't accidentally delete the ontology database.
        """
        url = self.url
        print('Dropping the database')
        sqlf.drop_database(url)

    def connect_and_print(self, stmt, print_row=False):
        """
        This is a useful test function. It prints the SQL query and then executes it.
        :param stmt:
        :return: n/a
        """
        # print('------------------------------')
        # print('     connect and print')
        # print(stmt)
        i = 0
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            num_rows = result.rowcount
            if print_row:
                for row in result:
                    print(f"   row = {row}")
                    # i = i + 1
                    # if i > 10:
                    #     break
        return num_rows
