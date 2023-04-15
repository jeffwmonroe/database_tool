import sqlalchemy as sqla
import sqlalchemy.exc
import sqlalchemy_utils.functions as sqlf


class DatabaseConnection:
    url: str
    engine: sqla.Engine | None
    metadata_obj: sqla.MetaData | None
    schema: str | None

    def __init__(self, url: str, schema: str | None = None) -> None:
        self.url = url
        self.engine = None
        self.metadata_obj = None
        self.schema = schema

    def connect_engine(self) -> None:
        self.engine = sqla.create_engine(self.url)
        with self.engine.connect() as conn:
            conn.execute(sqla.text("select 'hello world'"))
            print("Successful login. Database exists. Connection good...")
        if self.schema is None:
            self.metadata_obj = sqla.MetaData()
        else:
            self.metadata_obj = sqla.MetaData(schema=self.schema)
        print("metadata has been set")

    def reflect_tables(self) -> list[str]:
        # ToDo check to see the right exception to raise
        if self.metadata_obj is None:
            raise ValueError("trying to reflect_tables with no metadata object")
        if self.engine is None:
            raise ValueError("trying to reflect_tables with no engine object")
        self.metadata_obj.reflect(bind=self.engine)
        table_list = list(self.metadata_obj.tables.keys())
        return table_list

    def create_database(self) -> None:
        """
        This will create the new database if it does not exist
        """

        if not sqlf.database_exists(self.url):
            print("no database found ... building database ...")
            sqlf.create_database(self.url)
        else:
            print("database already exists")

    def build_tables(self) -> None:
        """
        This is behaving like an abstract method. Python does not have abstract
        methods; however, this is a reminder that this method needs to be implemented
        in child classes.
        :return:
        """
        self.connect_tables(commit=True)

    def connect_tables(self, commit=False) -> None:
        pass

    def drop_database(self) -> None:
        """
        This will drop the test database
        This method really belongs in the base class. I am putting it here
        so that I don't accidentally delete the ontology database.
        """
        url = self.url
        print("Dropping the database")
        try:
            sqlf.drop_database(url)
        except sqlalchemy.exc.ProgrammingError:
            pass
