import sqlalchemy as sqla
# from sqlalchemy import MetaData
# from sqlalchemy import text
import sqlalchemy_utils.functions as sqlf


# from sqlalchemy import Table, Column, Integer, String, DateTime
# import datetime

# import database_exists, create_database, drop_database
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


def create_database():
    """
    This will create the new database if it does not exist
    """
    url = db_url()
    # 'postgresql+psycopg2://postgres:aqua666@localhost:5432/test'
    if not sqlf.database_exists(url):
        print("no database found ... building database ...")
        sqlf.create_database(url)
    else:
        print('database already exists')

    engine = sqla.create_engine(url)

    with engine.connect() as conn:
        result = conn.execute(sqla.text("select 'hello world'"))
        print('Successful login. Database exists. Connection good...')

    return engine


def build_tables(engine):
    metadata_obj = sqla.MetaData()

    type_table = sqla.Table(
        "type",
        metadata_obj,
        sqla.Column("n_id", sqla.Integer, primary_key=True),
        sqla.Column("type", sqla.String(30)),
    )

    vendor_table = sqla.Table(
        "vendor",
        metadata_obj,
        sqla.Column("v_id", sqla.Integer, primary_key=True),
        sqla.Column("vendor", sqla.String(30)),
        sqla.Column("database", sqla.String(30)),
    )

    names_table = sqla.Table(
        "names",
        metadata_obj,
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

    metadata_obj.create_all(engine)


from sqlalchemy import insert
stmt = insert(user_table).values(name="spongebob", fullname="Spongebob Squarepants")
print(stmt)
compiled = stmt.compile()
compiled.params

with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()

def drop_database():
    """
    This will drop the test database
    """
    url = db_url()
    print('Dropping the database')
    sqlf.drop_database(url)


def main():
    engine = create_database()
    build_tables(engine)
    # drop_database()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Main Called')
    main()
