from sqlalchemy import select, func
from database_connection import DatabaseConnection
from otable import MapTable, ThingTable


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


class OntologySchema(DatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['schema'] = 'ontology'
        super().__init__(ontology_url(), *args, **kwargs)
        self.artist_table = None
        self.map_table = None

    def connect_tables(self, commit=False):
        self.artist_table = ThingTable(self.metadata_obj, "artist")
        self.map_table = MapTable(self.metadata_obj, "artist", "cheetah")

    def drop_database(self):
        """
        This will NOT drop the Ontology database
        This is overriding the base case to protect myself from mistakes
        """
        print('-------------------------------')
        print('NOT Dropping the database')
        print(f'database = {self.url}')

    def is_one_to_one(self):
        subquery = select(
            self.map_table.table.c.ext_id,
            func.count(self.map_table.table.c.ext_id).label("count")
        ).group_by(self.map_table.table.c.ext_id).subquery()

        stmt = select(subquery).filter(subquery.c.count > 1)
        ext_row = self.connect_and_print(stmt)

        subquery = select(
            self.map_table.table.c.artist_id,
            func.count(self.map_table.table.c.artist_id).label("count")
        ).group_by(self.map_table.table.c.artist_id).subquery()

        stmt = select(subquery).filter(subquery.c.count > 1)
        type_row = self.connect_and_print(stmt)

        print(f"rows = {ext_row}, {type_row}")
        if ext_row + type_row == 0:
            print("One to One relationship!!")
            return True
        else:
            print("Many to One relationship!!")
            return False

    def loop_over(self):
        """
        This is bad code but it was my first loop over the artist_table so I am
        keeping it around so that I can reuse code.
        """
        stmt = select(self.artist_table.table)
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
        stmt = select(self.map_table.table)
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
        count_fn = func.count(self.map_table.table.c.artist_id)
        print(count_fn)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            print(f"Results from count= {result.rowcount}")

        stmt = select(self.map_table.table).order_by(self.map_table.table.c.artist_id)
        print(f'stmt = {stmt}')
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

    def column_test(self):
        print('Column Test:')
        print(type(self.artist_table.table.c))
        print(self.artist_table.table.c['artist_id'])

        t_list = ['artist_id', 'artist_name']
        c_list = [self.artist_table.table.c[x] for x in t_list]
        print(c_list)
        stmt = select(
            *c_list,
            # self.artist_table.table.c[t_list[0]],
            # self.artist_table.table.c[t_list[1]],
        )
        self.connect_and_print(stmt)
