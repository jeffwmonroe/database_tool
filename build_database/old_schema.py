from sqlalchemy import select, func
from database_connection import DatabaseConnection
from otable import MapTable, ThingTable
from thing import Thing
from utilities import test_fill


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


# ActionTable = namedtuple("ActionTable", "thing vendor action")


class OntologySchema(DatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['schema'] = 'ontology'
        super().__init__(ontology_url(), *args, **kwargs)
        self.artist_table = None
        self.map_list = None
        self.things = dict()

    def test_fill(self):
        print('test fill:')
        ethnicity = self.things["ethnicity"]
        print('table found')
        print(ethnicity.vendor_actions.keys())
        map = ethnicity.vendor_actions[('koala', 'map')]
        print('map found')
        test_fill(self.engine, ethnicity, map)

    def connect_tables(self, commit=False):
        self.artist_table = ThingTable(self.metadata_obj, "artist")
        vendor_list = ['cheetah', 'mockingbird', 'monkey', 'porcupine', 'tapir']
        self.map_list = [MapTable(self.metadata_obj, "artist", vendor) for vendor in vendor_list]

    def drop_database(self):
        """
        This will NOT drop the Ontology database
        This is overriding the base case to protect myself from mistakes
        """
        print('-------------------------------')
        print('NOT Dropping the database')
        print(f'database = {self.url}')

    def is_one_to_one(self, show_many_to_one=False):
        return_list = []
        print("is_one_to_one")
        for map_table in self.map_list:
            print(f'Checking map table: {map_table.thing}, {map_table.vendor}')
            stmt = select(
                map_table.table.c.artist_id
            )
            total_rows = self.connect_and_print(stmt)
            subquery = select(
                map_table.table.c.ext_id,
                func.count(map_table.table.c.ext_id).label("count")
            ).group_by(map_table.table.c.ext_id).subquery()

            stmt = select(subquery).filter(subquery.c.count > 1)
            ext_row = self.connect_and_print(stmt)

            subquery = select(
                map_table.table.c.artist_id,
                func.count(map_table.table.c.artist_id).label("count")
            ).group_by(map_table.table.c.artist_id).subquery()

            stmt = select(subquery).filter(subquery.c.count > 1).order_by(subquery.c.count, subquery.c.artist_id)
            type_row = self.connect_and_print(stmt, print_row=show_many_to_one)

            print(f"    rows = ext: {ext_row}, ont: {type_row} total: {total_rows}")
            if ext_row + type_row == 0:
                print("    One to One relationship!!")
                return_list.append(True)
            else:
                print("    Many to One relationship!!")
                return_list.append(True)
        return return_list

    def simple_loop(self, t_obj):
        print(type(t_obj.table))
        stmt = select(t_obj.table.c.ext_id, t_obj.table.c.artist_id)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            i = 0
            for row in result:
                t_obj.print_row(row)
                i = i + 1
                if i > 10:
                    break

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

    def check_tables(self):
        result = []
        for key in self.things:
            self.things[key].validate(self.engine, self.metadata_obj)
            result = result + self.things[key].validation_data()
        return result

    def enumerate_tables(self):
        thing_list = ['actor', 'app', 'artist', 'brand', 'category', 'category_hierarchy',
                      'company', 'division', 'eduction_level', 'entity_type', 'ethnicity',
                      'franchise', 'gender', 'generation', 'genre', 'has_children', 'household_size',
                      'marital_status', 'political_affiliation', 'product', 'property_value',
                      'region', 'residence', 'space', 'sport', 'sport_league', 'sport_team', 'sport_team_league',
                      'state', 'theatre_tite', 'ticker', 'time', 'time_table', 'time_type', 'title',
                      'venue', 'yearly_income',
                      ]
        vendor_list = ['alligator', 'android', 'brand', 'ios', 'cheetah', 'ferret', 'ferret_retailer',
                       'genre', 'koala',
                       'mockingbird', 'monkey', 'moose', 'natterjack', 'panther', 'peacock',
                       'platypus', 'porcupine',
                       'stork', 'stork_ios_android', 'swan',
                       'tapir',
                       'value_koala', 'concert_venue',
                       ]
        action_list = ['map', 'fuzzymatch', 'import', 'exp', 'history']

        # reflected_dict = self.metadata_obj.tables
        # print(type(table_dict))
        schema = 'ontology.'
        # ------------------------------------
        #    build the thing level dictionary
        for thing in thing_list:
            key = schema + thing
            if key in self.metadata_obj.tables.keys():
                thing_obj = Thing(thing, self.metadata_obj.tables[key])
                self.things[thing] = thing_obj
                # print(f'Thing added: {thing}')
                for action in action_list:
                    a_key = schema + action + "_" + thing
                    if a_key in self.metadata_obj.tables.keys():
                        # print(f"   thing action added: {action}")
                        thing_obj.add_action(action, self.metadata_obj.tables[a_key])

        for thing in self.things.keys():
            if thing in self.things.keys():
                for vendor in vendor_list:
                    for action in action_list:
                        o_key = schema + action + "_" + thing + "_" + vendor
                        if o_key in self.metadata_obj.tables.keys():
                            self.things[thing].add_vendor_action(vendor, action, self.metadata_obj.tables[o_key])
