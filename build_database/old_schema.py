from sqlalchemy import select, func
from database_connection import DatabaseConnection
# from otable import MapTable, ThingTable
from thing import Thing
from utilities import test_fill, fill_thing_table
import time


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
        self.map_list = None
        self.things = dict()

    def test_fill(self, database):

        start_time = time.time()
        pk = 1000
        print(f'--------------------------------------')
        print('Iteration Test')
        for key in self.things.keys():
            thing_table = self.things[key]
            if thing_table.thing in database.vendors.keys():
                data_table = database.data_tables[key]
                bridge, pk = fill_thing_table(database, data_table, self.engine, thing_table, pk)
                for action in thing_table:
                    if action.vendor in database.vendors[thing_table.thing]:
                        test_fill(database, self.engine, thing_table, action, action.vendor, bridge)
        duration = time.time() - start_time
        print(f'Total duration: {duration}')

    def connect_tables(self, commit=False):
        pass

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
