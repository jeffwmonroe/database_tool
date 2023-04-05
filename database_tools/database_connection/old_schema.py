from database_tools.transfer_table.thing import Thing
from database_tools.database_connection.database_connection import DatabaseConnection


def ontology_url() -> str:
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
    def __init__(self, *args, **kwargs) -> None:
        kwargs['schema'] = 'ontology'
        super().__init__(ontology_url(), *args, **kwargs)
        self.artist_table = None
        self.map_list = None
        self.things: dict[str, Thing] = dict()

    # ToDo refactor test_fill

    def connect_tables(self, commit: bool = False) -> None:
        pass

    def drop_database(self) -> None:
        """
        This will NOT drop the Ontology database
        This is overriding the base case to protect myself from mistakes
        """
        print('-------------------------------')
        print('NOT Dropping the database')
        print(f'database = {self.url}')

    def check_tables(self) -> str:
        result = []
        for key in self.things:
            self.things[key].validate(self.engine, self.metadata_obj)
            result = result + self.things[key].validation_data()
        return result

    def enumerate_tables(self) -> None:
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

        for thing_key in self.things.keys():
            if thing_key in self.things.keys():
                for vendor in vendor_list:
                    for action in action_list:
                        o_key = schema + action + "_" + thing_key + "_" + vendor
                        if o_key in self.metadata_obj.tables.keys():
                            self.things[thing_key].add_vendor_action(vendor, action, self.metadata_obj.tables[o_key])
