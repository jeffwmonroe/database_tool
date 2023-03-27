import pandas as pd
import psycopg2
import sqlalchemy
from new_schema import NewDatabaseSchema
from old_schema import OntologySchema
import argparse




def parse_arguments():
    parser = argparse.ArgumentParser(prog="scratch",
                                     description="Database practice tool",
                                     epilog="Thanks for practicing with %(prog)s! :-)",
                                     allow_abbrev=False,
                                     # argument_default=argparse.SUPPRESS,
                                     )
    # parser.add_argument("echo")
    database = parser.add_argument_group("database tools")
    database.add_argument("-c", "--create",
                          help='create a new database',
                          action="store_true",
                          )
    database.add_argument("--drop",
                          help='drop the database',
                          action="store_true",
                          )
    table = parser.add_argument_group("table tools")
    table.add_argument("-t", "--table",
                       help='build the new schema tables in a fresh database',
                       action="store_true",
                       )
    table.add_argument("-r", "--reflect",
                       help='reflect the tables from the ontology database',
                       action="store_true",
                       )
    table.add_argument("-e", "--enumerate",
                       help='go over the tables and build structure',
                       action="store_true",
                       )
    table.add_argument("--check",
                       help='perform checks on all of the reflected and enumerated tables',
                       action="store_true",
                       )
    table.add_argument("-f", "--fill",
                       help='fill the tables with data',
                       action="store_true",
                       )
    execute = parser.add_argument_group("Execution")
    execute.add_argument("--run",
                         help='execute scratch code',
                         action="store",
                         type=str,
                         nargs=1  #"+" on or more
                         )
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true",
                        )
    args = parser.parse_args()

    return args

# ToDo This method is deprecated
def parse_tables(table_list):
    table = table_list[4]
    # print(f'The first table is: {table}')
#     Prune out the ontology schema

    new_list = [table[9:] for table in table_list]
    # print(new_list)

def main():
    args = parse_arguments()
    if args.verbose:
        print('----------------------------')
        print('       Welcome to build_database')

    database = NewDatabaseSchema()
    ontology = OntologySchema()

    if args.create:
        database.create_database()
    try:
        database.connect_engine()
    except sqlalchemy.exc.OperationalError:
        print("Error: Could not connect. New database not found")
        return
    try:
        ontology.connect_engine()
    except sqlalchemy.exc.OperationalError:
        print("Error: Could not connect. Ontology database not found")
        return
    if args.table:
        database.build_tables()
    else:
        database.connect_tables()
    if args.reflect:
        # ToDo move this to a good place
        table_list = ontology.reflect_tables()
        parse_tables(table_list)
    else:
        ontology.connect_tables()
    if args.enumerate:
        ontology.enumerate_tables()
    if args.check:
        result = ontology.check_tables()
        df = pd.DataFrame(result,
                          columns=result[0].keys())
        df.to_csv('table validation.csv')
    if args.fill:
        ontology.test_fill()
        # database.insert_types()
        # scratch.insert_rows()
        pass
    if args.run is not None:
        print(f'run = {args.run}')
        command = args.run[0]
        match command:
            case "loop":
                ontology.simple_loop(ontology.map_list[-1])
            case "column":
                ontology.column_test()
            case "one":
                ontology.is_one_to_one()
            case "reflect":
                pass
            case _:
                print(f'command not found: {command}')
    if args.drop:
        database.drop_database()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


# def main():
#     args = parse_arguments()
#     if args.verbose:
#         print('----------------------------')
#         print('Main Called')
#
# # ToDo put in more precise commands: create, fill, etc
#     if args.command == 'run':
#         database = NewDatabaseSchema()
#         database.create_database()
#         database.connect_engine()
#         database.build_tables()
#         database.insert_types()
#
#         ontology = OntologySchema()
#         ontology.connect_engine()
#         ontology.connect_tables()
#         ontology.loop_over()
#     elif args.command == 'drop':
#         database = NewDatabaseSchema()
#         database.drop_database()
#     elif args.command == 'test':
#         # Place holder for test command
#         pass
#     else:
#         print(f'unrecognized command: {args.command}')
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     main()
