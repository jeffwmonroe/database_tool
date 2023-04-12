import pandas as pd
# import sqlalchemy as sqla
from database_tools import (NewDatabaseSchema, OntologySchema, load_thing_table_from_file,
                            load_many_thing_tables_from_file, get_latest_thing,
                            create_additional_things,
                            str_to_action, str_to_status,
                            )
import argparse
from sqlalchemy.exc import OperationalError


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="scratch",
        description="Database practice tool",
        epilog="Thanks for practicing with %(prog)s! :-)",
        allow_abbrev=False,
    )
    # parser.add_argument("echo")
    database = parser.add_argument_group("database tools")
    database.add_argument(
        "-c",
        "--create",
        help="create a new database",
        action="store_true",
    )
    database.add_argument(
        "--drop",
        help="drop the database",
        action="store_true",
    )
    database.add_argument(
        "-f",
        "--fill",
        help="fill the tables with data",
        action="store_true",
    )
    table = parser.add_argument_group("table tools")
    table.add_argument(
        "-t",
        "--table",
        help="build the new jason_schema tables in a fresh database",
        action="store_true",
    )
    table.add_argument(
        "-r",
        "--reflect",
        help="reflect the tables from the ontology database",
        action="store_true",
    )
    table.add_argument(
        "-e",
        "--enumerate",
        help="go over the tables and build structure",
        action="store_true",
    )
    table.add_argument(
        "--check",
        help="perform checks on all of the reflected and enumerated tables",
        action="store_true",
    )
    table.add_argument(
        "-s",
        "--short",
        help="only use the first 10 rows",
        action="store_true",
    )
    execute = parser.add_argument_group("Execution")
    execute.add_argument(
        "--load",
        help="load in a single import file",
        action="store",
        type=str,
        metavar=('<table>', '<filename>'),
        nargs=2,  # "+" on or more
    )
    execute.add_argument(
        "--load_series",
        help="Load a series of import files",
        action="store",
        type=str,
        metavar=('<table name>', '<file name>', '<number of tabs>'),
        nargs=3,  # "+" on or more
    )
    execute.add_argument(
        "-a",
        "--additional",
        help="add additional rows for stress testing",
        action="store",
        type=str,
        metavar=('<table name>', '<number of duplicates>'),
        nargs=2,  # "+" on or more
    )
    execute.add_argument(
        "-q",
        "--query",
        help="query the thing table",
        # nargs=1,
        # type=str,
        # metavar='<table name>',
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity",
        action="store_true",
    )
    query = parser.add_argument_group("Load Flags")
    query.add_argument(
        "--sheet",
        help="name of sheet to load",
        type=str,
        nargs=1,
        metavar='<sheet name>',
        action="store",
    )
    query = parser.add_argument_group("Query Flags")
    query.add_argument(
        "-n",
        "--n_id",
        help="name id (n_id) for query",
        type=int,
        metavar='<primary key>',
        default=None,
        action="store",
    )
    query.add_argument(
        "--status",
        help="status for query",
        choices=['draft', 'stage', 'production'],
        default=None,
        type=str,
        action="store",
    )
    query.add_argument(
        "--action",
        help="action for query",
        choices=['create', 'modify', 'delete'],
        default=None,
        type=str,
        action="store",
    )
    query.add_argument(
        "--latest",
        help="query only the most recent value",
        action="store_true",
    )
    args = parser.parse_args()

    return args


def main():
    args = parse_arguments()
    if args.verbose:
        print("----------------------------")
        print("       Welcome to build_database")

    database = NewDatabaseSchema()
    ontology = OntologySchema()

    if args.drop:
        database.drop_database()
    if args.create:
        database.create_database()
    try:
        database.connect_engine()
    except OperationalError:
        if args.drop:
            return
        print("Error: Could not connect to new database. New database not found")
        return
    try:
        ontology.connect_engine()
    except OperationalError:
        print("Error: Could not connect. Ontology database not found")
        return
    if args.table:
        database.build_tables()
    else:
        database.connect_tables()
    if args.reflect:
        ontology.reflect_tables()
    else:
        ontology.connect_tables()
    if args.enumerate:
        ontology.enumerate_tables()
    if args.check:
        validation_results = ontology.check_tables()
        validation_column_names = [column_name for column_name in validation_results[0].keys()]
        df = pd.DataFrame(validation_results, columns=validation_column_names)
        # ToDo move this to an environment
        df.to_csv("./data/table validation.csv")
    if args.short:
        short_load: bool = True
    else:
        short_load: bool = False
    if args.fill:
        # ontology.test_fill(database) # old style
        database.fill_tables(ontology, short_load)
    if args.load is not None:
        print(f"load = {args.load}")

        table_name = args.load[0]
        file_name = args.load[1]
        sheet_name = None
        if args.sheet:
            sheet_name = args.sheet[0]
        load_thing_table_from_file(table_name, file_name, database.engine, database.metadata_obj, sheet_name)
    if args.load_series is not None:
        print(f"load_series = {args.load_series}")

        table_name = args.load_series[0]
        file_name = args.load_series[1]
        number_of_files = int(args.load_series[2])
        load_many_thing_tables_from_file(table_name, file_name, number_of_files,
                                         database.engine, database.metadata_obj)
    if args.query:
        nid = args.n_id  # 56894
        status = str_to_status(args.status)
        action = str_to_action(args.action)
        latest = args.latest
        print(f'query: status = {status}')
        print(f'   n_id  = {nid}')
        print(f'   action = {action}')
        # ontology.test_fill(database) # old style
        # table_name = args.query[0],
        get_latest_thing('artist',
                         database.engine,
                         database.metadata_obj,
                         status=status,
                         n_id=nid,
                         action=action,
                         latest=latest,
                         )
    if args.additional is not None:
        print('inside of args.additional')
        table_name = args.additional[0]
        print(f'table name = {table_name}')
        additional = int(args.additional[1])
        print(f'additional = {additional}')
        # ontology.test_fill(database) # old style
        create_additional_things(table_name, additional, database.engine, database.metadata_obj)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()
