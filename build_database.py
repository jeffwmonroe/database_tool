import pandas as pd
# import sqlalchemy as sqla
from database_tools import NewDatabaseSchema, OntologySchema, load_thing_table_from_file
import argparse
from sqlalchemy.exc import OperationalError


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="scratch",
        description="Database practice tool",
        epilog="Thanks for practicing with %(prog)s! :-)",
        allow_abbrev=False,
        # argument_default=argparse.SUPPRESS,
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
    table.add_argument(
        "-f",
        "--fill",
        help="fill the tables with data",
        action="store_true",
    )
    execute = parser.add_argument_group("Execution")
    execute.add_argument(
        "--load",
        help="execute scratch code",
        action="store",
        type=str,
        nargs=2,  # "+" on or more
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity",
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
        df.to_csv("./data/table validation.csv")
    if args.short:
        short_load: bool = True
    else:
        short_load: bool = False
    if args.fill:
        # ontology.test_fill(database) # old style
        database.fill_tables(ontology, short_load)
        pass
    if args.load is not None:
        print(f"load = {args.load}")

        table_name = args.load[0]
        file_name = args.load[1]
        load_thing_table_from_file(table_name, file_name, database)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()
