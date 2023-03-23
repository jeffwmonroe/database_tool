import argparse

import sqlalchemy
from sqlalchemy import text, insert
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String, DATE
from sqlalchemy import ForeignKey
from sqlalchemy import select, bindparam, join
from sqlalchemy import func, cast, literal_column, and_, or_, desc, asc
from build_database.database_connection import DatabaseConnection


class Scratch(DatabaseConnection):
    def __init__(self):
        super().__init__('postgresql+psycopg2://postgres:aqua666@localhost:5432/scratch')
        self.practice_table = None

    def connect_tables(self, commit=False):
        """
        Builds the tables for the database
        :return:
        """
        if self.practice_table is not None:
            print("Error: tables already exist")

        self.practice_table = Table(
            "practice",
            self.metadata_obj,
            Column("id", Integer, primary_key=True),
            Column("identifier", String(30)),
            Column("date", DATE),
            Column("value", Integer),
        )
        if commit:
            self.metadata_obj.create_all(self.engine)

    def insert_rows(self):
        with self.engine.connect() as conn:
            result = conn.execute(
                insert(self.practice_table),
                [
                    {"identifier": "A", "date": "2017-01-01", "value": 2},
                    {"identifier": "A", "date": "2017-01-02", "value": 1},
                    {"identifier": "A", "date": "2017-01-03", "value": 7},
                    {"identifier": "B", "date": "2017-01-01", "value": 2},
                    {"identifier": "B", "date": "2017-01-02", "value": 7},
                    {"identifier": "B", "date": "2017-01-03", "value": 3},
                    {"identifier": "A", "date": "2017-01-03", "value": 8},
                ],
            )
            conn.commit()

    def show_all(self):
        stmt = select(self.practice_table)
        self.connect_and_print(stmt)

    def is_one_to_one(self):
        stmt = select(
            self.practice_table.c.identifier,
            func.count(self.practice_table.c.identifier),
        ).group_by(
            self.practice_table.c.identifier,
        ).order_by(
            asc(self.practice_table.c.identifier),
            # desc(self.practice_table.c.identifier),
        )

        self.connect_and_print(stmt)

    def max_value_query(self):
        print('--------------------------------------------------')
        print('--------------------------------------------------')

        subq = select(self.practice_table.c.identifier,
                      func.max(self.practice_table.c.date).label("maxdate"),
                      func.max(self.practice_table.c.value).label("maxvalue"),
                      ).group_by(self.practice_table.c.identifier).subquery()

        query = (select(self.practice_table.c.id,
                        subq.c.identifier,
                        subq.c.maxdate,
                        self.practice_table.c.value)
                 .join_from(self.practice_table,
                            subq,
                            and_(self.practice_table.c.identifier == subq.c.identifier,
                                 subq.c.maxdate == self.practice_table.c.date,
                                 # subq.c.maxvalue == self.practice_table.c.value,
                                 )))

        self.connect_and_print(query)


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
                       help='build the tables in a fresh database',
                       action="store_true",
                       )
    table.add_argument("-f", "--fill",
                       help='fill the tables with data',
                       action="store_true",
                       )
    execute = parser.add_argument_group("Execution")
    execute.add_argument("-r", "--run",
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
    # print(f'parser = {args.execute}')

    # if args.verbose:
    #     print('Verbose is on')
    return args


def main():
    args = parse_arguments()
    if args.verbose:
        print('----------------------------')
        print('       Welcome to scratch')
    scratch = Scratch()

    if args.create:
        scratch.create_database()
    scratch.connect_engine()
    if args.table:
        scratch.build_tables()
    else:
        scratch.connect_tables()
    if args.fill:
        scratch.insert_rows()
    if args.run is not None:
        print(f'run = {args.run}')
        command = args.run[0]
        match command:
            case "max":
                scratch.max_value_query()
            case "all":
                scratch.show_all()
            case "one":
                scratch.is_one_to_one()
            case _:
                print(f'command not found: {command}')
    if args.drop:
        scratch.drop_database()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
