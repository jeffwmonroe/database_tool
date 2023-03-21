from new_schema import NewDatabaseSchema
from old_schema import OntologySchema
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser("Manipulate the ontology and new schema databases")
    # parser.add_argument("echo")
    parser.add_argument("command",
                        help='database command',
                        type=str,
                        choices=['run', 'drop', 'test'])
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")
    # parser.add_argument("-j", "--jeff",
    #                     help="jeffness turned on",
    #                     type=str,
    #                     default="Jeff"
    #                     )
    args = parser.parse_args()


    if args.verbose:
        print('Verbose is on')
    return args


def main():
    args = parse_arguments()
    if args.verbose:
        print('----------------------------')
        print('Main Called')

    if args.command == 'run':
        database = NewDatabaseSchema()
        database.create_database()
        database.connect_engine()
        database.build_tables()
        database.insert_types()

        ontology = OntologySchema()
        ontology.connect_engine()
        ontology.build_tables()
        ontology.loop_over()
    elif args.command == 'drop':
        database = NewDatabaseSchema()
        database.drop_database()
    elif args.command == 'test':
        # Place holder for test command
        pass
    else:
        print(f'unrecognized command: {args.command}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
