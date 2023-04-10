import argparse
import json

import pandas as pd
from json_schema import (
    JsonColumn,
    JsonDataTable,
    JsonSchema,
    read_schema_json,
)

# ToDo move this to a .env vile
table_list = "../../data/table_list.xlsx"


def read_vendor_info() -> dict[str, list[str]]:
    print("---------------------------------------")
    print("read_vendor_info")  # ToDo fix the file name
    df = pd.read_excel(table_list, sheet_name="vendors", keep_default_na=False)
    table_vendors = {}
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        vendors = []
        for val in row_list[1:]:
            if val != "":
                vendors.append(val)
        table_vendors[table_name] = vendors
    return table_vendors


def read_table_info(vendors: dict[str, list[str]]) -> list[JsonDataTable]:
    print("---------------------------------------")
    print("new_schema: read_table_info")
    df = pd.read_excel(table_list, sheet_name="tables", keep_default_na=False)
    tables = []
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        row_list = row_list[1:]
        columns = []
        while len(row_list) > 1:
            col_name = row_list[0]
            col_type = row_list[1]
            row_list = row_list[2:]
            if col_name == "" or col_type == "":
                break
            # columns[col_name] = (col_name, col_type)
            columns.append(JsonColumn(name=col_name, data_type=col_type))
        tables.append(
            JsonDataTable(
                name=table_name,
                columns=columns,
                vendors=(vendors[table_name] if table_name in vendors.keys() else []),
            )
        )

    return tables


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Build Schema",
        description="Schema Build Tool",
        epilog="Thanks for practicing with %(prog)s! :-)",
        allow_abbrev=False,
    )
    database = parser.add_argument_group("build jason_schema")
    database.add_argument(
        "-v",
        "--verbose",
        help="verbose output",
        action="store_true",
    )
    database.add_argument(
        "-b",
        "--build",
        help="build jason_schema JSON from excel file",
        action="store_true",
    )
    database.add_argument(
        "-r",
        "--read",
        help="read the JSON file",
        action="store_true",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    if args.verbose:
        print("---------------------------------")
        print("       Welcome to build_schema")

    # ToDo move this to a .env vile
    json_file = "../../data/tables.json"
    if args.build:
        vendors = read_vendor_info()
        tables = read_table_info(vendors)
        schema = JsonSchema(tables=tables)

        json_object = json.dumps(schema.dict(), indent=4)
        with open(json_file, "w") as outfile:
            outfile.write(json_object)
    if args.read:
        schema = read_schema_json(json_file)
        schema.pprint()


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main()
