import pandas as pd
from pydantic import BaseModel
from collections.abc import Sequence
import json
# ToDo move this to a .env vile
table_list = "../../data/table_list.xlsx"
json_file = "../../data/tables.json"


class Column(BaseModel):
    """
    Column is the class for a data column outside of the standard columns
    """
    name: str
    data_type: str
    # foreign_table: str
    # foreign_column: str


class DataTable(BaseModel):
    """
    DataTable base model definition
    """
    name: str
    columns: list[Column]
    vendors: list[str]


class Schema(BaseModel):

    tables: list[DataTable]


def read_vendor_info() -> dict[str, list[str]]:
    print("---------------------------------------")
    print('read_vendor_info') \
        # ToDo fix the file name
    df = pd.read_excel(table_list,
                       sheet_name='vendors',
                       keep_default_na=False)
    table_vendors = {}
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        print(f'   table name: {table_name}')
        vendors = []
        for val in row_list[1:]:
            if val != '':
                vendors.append(val)
        table_vendors[table_name] = vendors
    return table_vendors


def read_table_info(vendors: dict[str, list[str]]) -> list[DataTable]:
    print('---------------------------------------')
    print('new_schema: read_table_info')
    df = pd.read_excel(table_list,
                       sheet_name='tables',
                       keep_default_na=False)
    tables = []
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        row_list = row_list[1:]
        print(f'   table name: {table_name}')
        columns = []
        while len(row_list) > 1:
            col_name = row_list[0]
            col_type = row_list[1]
            row_list = row_list[2:]
            if col_name == '' or col_type == '':
                break
            # columns[col_name] = (col_name, col_type)
            columns.append(Column(name=col_name,
                                  data_type=col_type))
        tables.append(DataTable(name=table_name,
                                columns=columns,
                                vendors=(vendors[table_name] if table_name in vendors.keys() else []),
                                ))

    return tables


def main():
    print("schema")
    vendors = read_vendor_info()
    tables = read_table_info(vendors)
    schema = Schema(tables=tables)

    tables = {"tables": tables}
    print(schema.dict())
    json_object = json.dumps(schema.dict(), indent=4)
    with open(json_file, "w") as outfile:
        outfile.write(json_object)

    # Data to be written
    dictionary = {
        "name": "sathiyajith",
        "rollno": 56,
        "cgpa": 8.6,
        "phonenumber": "9976770500"
    }

    # Serializing json
    json_object = json.dumps(dictionary, indent=4)

    # Writing to sample.json
    with open("..\..\data\sample.json", "w") as outfile:
        outfile.write(json_object)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
