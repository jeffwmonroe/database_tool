import pandas as pd
from pydantic import BaseModel
from collections.abc import Sequence

# ToDo move this to a .env vile
table_list = "../../data/table_list.xlsx"


def read_vendor_info() -> dict[str, list[str]]:
    print('---------------------------------------')
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


def read_table_info() -> dict[str, list[str]]:
    print('---------------------------------------')
    print('new_schema: read_table_info')
    df = pd.read_excel(table_list,
                       sheet_name='tables',
                       keep_default_na=False)
    tables = {}
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
            columns.append((col_name, col_type))
        tables[table_name] = columns
    return tables


class Column(BaseModel):
    """
    Column is the class for a data column outside of the standard columns
    """
    name: str
    data_type: str
    foreign_table: str
    foreign_column: str


class DataTable(BaseModel):
    """
    DataTable base model definition
    """
    name: str
    columns: dict[str, Column]
    vendors: list[str]


def main():
    print("schema")
    vendors = read_vendor_info()
    print(vendors)
    table = read_table_info()
    print(table)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
