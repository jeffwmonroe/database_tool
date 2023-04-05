import sqlalchemy as sqla
import pandas as pd
from database_tools.database_connection.utilities import build_sql_column, build_data_table


# ToDo move this to a .env vile
table_list = "./data/table_list.xlsx"


def read_table_info(metadata: sqla.MetaData) -> dict[sqla.Table]:
    print('---------------------------------------')
    print('new_schema: read_table_info')
    df = pd.read_excel(table_list,
                       sheet_name='tables',
                       keep_default_na=False)
    data_tables: dict[sqla.Table] = {}
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        row_list = row_list[1:]
        # print(f'   table name: {table_name}')
        data_columns = []
        while len(row_list) > 1:
            col_name = row_list[0]
            col_type = row_list[1]
            row_list = row_list[2:]
            if col_name == '' or col_type == '':
                break
            data_columns.append(build_sql_column(col_name, col_type))
        data_tables[table_name] = build_data_table(metadata,
                                                   table_name,
                                                   use_name=True,
                                                   extra_data_columns=data_columns)
    return data_tables


def read_vendor_info() -> dict[str, list[str]]:
    print('---------------------------------------')
    print('read_vendor_info') \
        # ToDo fix the file name
    df = pd.read_excel(table_list,
                       sheet_name='vendors',
                       keep_default_na=False)
    vendors: dict[str, list[str]] = {}
    for row_index, row in df.iterrows():
        row_list = list(row.values)
        table_name = row_list[0]
        vendor_list = []
        for val in row_list[1:]:
            if val != '':
                vendor_list.append(val)
        vendors[table_name] = vendor_list
    return vendors
