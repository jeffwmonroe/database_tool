import functools
import time

import pandas as pd
import sqlalchemy as sqla

import database_tools.database_connection.enums as db_enum
from datetime import timedelta


# ToDo move this to a different utility module
def timer(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        value = function(*args, **kwargs)
        end_time = time.perf_counter()
        duration = end_time - start_time
        print(f"Total duration: {duration}")
        return value

    return wrapper


def generate_new_id_pks(
        number: int, thing_name: str, thing_table: sqla.Table, engine: sqla.Engine
) -> list[int]:
    thing_table_values: list[dict[str, str]] = [
        {"thing": thing_name} for index in range(number)
    ]
    with engine.connect() as new_connection:
        insert_thing = thing_table.insert().returning(sqla.column("n_id"))
        result: sqla.CursorResult = new_connection.execute(
            insert_thing, thing_table_values
        )
        pks: list[int] = [row[0] for row in result]
        for row in result:
            print(f"row = {row}")
        new_connection.commit()
    return pks


def load_thing_table(
        table_name: str, df: pd.DataFrame, engine: sqla.Engine, meta_data: sqla.MetaData,
) -> None:
    load_table = meta_data.tables[table_name]
    nrows = len(df.index)
    if "n_id" in df.keys():
        df['action'] = ['modify'] * nrows
    else:
        # ToDo refactor this
        thing_table = meta_data.tables["thing"]
        df["n_id"] = generate_new_id_pks(
            len(df.index), table_name, thing_table, engine
        )
        df['action'] = ['create'] * nrows

    values = df.to_dict(orient="records")
    with engine.connect() as new_connection:
        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()


def load_thing_table_from_file(
        table_name: str, file_name: str, engine: sqla.Engine, meta_data: sqla.MetaData,
) -> None:
    df = pd.read_excel(file_name)
    load_thing_table(table_name, df, engine, meta_data)


def load_many_thing_tables_from_file(
        table_name: str, file_name: str, number_of_files: int, engine: sqla.Engine, meta_data: sqla.MetaData,
) -> None:
    for index in range(number_of_files):
        sheet_name = f'load-{index + 1:d}'
        print(f'sheet_name = ({sheet_name})')
        df = pd.read_excel(file_name, sheet_name=sheet_name)
        load_thing_table(table_name, df, engine, meta_data)


@timer
def get_latest_thing(table_name: str, engine: sqla.Engine, meta_data: sqla.MetaData) -> None:
    table = meta_data.tables[table_name]
    # stmt = sqla.select(table).where(table.c['status'] == db_enum.Status.draft)
    sub_query1 = sqla.select(table)#.where(table.c['n_id'] == 56894).subquery()
    sub_query2 = sqla.select(sqla.func.max(sub_query1.c['log_id']).label('max_log'),
                             sqla.func.max(sub_query1.c['created_ts'])).group_by('n_id').subquery()
    stmt = sqla.select(sub_query2,
                       table.c['name'],
                       table.c['n_id']).join(table, sub_query2.c['max_log'] == table.c['log_id'])

    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        for row in result:
            print(f'row = {row}')
            index += 1
            if index > 10:
                break

@timer
def create_additional_things(table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData) -> None:
    print('create addditional things')
    table = meta_data.tables[table_name]
    stmt = sqla.select(table)
    row_list = []
    names = {}
    with engine.connect() as connection:
        result = connection.execute(stmt)
        for row in result:
            row_dict = row._asdict()
            row_dict['action'] = db_enum.Action.modify
            row_dict.pop('log_id', None)
            names[row_dict['n_id']] = row_dict['name']
            # print(f'row = {row_dict}')
            row_list.append(row_dict)
    print(f'query complete num rows = {len(row_list)}')
    rows_to_add = []
    for index in range(additional):
        print(f'   index = {index}')
        for row in row_list:
            row['created_ts'] = row['created_ts'] + timedelta(days=1)
            row['name'] = names[row['n_id']] + f'-{index}'
        rows_to_add = rows_to_add + row_list
    # print(rows_to_add)
    print(f'total rows to add = {len(rows_to_add)}')
    with engine.connect() as new_connection:
        insert_type = table.insert()
        new_connection.execute(insert_type, rows_to_add)
        new_connection.commit()


def create_additional_things2(table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData) -> None:
    print('create_additional_things')
    table_df = pd.read_sql_table(table_name, engine.connect())
    print('database is connected')
    new_dfs = []
    for index in range(additional):
        print(f'   index = {index}')
        copy_df = table_df.copy()
        copy_df['action'] = ['modify'] * len(copy_df.index)
        copy_df['name'] = copy_df['name'] + f'-{index:d}'
        copy_df['created_ts'] = copy_df['created_ts'] + timedelta(days=1 * (index + 1))
        new_dfs.append(copy_df)
    result_df = pd.concat(new_dfs)
    result_df = result_df.drop('log_id', axis=1)
    print(result_df)
    load_table = meta_data.tables[table_name]
    values = result_df.to_dict(orient="records")
    # print(values)
    with engine.connect() as new_connection:
        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()
