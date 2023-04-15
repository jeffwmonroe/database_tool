import copy
import functools
import time
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy as sqla
from sqlalchemy.exc import OperationalError

import database_tools.database_connection.enums as db_enum


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
    table_name: str,
    df: pd.DataFrame,
    engine: sqla.Engine,
    meta_data: sqla.MetaData,
) -> None:
    """
    This function will load one table into the database from a pandas dataframe.
    If the dataframe has an n_id column, the load will modify existing things.
    If the dataframe does not have an n_id column the load will create new
    things.
    :param table_name: name of the table, in the database, for the load
    :param df: pandas dataframe containing the data to load
    :param engine: SqlAlchemy Engine that needs to be connected to the database
    :param meta_data: SqlAlchemy MetaData that needs to be connected to the database.
    :return: None
    """
    print("load_thing_table")
    print(f"   table_name = {table_name}")
    load_table = meta_data.tables[table_name]
    nrows = len(df.index)
    thing_table = meta_data.tables["thing"]
    if "n_id" in df.keys():
        df["action"] = ["modify"] * nrows
        # ToDo need to check that the n_id pks are correct
        n_ids = df["n_id"].to_list()
        stmt = sqla.select(thing_table).where(thing_table.c["n_id"].in_(n_ids))
        with engine.connect() as new_connection:
            result = new_connection.execute(stmt)
            for row in result:
                if row.thing != table_name:
                    print(
                        f"error: n_id:{row.n_id} corresponds to {row.thing} not {table_name}"
                    )
                    return
        # stmt = sqla.select(load_table.c['n_id'],
        #                    load_table.c['name'],
        #                    ).join(thing_table)
    else:
        # ToDo refactor this
        df["n_id"] = generate_new_id_pks(len(df.index), table_name, thing_table, engine)
        df["action"] = ["create"] * nrows

    values = df.to_dict(orient="records")
    with engine.connect() as new_connection:
        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()


def load_thing_table_from_file(
    table_name: str,
    file_name: str,
    engine: sqla.Engine,
    meta_data: sqla.MetaData,
    sheet_name=None,
) -> None:
    if sheet_name is None:
        df = pd.read_excel(file_name)
    else:
        df = pd.read_excel(file_name, sheet_name=sheet_name)
    load_thing_table(table_name, df, engine, meta_data)


def load_many_thing_tables_from_file(
    table_name: str,
    file_name: str,
    number_of_files: int,
    engine: sqla.Engine,
    meta_data: sqla.MetaData,
) -> None:
    for index in range(number_of_files):
        sheet_name = f"load-{index + 1:d}"
        print(f"sheet_name = ({sheet_name})")
        df = pd.read_excel(file_name, sheet_name=sheet_name)
        load_thing_table(table_name, df, engine, meta_data)


def print_row_header() -> None:
    print(
        f'{"log_id":<7s} {"n_id":<5} {"action":<8} {"time_string":<12} {"user":<15} {"name":<30} {"status":<10}'
    )


def print_row(
    row: tuple[int, int, db_enum.Action, datetime, str, db_enum.Status, str]
) -> None:
    log_id = row[0]
    n_id = row[1]
    action = db_enum.action_to_str(row[2])
    the_time: datetime = row[3]
    time_string = f"{the_time.year}-{the_time.month}-{the_time.day}"
    user = row[4]
    status = db_enum.status_to_str(row[5])
    name = row[6]
    print(
        f"{log_id:<7d} {n_id:<5d} {action:<8} {time_string:<12} {user:<15} {name:<30} {status:<10}"
    )


@timer
def get_latest_thing(
    table_name: str,
    engine: sqla.Engine,
    meta_data: sqla.MetaData,
    status: db_enum.Status | None = None,
    action: db_enum.Action | None = None,
    n_id: int | None = None,
    latest: bool = False,
) -> None:
    table: sqla.Table = meta_data.tables[table_name]
    # stmt = sqla.select(table).where(table.c['status'] == db_enum.Status.draft)
    if status is None:
        status_sub_query = sqla.select(table).subquery()
    else:
        status_sub_query = (
            sqla.select(table).where(table.c["status"] == status).subquery()
        )
    if action is None:
        action_sub_query = sqla.select(status_sub_query).subquery()
    else:
        action_sub_query = (
            sqla.select(status_sub_query)
            .where(status_sub_query.c["action"] == action)
            .subquery()
        )
    if n_id is None:
        nid_sub_query = sqla.select(action_sub_query).subquery()
    else:
        nid_sub_query = (
            sqla.select(action_sub_query)
            .where(action_sub_query.c["n_id"] == n_id)
            .subquery()
        )
    if latest:
        latest_sub_query = (
            sqla.select(
                sqla.func.max(nid_sub_query.c["log_id"]).label("max_log"),
                sqla.func.max(nid_sub_query.c["created_ts"]),
            )
            .group_by("n_id")
            .subquery()
        )
        latest_sub_query2 = (
            sqla.select(
                table,
            )
            .join(latest_sub_query, latest_sub_query.c["max_log"] == table.c["log_id"])
            .subquery()
        )
    else:
        latest_sub_query2 = sqla.select(nid_sub_query).subquery()

    stmt = sqla.select(latest_sub_query2)
    index = 0
    print_row_header()
    with engine.connect() as connection:
        result = connection.execute(stmt)
        number_of_rows = result.rowcount
        for row in result:
            print_row(row)
            index += 1
            if index > 101:
                break

    print(f"Total rows found = {number_of_rows}")


@timer
def create_additional_things(
    table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData
) -> None:
    print("create addditional things")
    table = meta_data.tables[table_name]
    stmt = sqla.select(table)
    row_list = []
    names = {}
    with engine.connect() as connection:
        result = connection.execute(stmt)
        for row in result:
            row_dict = row._asdict()
            row_dict["action"] = db_enum.Action.modify
            row_dict.pop("log_id", None)
            names[row_dict["n_id"]] = row_dict["name"]
            # print(f'row = {row_dict}')
            row_list.append(row_dict)
    print(f"query complete num rows = {len(row_list)}")
    rows_to_add = []
    for index in range(additional):
        print(f"   index = {index}")
        row_list_copy = copy.deepcopy(row_list)
        for row in row_list_copy:
            row["created_ts"] = row["created_ts"] + timedelta(days=(1 + index))
            row["name"] = names[row["n_id"]] + f"-{index}"
            if index % 5 == 0:
                row["status"] = db_enum.Status.stage
            if index % 13 == 0:
                row["status"] = db_enum.Status.production

        rows_to_add = rows_to_add + row_list_copy
    # print(rows_to_add)
    print(f"total rows to add = {len(rows_to_add)}")
    with engine.connect() as new_connection:
        insert_type = table.insert()
        new_connection.execute(insert_type, rows_to_add)
        new_connection.commit()


def create_additional_things2(
    table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData
) -> None:
    print("create_additional_things")
    table_df = pd.read_sql_table(table_name, engine.connect())
    print("database is connected")
    new_dfs = []
    for index in range(additional):
        print(f"   index = {index}")
        copy_df = table_df.copy()
        copy_df["action"] = ["modify"] * len(copy_df.index)
        copy_df["name"] = copy_df["name"] + f"-{index:d}"
        copy_df["created_ts"] = copy_df["created_ts"] + timedelta(days=1 * (index + 1))
        new_dfs.append(copy_df)
    result_df = pd.concat(new_dfs)
    result_df = result_df.drop("log_id", axis=1)
    print(result_df)
    load_table = meta_data.tables[table_name]
    values = result_df.to_dict(orient="records")
    # print(values)
    with engine.connect() as new_connection:
        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()
