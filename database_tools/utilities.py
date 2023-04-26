"""
Basic utilities for dealing with the database.

These functions help with the database and are used by the classes in database_connection:

- OntologySchema
- NewSchema
"""
import copy
import csv
import functools
import time
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy as sqla

import database_tools.database_connection.enums as db_enum
from database_tools.database_connection.old_schema import enumerated_thing_list
from database_tools.database_connection.ontology_table import OntologyTable
from database_tools.transfer_table.utilities import is_standard_column


# ToDo move this to a different utility module
def timer(function):
    """
    Record and print the execution time of a function.

    This function is to be used as a decorator to functions. It will record the time
    that the function starts and stops, and will print the total duration to STDOUT.

    :param function: The function to be timed.
    :return: the wrapper function
    """

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
        number_of_keys: int, thing_name: str, thing_key_table: sqla.Table, engine: sqla.Engine
) -> list[int]:
    """
    Generate primary keys for thing table.

    The primary key in all of the thing tables is the log_id. This is what enables
    the system to track changes. Therefore, the primary keys  for the actual thing need
    to be stored in a separate table. This allows us to enforce uniqueness on the thing
    primary keys.

    This function will create a number of keys and will return it so that other functions
    can add the new items to the thing table.

    :param number_of_keys: The number of new primary keys to generate.
    :param thing_name: The name of the thing to add.
    :param thing_key_table: The SqlAlchemy table for the think keys
    :param engine: Properly initialized SqlAlchemy engine
    :return: Returns a list of ints (primary keys)
    """
    thing_table_values: list[dict[str, str]] = [
        {"thing": thing_name} for index in range(number_of_keys)
    ]
    with engine.connect() as new_connection:
        insert_thing = thing_key_table.insert().returning(sqla.column("n_id"))
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
    Load one table into the database from a pandas dataframe.

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
    """
    Read an Excel file and load the contents into a thing table.

    This function uses pandas to read in the Excel file and then calls load_thing_table.
    Pandas has the nice side effect of ensuring that all of the types in each column
    are homogeneous.

    :param table_name: Name of the thing table.
    :param file_name: Name of the Excel file with the data
    :param engine: Properly initialized SqlAlchemy Engine.
    :param meta_data: SqlAlchemy initialized MetaData
    :param sheet_name: Name of the sheet with the thing data. None will load the first sheet.
    :return: None
    """
    if sheet_name is None:
        df = pd.read_excel(file_name)
    else:
        df = pd.read_excel(file_name, sheet_name=sheet_name)
    load_thing_table(table_name, df, engine, meta_data)


def load_many_thing_tables_from_file(
        table_name: str,
        file_name: str,
        number_of_sheets: int,
        engine: sqla.Engine,
        meta_data: sqla.MetaData,
) -> None:
    """
    Load multiple sheets from an Excel file and load them into a thing table.

    This will load in multiple sheets from the Excel file. THe sheets need to be of the form:
    load-#. The first sheet is load-1. The function always starts with the first sheet.

    # ToDo add functionality to start with a sheet other than 1

    :param table_name: Name of the thing table to get loaded data.
    :param file_name: Name of the Excel File to load
    :param number_of_sheets: Number of sheets to load
    :param engine: Properly initialized SqlAlchemy Engine.
    :param meta_data: SqlAlchemy initialized MetaData
    :return: None
    """
    for index in range(number_of_sheets):
        sheet_name = f"load-{index + 1:d}"
        print(f"sheet_name = ({sheet_name})")
        df = pd.read_excel(file_name, sheet_name=sheet_name)
        load_thing_table(table_name, df, engine, meta_data)


def print_row_header() -> None:
    """
    Print a nicely formatted header for standard thing columns.

    Simple helper function to print the standard columns for a thing table. This is formatted
    and spaced to make it easy for a person to read. The column spacing is paired with print_row.

    return: None
    """
    print(
        f'{"log_id":<7s} {"n_id":<5} {"action":<8} {"time_string":<12} {"user":<15} {"name":<30} {"status":<10}'
    )


def print_row(
        row: tuple[int, int, db_enum.Action, datetime, str, db_enum.Status, str]
) -> None:
    """
    Print a nicely formatted row of data from a thing table.

    This will print the standard columns from a thing table in a nicely formatted and spaced manner
    so that it is human-readable.

    :param row: Named tuple from the result of a database query.
    :return: None
    """
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


def make_row_dict(row, column_names):
    """
    Make a dictionary from a result tuple.

    **Note** Pandas has difficulty processing DataTimes with timezone information. It takes an order
    of magnitude longer to manage this data. For this reason the timezone information is stripped out
    of the dictionary.

    **Note:** for the status and action columns the enums are converted to str so that they are
    more easily read by people.

    :param row: Tuple that is the result of a SqLAlchemy query.
    :param column_names: Names of the columns in the Tuple
    :return: dictionary.
    """
    row_dict = {}
    for index in range(len(column_names)):
        value = row[index]
        if column_names[index] == "created_ts":
            value = value.replace(tzinfo=None)
        if column_names[index] == "status":
            value = db_enum.status_to_str(value)
        if column_names[index] == "action":
            value = db_enum.action_to_str(value)
        row_dict[column_names[index]] = value
    return row_dict


@timer
def query_thing_table(
        table_name: str,
        engine: sqla.Engine,
        meta_data: sqla.MetaData,
        status: db_enum.Status | None = None,
        action: db_enum.Action | None = None,
        n_id: int | None = None,
        latest: bool = False,
        filename: str = None,
) -> None:
    """
    Query a thing table.

    This is a general query function for the thing tables. This was developed primarily for testing
    and demonstrating the new database functionality; however, it has all of the tools within the
    function to use as building blocks for a more tailored set of query functions.

    The basic query takes just a table name, Engine and MetaData. Each of the optional entries is added
    to the basic query in an AND type of operation.
    Basic Uses:

    - Basic query with table_name and none of the defaults will return all entries in the table.
    - Status: query with status equal to status
    - latest: returns only the latest version of each entry (by primary key)

    **None:** If the optional parameters are not provided they will not be used in the query.

    Query results will be written to STDOUT.

    :param table_name: Name of the thing table to query.
    :param engine: SqlAlchemy Engine
    :param meta_data: SqlAlchemy MetaData
    :param status: Status of the entry
    :param action: Action of the entry
    :param n_id: Primary key of the entry.
    :param latest: Show only the latest version of each entry.
    :param filename: Name of a file. If provided the results will be written in Excel to that file.
    :return: None
    """
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

    stmt = sqla.select(latest_sub_query2).order_by(latest_sub_query2.c["created_ts"])
    with engine.connect() as connection:
        result = connection.execute(stmt)
        column_names = [col for col in result.keys()]
        df = pd.DataFrame(columns=column_names)
        row_list = []
        for row in result:
            row_dict = make_row_dict(row, column_names)
            row_list.append(row_dict)
    df = pd.DataFrame(row_list)
    number_of_rows = len(df.index)
    print("dataframe:")
    print(df)
    if filename is not None:
        df.to_excel(filename)
    print(f"Total rows found = {number_of_rows}")


@timer
def create_additional_things(
        table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData
) -> None:
    """
    Create additional entries to a thing table for stress testing.

    This function is only useful for stress testing the system. It takes the name of a thing table
    and will add an *<additional>* number of entries to the table for each entry.

    If the table had 100 create entries and *<additonal>* is set to 100 the end result would be 100
    create entries, and 100*100 == 10,000 modify entries. Each modify entry is has the name column modified
    by -<number> to illustrate how the names change. Each 5th entry the status is set to Stage and each
    13th entry the status is set to production. This is for illustration and test purposes.
    :param table_name: Name of the thing table to modify.
    :param additional: Mumber of additional copies of the table to add.
    :param engine: SqlAlchemy Engine
    :param meta_data: SqlAlchemy MetaData
    :return: None
    """
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


# ToDo deprecate create_additional_things2
def create_additional_things2(
        table_name: str, additional: int, engine: sqla.Engine, meta_data: sqla.MetaData
) -> None:
    """
    Create additional things.

    This is a deprecated version of create_additional_things. It uses a Pandas dataframe to
    make the copies in memory before inserting them in the database. THere is some type of
    error in the Pandas Dataframe having to do with time zones that makes the code very slow.

    Theoretically the dataframe should be faster than the dictionary based version above. I need
    to work out the issues with the dataframes. I will either fix the dataframe error or delete
    this code.

    **Note** this functionality will not be important moving forward; however, getting the
    Pandas Dataframe to work properly with the DateTime and timezones will be important.

    :param table_name: Name of the thing table to modify.
    :param additional: Mumber of additional copies of the table to add.
    :param engine: SqlAlchemy Engine
    :param meta_data: SqlAlchemy MetaData
    :return: None
    """
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


def get_extra_columns(old_table: sqla.Table) -> list[sqla.Column]:
    """
    Return a list of the extra columns in the given table.

    Each thing table has standard columns: name, id, created_by, created_ts, updated_by, and updated_ts.
    This function simply returns the list of columns that are not in the above list.

    **Note** There are several naming schemes for the above columns. See *is_standard_column* for more
    info.

    :param old_table: SqlAlchemy Table of the old schema.
    :return: list of SqlAlchemy Columns
    """
    print(f"   old_table.name = {old_table.name}")
    return_list = []
    for column in old_table.c:
        if not is_standard_column(old_table.name, column):
            return_list.append(column)
    return return_list


def read_table_exceptions() -> list[str]:
    """
    Read in the table of exceptions.

    The goal of the Ontology Database Tool is to retool the schema into a new format that is completely
    controlled by the codebase. As part of this process the code validates that every table in the
    old schema has been accounted for.

    There are two csv files which contain a list of table that are known to not be in the new schema:

    - table_exceptions.csv: This is a list of tables that should be excluded from the new database. To be validated
    - table_todo.csv: This is a list of tables that *May* be excluded from the new database.
    - Each entry in table_todo.csv will either be moved to table_exceptions.csv or added to the new database.

    :return: None
    """
    results = []
    with open("table_exceptions.csv", "r") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        for row in readCSV:
            results.append(row[0])

    with open("table_todo.csv", "r") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=",")
        for row in readCSV:
            results.append(row[0])
    return results


def validate_schema(
        old_metadata: sqla.MetaData,
        new_tables: dict[str, OntologyTable],
) -> None:
    """
    Validate that the data in the new database is equivalent to the old database.

    The new database is completely described in a JSON file. This code does a test
    to make certain that all of the information in the old database is represented in
    the new_tables (dict of OntologyTables) which is created from the JSON file.

    This function will validate that all of the tables from the old database exist in the
    JSON schema and that all of the columns are correct.

    The schema checks are written to STDOUT.
    # ToDo check that the types are correct in the columns.

    :param old_metadata: SqlAlchemy MetaData for the old database. The tables need to be reflected.
    :param new_tables: Dictionary of OntologyTables built from the new schema JSON
    :return: None.
    """
    print("validate_schema:")
    # print(f'new tables = {new_tables.keys()}')
    # print(f'old tables = {old_metadata.tables.keys()}')
    keys = [key for key in old_metadata.tables.keys()]

    enumerated_list = enumerated_thing_list()
    for key in new_tables.keys():
        if key not in enumerated_list:
            print(f"ERROR: {key} table from JSON is not in the enumerated list")
    # with open('old tables.csv', 'w') as f:
    #     write = csv.writer(f)
    #     for key in keys:
    #         write.writerow([key])
    table_exceptions = read_table_exceptions()
    missing_tables = []
    for new_table in new_tables.values():
        table_exceptions = table_exceptions + new_table.standard_table_exceptions()
    for table_name, old_table in old_metadata.tables.items():
        table_name = table_name[9:]  # strip off the name of the schema: ontology.
        new_table = new_tables.get(table_name)
        if new_table:
            # print(f'   table <{table_name}> found')
            columns = get_extra_columns(old_table)
            for column in columns:
                if new_table.validate_column(column):
                    pass
                else:
                    print(
                        f"      Error in table <{table_name}> missing column: {column.name}"
                    )
        else:
            if table_name not in table_exceptions:
                print(f"   ERROR: table <{table_name}> not found in new database")
                missing_tables.append(table_name)

    print(f"missing tables: {len(missing_tables)}")
    missing_tables.sort()
    with open("missing_tables.csv", "w+", newline="") as file:
        write = csv.writer(file)
        for row in missing_tables:
            write.writerow([row])
