import time

import sqlalchemy as sqla
from database_tools.database_connection.new_schema import NewDatabaseSchema
import pandas as pd
import functools


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


def load_thing_table_from_file(
        table_name: str, file_name: str, new_database: NewDatabaseSchema
) -> None:
    df = pd.read_excel(file_name)
    start_time = time.time()
    load_table = new_database.metadata_obj.tables[table_name]
    nrows = len(df.index)
    if "n_id" in df.keys():
        df['action'] = ['modify'] * nrows
    else:
        # ToDo refactor this
        thing_table = new_database.metadata_obj.tables["thing"]
        df["n_id"] = generate_new_id_pks(
            len(df.index), table_name, thing_table, new_database.engine
        )
        df['action'] = ['create'] * nrows

    values = df.to_dict(orient="records")
    with new_database.engine.connect() as new_connection:
        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()
    duration = time.time() - start_time
    print(f"Total duration: {duration}")
