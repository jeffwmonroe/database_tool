import time
from typing import Any, Callable

import sqlalchemy as sqla
from sqlalchemy import func, select

import database_tools.database_connection.enums as db_enum
from database_tools.database_connection.new_schema import NewDatabaseSchema
from database_tools.database_connection.ontology_table import OntologyTable
from database_tools.transfer_table.thing import Thing
import pandas as pd


def generate_new_id_pks(
    number, thing_name, thing_table, engine
) -> list[dict[str, str]]:
    thing_table_values: list[dict[str, str]] = [
        {"thing": thing_name} for index in range(number)
    ]

    with engine.connect() as new_connection:
        insert_thing = thing_table.insert()
        new_connection.execute(insert_thing, thing_table_values)

    print("finished inserts")

    return thing_table_values


def load_table_from_file(
    table_name: str, file_name: str, new_database: NewDatabaseSchema
) -> None:
    thing_pk_start = 3000

    df = pd.read_excel(file_name)
    if "n_id" in df.keys():
        new_id_col = [
            row for row in range(thing_pk_start, thing_pk_start + len(df.index))
        ]
        print("append")
    else:
        print("new entries")
    df["n_id"] = new_id_col

    print(f"column names = {df.keys()}")
    print(f"len = {len(df.index)}")
    print(f"new_col = {new_id_col}")

    values = df.to_dict(orient="records")
    print(f"values = {values}")
    load_table = new_database.metadata_obj.tables[table_name]
    print(load_table)

    thing_table_values: list[dict[str, int | str]] = [
        {"n_id": index, "thing": table_name} for index in new_id_col
    ]

    print(f"values = {thing_table_values}")
    with new_database.engine.connect() as new_connection:
        insert_thing = new_database.thing_table.insert()
        new_connection.execute(insert_thing, thing_table_values)

        insert_type = load_table.insert()
        new_connection.execute(insert_type, values)
        new_connection.commit()
    print("finished inserts")
