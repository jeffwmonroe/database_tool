import time
from typing import Any, Callable

import sqlalchemy as sqla
from sqlalchemy import func, select

import database_tools.database_connection.enums as db_enum
from database_tools.database_connection.new_schema import NewDatabaseSchema
from database_tools.database_connection.ontology_table import OntologyTable
from database_tools.transfer_table.thing import Thing
import pandas as pd
def load_table_from_file(table_name: str, file_name: str, new_database: NewDatabaseSchema):
    df = pd.read_excel(file_name)
    print(df)

    pass

