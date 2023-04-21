"""
This module contains the readers for the JSON database schema.

These modules are only used for reading the file. This seems like an extra step; however,
it used pydantic which enables error checking on the JSON file for both format at
content. This will be useful in making certain that the JSON is always correct.
"""
import json
from typing import Optional

from pydantic import BaseModel


class JsonColumn(BaseModel):
    """
    Column is the class for a data column in addition to the standard columns.

    It currently has name and data type for each column.

    foreign_table is an optional str which is the name of the foreign table if
    the column is a foreign key. Otherwise, this is set to None.
    """

    name: str
    data_type: str
    foreign_table: Optional[str]


class JsonDataTable(BaseModel):
    """
    This is the base class for the data table in the Ontology JSON schema.

    This table is currently configured for use with thing tables.

    **Note** Thing tables aways have certain basic columns:

    - *name:* The name of the thing. For example, Nick Cage in the actor table.
    - *modified_by:* User who modified the row.
    - *modified_ts:* timestamp for when the row was modified.
    - *status:* Workflow status - draft, stage, production
    - *action:* Action performed - create, modify, delete
    - *log_id:* primary key for the thing table
    - *n_id:* foreign key to the thing table which stores all of the primary keys for the things.

    Since all tables have these columns, it is unnecessary to specify them in the JSON. Only
    columns for additional columns are listed.

    Vendor is a list of strings of vendors that are used with the thing. The vendors should eventually
    be put into a separate table and foreign keys should be used.

    # ToDo Add a vendor table
    """

    name: str
    columns: list[JsonColumn]
    vendors: list[str]

    def pprint(self) -> None:
        """
        Print the JSON table in a human-readable format.

        :return: None
        """
        print(f"build table: {self.name}")
        for column in self.columns:
            print(f"    column: ({column.name}, {column.data_type})")


class JsonSchema(BaseModel):
    """
    Schema holds the tables for the new database json_schema.

    This class is just a simple way to contain the data tables.
    """

    tables: list[JsonDataTable]

    def pprint(self) -> None:
        """
        Print the Json Tables in a human-readable format.

        :return: None
        """
        for table in self.tables:
            table.pprint()


def read_schema_json(json_file: str) -> JsonSchema:
    """
    Read the json schema file.

    :param json_file: Name of the json_file.
    :return: JsonSchema
    """
    with open(json_file, "r") as infile:
        json_object = json.loads(infile.read())

    schema = JsonSchema(tables=json_object["tables"])
    return schema
