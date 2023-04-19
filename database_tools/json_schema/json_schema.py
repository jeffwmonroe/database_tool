import json
from typing import Optional, Union

from pydantic import BaseModel


class JsonColumn(BaseModel):
    """
    Column is the class for a data column in addition to the standard columns
    """

    name: str
    data_type: str
    foreign_table: Optional[str]


class JsonDataTable(BaseModel):
    """
    DataTable base model definition
    """

    name: str
    columns: list[JsonColumn]
    vendors: list[str]

    def pprint(self) -> None:
        print(f"build table: {self.name}")
        for column in self.columns:
            print(f"    column: ({column.name}, {column.data_type})")


class JsonSchema(BaseModel):
    """
    Schema holds the tables for the new database json_schema.
    """

    tables: list[JsonDataTable]

    def pprint(self) -> None:
        for table in self.tables:
            table.pprint()


def read_schema_json(json_file: str) -> JsonSchema:
    with open(json_file, "r") as infile:
        json_object = json.loads(infile.read())

    schema = JsonSchema(tables=json_object["tables"])
    return schema
