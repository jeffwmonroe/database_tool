from pydantic import BaseModel
import json



class Column(BaseModel):
    """
    Column is the class for a data column outside of the standard columns
    """
    name: str
    data_type: str
    # foreign_table: str
    # foreign_column: str


class DataTable(BaseModel):
    """
    DataTable base model definition
    """
    name: str
    columns: list[Column]
    vendors: list[str]


class Schema(BaseModel):

    tables: list[DataTable]




