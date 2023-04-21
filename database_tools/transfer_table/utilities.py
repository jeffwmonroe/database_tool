"""
general utilities for working with the transfer tables.

This contains code for dealing with the irregularities in the original Ontology
Database. For example irregularly named columns.
"""
import sqlalchemy as sqla


def is_name_column(table_name: str, column: sqla.Column) -> bool:
    """
    Return True if the column is a name column. False otherwise.

    For the majority of the old Ontology database the name column is <table_name>_name.
    Sometimes the name column is the same as the table name. There are several tables where
    these convention is not followed. All the irregularities are handled
    with exceptions.
    :param table_name: The name of the table that contains the column to check.
    :param column: The SqlAlchemy column to check.
    :return: True if the column is a name column. False otherwise.
    """
    if column.name == table_name:
        return True
    if column.name == table_name + "_name":
        return True
    if table_name == "sport_league" and column.name == "league_name":
        return True
    if table_name == "sport_team" and column.name == "team_name":
        return True
    if table_name == "theatre_title" and column.name == "title":
        return True
    return False


def is_id_column(table_name: str, column: sqla.Column) -> bool:
    """
    Return True if the column is an id column. False otherwise.

    For the majority of the old Ontology database the id column is <table_name>_id.
    There are several tables where these convention is not followed. All the irregularities
    are handled.

    **Note** The id column is the primary key for the thing in the table, which is actually
    a foreign key to the name table.
    with exceptions.
    :param table_name: The name of the table that contains the column to check.
    :param column: The SqlAlchemy column to check.
    :return: True if the column is an id column. False otherwise.
    """
    if column.name == table_name + "_id":
        return True
    if table_name == "sport_team" and column.name == "team_id":
        return True
    if table_name == "sport_league" and column.name == "league_id":
        return True
    if table_name == "theatre_title" and column.name == "title_id":
        return True
    return False


def is_log_column(table_name: str, column: sqla.Column) -> bool:
    """
    Return True if the column is log column. False otherwise.

    A logistics column, or log column, is one of the columns from the old schema
    when tracked who created or updated the data and the timestamp when that occurred.

    **Note** Most standard tables have 4 log columns. Created_by, updated_by, created_ts,
    and updated_ts. Some of the tables have half of these. Typcially it is updated_by and
    updated_ts.
    :param table_name: The name of the table that contains the column to check.
    :param column: The SqlAlchemy column to check.
    :return: True if the column is a log column. False otherwise.
    """
    if (
        column.name == "created_by"
        or column.name == "updated_by"
        or column.name == "created_ts"
        or column.name == "updated_ts"
    ):
        return True
    return False


def is_standard_column(table_name: str, column: sqla.Column) -> bool:
    """
    Return True if the column is a standard colum. False otherwise.

    The six standard columns are:

    - name
    - id
    - created_by
    - created_ts
    - updated_by
    - updated_ts

    **Note** It is useful for the code to identify a standard column so that it
    can differentiate between the standard and extra columns which contain data.

    :param table_name: The name of the table that contains the column to check.
    :param column: The SqlAlchemy column to check.
    :return: True if the column is a standard column. False otherwise.
    """
    if is_name_column(table_name, column):
        return True
    if is_id_column(table_name, column):
        return True
    if is_log_column(table_name, column):
        return True
    return False
