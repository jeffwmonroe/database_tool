import sqlalchemy as sqla
from sqlalchemy import Enum

import database_tools.database_connection.enums as db_enum
from database_tools.transfer_table.action import Action
from database_tools.transfer_table.thing import Thing


def build_sql_column(col_name: str, col_type: str) -> sqla.Column:
    match col_type:
        case "string30":
            return sqla.Column(col_name, sqla.String(30))
        case "string200":
            return sqla.Column(col_name, sqla.String(200))
        case "varchar":
            return sqla.Column(col_name, sqla.VARCHAR)
        case "int":
            return sqla.Column(col_name, sqla.Integer)
        case "double":
            return sqla.Column(col_name, sqla.DOUBLE_PRECISION)
    raise ValueError("Column type not found in build_sql_column", col_type)


def build_data_table(
    metadata_obj: sqla.MetaData,
    name: str,
    use_vid: bool = False,
    use_name: bool = False,
    extra_data_columns: list[sqla.Column] | None = None,
) -> sqla.Table:
    """
    This method builds a sql table for standard data.
    Standard data has contains the following columns:
        Keys:
            log_id (primary key)
            n_id: this is a foreign key to the thing table
            v_id (optional): foreign key to the vendor table
        logistics columns:
            action: the action performed for this row
            created_ts: time of row insert
            created_by: user to insert the row
            status: this is workflow control
    :param metadata_obj: sqla.MetaData for the database where the table resides
    :param name: name of the table
    :param use_vid: boolean to control whether the v_id column is added
    :param use_name: boolean to control whether the n_id column is added
    :param extra_data_columns: list of additional columns to add
    :return: sqla.Table with all of the above columns
    """
    data_cols: list[sqla.Column] = []
    if extra_data_columns is not None:
        data_cols = extra_data_columns[:]

    keys = [
        sqla.Column("log_id", sqla.Integer, primary_key=True),
        sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
    ]
    if use_vid:
        keys.append(sqla.Column("v_id", sqla.ForeignKey("vendor.v_id"), nullable=False))

    if use_name:
        data_cols.insert(0, sqla.Column("name", sqla.String(200)))

    logs = [
        sqla.Column("action", Enum(db_enum.Action)),
        sqla.Column("created_ts", sqla.DateTime(timezone=True)),
        # ToDo make this a user ID (authenticated)
        sqla.Column("created_by", sqla.String(30)),
        sqla.Column("status", Enum(db_enum.Status)),
    ]
    args = keys + logs + data_cols
    table = sqla.Table(name, metadata_obj, *args)
    return table


def join_thing_and_map(
    old_thing: Thing,
    old_action: Action,
    short_load: bool,
) -> sqla.Select:
    subquery1 = sqla.select(
        old_thing.get_id_column().label("old_id"),
        old_thing.get_name_column().label("name"),
        old_thing.table.c.updated_ts.label("t_updated_ts"),
        old_thing.table.c.updated_by.label("t_updated_by"),
        sqla.func.rank().over(order_by=old_thing.get_id_column()).label("rank"),
    ).subquery()
    if short_load:
        subquery2 = sqla.select(subquery1).filter(subquery1.c.rank < 11).subquery()
    else:
        subquery2 = sqla.select(subquery1).subquery()

    subquery3 = sqla.select(
        subquery2,
        # map.table.c.id,
        old_action.table.c.ext_id,
        old_action.table.c.updated_ts.label("m_updated_ts"),
        old_action.table.c.updated_by.label("m_updated_by"),
    ).join(old_action.table, old_action.get_id_column() == subquery2.c.old_id)
    return subquery3
