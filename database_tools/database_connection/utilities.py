import sqlalchemy as sqla
from sqlalchemy import Enum
from database_tools.database_connection.enums import Action, Status


def build_sql_column(col_name: str, col_type: str) -> sqla.Column:
    match col_type:
        case 'string30':
            return sqla.Column(col_name, sqla.String(30))
        case 'string200':
            return sqla.Column(col_name, sqla.String(200))
        case 'varchar':
            return sqla.Column(col_name, sqla.VARCHAR)
        case 'int':
            return sqla.Column(col_name, sqla.Integer)
        case 'double':
            return sqla.Column(col_name, sqla.DOUBLE_PRECISION)
    raise ValueError('Column type not found in build_sql_column', col_type)


def build_data_table(metadata_obj: sqla.MetaData,
                     name: str,
                     use_vid:
                     bool = False,
                     use_name: bool = False,
                     extra_data_columns: list[sqla.Column] | None = None
                     ) -> sqla.Table:
    data_cols: list[sqla.Column] = []
    if extra_data_columns is not None:
        data_cols = extra_data_columns[:]

    keys = [sqla.Column("log_id", sqla.Integer, primary_key=True),
            sqla.Column("n_id", sqla.ForeignKey("thing.n_id"), nullable=False),
            ]
    if use_vid:
        keys.append(sqla.Column("v_id", sqla.ForeignKey("vendor.v_id"), nullable=False))

    if use_name:
        data_cols.insert(0, sqla.Column("name", sqla.String(200)))

    logs = [sqla.Column("action", Enum(Action)),
            sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            # ToDo make this a user ID (authenticated)
            sqla.Column("created_by", sqla.String(30)),
            sqla.Column("status", Enum(Status)),
            ]
    args = keys + logs + data_cols
    table = sqla.Table(name,
                       metadata_obj,
                       *args
                       )
    return table
