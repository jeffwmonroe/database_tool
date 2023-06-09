import sqlalchemy as sqla


def is_name_column(table_name: str, column: sqla.Column) -> bool:
    if column.name == table_name:
        return True
    if column.name == table_name + "_name":
        return True
    return False


def is_id_column(table_name: str, column: sqla.Column) -> bool:
    if column.name == table_name + "_id":
        return True
    return False


def is_log_column(table_name: str, column: sqla.Column) -> bool:
    if (
        column.name == "created_by"
        or column.name == "updated_by"
        or column.name == "created_ts"
        or column.name == "updated_ts"
    ):
        return True
    return False


def is_standard_column(table_name: str, column: sqla.Column) -> bool:
    if is_name_column(table_name, column):
        return True
    if is_id_column(table_name, column):
        return True
    if is_log_column(table_name, column):
        return True
    return False
