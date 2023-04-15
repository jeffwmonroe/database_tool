"""
Base class for the set of classes that wrap standard tables in the old Ontology schema.

The module also contains several helper functions and the class TableBase
The purpose of this code is to standardize the old database and access
it efficiently so that it can be ported to the new schema.
"""
import sqlalchemy as sqla
from sqlalchemy import func, select

ColumnCollection = sqla.sql.expression.ColumnCollection


def duplicate_row_query(engine: sqla.Engine, column: sqla.Column) -> int:
    """
    Return the number of duplicate rows for the given column.

    This function is used to check for uniqueness of values for a column.
    :param engine: SqlAlchemy engine that is connected to the database
    :param column: SqlAlchemy Column to use in the query.
    :return: the number of duplicates
    """
    subquery = (
        select(column, func.count(column).label("count")).group_by(column).subquery()
    )
    stmt = select(subquery).filter(subquery.c.count > 1)
    with engine.connect() as connection:
        result = connection.execute(stmt)
        num_rows = result.rowcount
    return num_rows


def good_column(table: ColumnCollection, labels: list[str], types: list[str]) -> bool:
    """
    Check to see if the input column exists in the table and if it is of the correct type.

    The ontology database has several required columns. For example the primary key for a
    table. If the table is artist the primary key is artist_name. Some of the tables
    do not follow this convention. This function takes a list of potential labels and checks
    to see if one of them is in the table.

    If the matching colum is found the function checks to make certain that the type
    matches one of the types in the list. There is a list of types because the types
    for the ontology tables are not regular. For example sometimes varchar200 is used
    for created_by and sometimes text is used.

    See good_log_cols below.
    :param table: The SqlAlchemy table to check
    :param labels: List of possible good labels
    :param types: List of possible good types
    :return: True if the column is found with the correct type. Otherwise False.
    """
    # I realize that this is horrifying ...
    for label in labels:
        if label in table.keys():
            col = table[label]
            if str(col.type) in types:
                return True

    return False


def good_log_cols(table: ColumnCollection) -> bool:
    """
    Check to see if the table has the correct log columns.

    A standard table has 4 log columns:

    * created_ts
    * created_by
    * updated_ts
    * updated_by.

    **Note** that the types vary for the created_by and updated_by columns.
    See the good_colum, helper function above for further explanation and details.
    :param table: SqlAlchemy table with expected logistics columns
    :return: true if all four columns are found with the correct types.
    """
    return (
        good_column(table, ["created_ts"], ["TIMESTAMP"])
        and good_column(table, ["updated_ts"], ["TIMESTAMP"])
        and good_column(table, ["created_by"], ["TEXT", "VARCHAR(200)"])
        and good_column(table, ["updated_by"], ["TEXT", "VARCHAR(200)"])
    )


def one_to_one_data(
    engine: sqla.Engine, map_table: sqla.Table, id_label: str, ext_id_label: str
) -> tuple[int | None, int | None]:
    """
    Check to see if the relationship between the two columns are one_to_one.

    This function performs a sanity check to see if the relationship between the
    id_label foreign key and ext_id_label foreign key is one_to_one. This is an important
    check because the old Ontology code may have allowed many_to_one relationships when
    it was not appropriate.

    :param engine: SqlAlchemy Engine
    :param map_table: SqlAlchemy table. This is a map table in the form of: thing_map
    :param id_label: foreign key to Ontology name table.
    :param ext_id_label: foreign key to external client database.
    :return: True if the relationship is one_to_one. False otherwise.
    """
    if id_label in map_table.c.keys():
        id_column = map_table.c[id_label]
    else:
        return None, None

    if ext_id_label in map_table.c.keys():
        ext_id_column = map_table.c[ext_id_label]
    else:
        return None, None

    duplicate_ext_ids = duplicate_row_query(engine, ext_id_column)
    duplicate_ids = duplicate_row_query(engine, id_column)

    return duplicate_ids, duplicate_ext_ids


# ToDo make this an actual abstract base class by adding appropriate decorators.
class TableBase:
    """
    Base Class for the transfer tables.

    This class encapsulates a single table from the old Ontology database. It provides a simple
    Python Object Oriented wrapper around the table with useful helper functions to check for
    regularity of the table.

    Note: This is intended as abstract base class. The initial wrapper does not use the
    abstract baseclass decorators. An instance of this class should not be build. Use one of
    the child sub-classes instead.
    """

    def __init__(self, thing: str, table: sqla.Table):
        """
        Initialize the class.

        It is important that the constructor on this class be called by the child base classes.
        Several instance variables are set to default values.
        :param thing: The name of the thing that the table represents.
        Examples are artist, vendor, and venue
        :param table: SqlAlchemy table that the class encapsulates
        """
        self.thing: str = thing
        self.table: sqla.Table = table

        self.num_rows: int = 0
        self.num_cols: int = 0

        self.good_id: bool = False
        self.good_name: bool = False
        self.good_log: bool = False

    def has_log(self) -> bool:
        """
        Check if the table SHOULD have logistics columns.

        This class should be overridden by the child classes. Some tables such as import_thing
        or export_thing do not have log_columns or only have half log columns.

        :return: True if the table should have log columns.
        """
        return False

    def has_id(self) -> bool:
        """
        Check to see if the table should have an id colum.

        The id is the primary key for a thing and the columns are typically called thing_id.
        For example: artist_id. It will be the primary key in the artist table and a foreign key
        in the artist_map table.
        :return: True if table should have id. False otherwise
        """
        return False

    def has_name(self) -> bool:
        """
        Check to see if the table should have a name column.

        The name colum is where the name of the thing is stored. It usually takes the form of thing_name.
        Example: artist_name. It is expected in the thing table (artist) but not in the map_thing_vendor
        table.
        :return: True if a name column is expected. False otherwise.
        """
        return True

    def validate(self, engine: sqla.Engine, metadata: sqla.MetaData) -> None:
        """
        Validate the table.

        This method does all the validation for the table that the class wraps. For example,
        if a name column is expected it checks to make certain that it is one of the columns
        in the table, with an acceptable name and data type. Similarly, for id, and log.

        **Note**: this method has side effects. The validation requires querying the database which can
        be slow. Therefore, this method caches the results in the class for future reference without
        need to query the database.

        :param engine: SqlAlchemy Engine
        :param metadata: SqlAlchemy MetaData
        :return: None
        """
        # print(validate: {self.thing}')
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount
            # print(f'    rows = {self.num_rows}')
        if self.has_name():
            self.good_name = good_column(
                self.table.c,
                [self.thing + "_name", self.thing],
                ["TEXT", "VARCHAR(200)"],
            )
        else:
            self.good_name = None
        if self.has_id():
            self.good_id = good_column(
                self.table.c, [self.thing + "_id"], ["INTEGER", "BIGINT"]
            )
        else:
            self.good_id = None
        if self.has_log():
            self.good_log = good_log_cols(self.table.c)
        else:
            self.good_log = None
        self.num_cols = len(self.table.c.keys())

    def table_name(self) -> str:
        """
        Return the name of the table. Needs to be overridden by child classes.

        This is a C++ style get method which enables overriding.
        :return: Name of the table or none
        """
        return "none"

    def get_thing(self) -> str:
        """
        Return the thing.

        This is a C++ style get method which enables overriding.
        :return: self.thing.
        """
        return self.thing

    def get_action(self) -> str | None:
        """
        Return the action.

        This is a C++ style get method which enables overriding
        :return: action (str) or None if the table has no action.
        """
        return None

    def get_vendor(self) -> str | None:
        """
        Return the vendor.

        This is a C++ style get method which enables overriding
        :return: vendor (str) or None if the table has no vendor.
        """
        return None

    def get_duplicate_ids(self) -> int:
        """
        Return the number of duplicated ids.

        This is a more detailed examination into the relationship between ids (PK) and
        ext_ids (foreign key to external database). A zero for both duplicate ids and
        duplicated ext_ids indicates a one-to-one relationship. If the relationship is
        many-to-one or one-to-many the size of this number indicates the extent of the
        many part of the relationship.
        :return: number of duplicates.
        """
        return 0

    def get_duplicate_ext_ids(self) -> int:
        """
        Return the number of duplicated ext_ids.

        This is a more detailed examination into the relationship between ids (PK) and
        ext_ids (foreign key to external database). A zero for both duplicate ids and
        duplicated ext_ids indicates a one-to-one relationship. If the relationship is
        many-to-one or one-to-many the size of this number indicates the extent of the
        many part of the relationship.
        :return: number of duplicates.
        """
        return 0

    def validation_data(self) -> list[dict[str, str | bool | int | None]]:
        """
        Return a list of objects (dict) for the validation data.

        This data will be written out to Excel for inspection of the data
        within the database.
        :return: list of one dict (table)
        """
        return [
            {
                "table_name": self.table_name(),
                "thing": self.get_thing(),
                "action": self.get_action(),
                "vendor": self.get_vendor(),
                "good_id": self.good_id,
                "good_name": self.good_name,
                "good_log": self.good_log,
                "columns": self.num_cols,
                "rows": self.num_rows,
                "dup id": self.get_duplicate_ids(),
                "dup ext_id": self.get_duplicate_ext_ids(),
            }
        ]

    def get_id_column(self) -> sqla.Column:
        """
        Return the id column (PK or foreign key).

        This method can be overridden and enables the program to look for multiple
        names of the id column.
        :return: id column
        """
        name = self.thing + "_id"
        if name in self.table.c.keys():
            # print(f'found it: {name}')
            return self.table.c[name]
        raise ValueError("No name found in get_id_column", name)

    def get_name_column(self) -> sqla.Column:
        """
        Return the name column.

        This method can be overridden and enables the program to look for multiple
        names of the id column.
        :return: id column
        :return:
        """
        name = self.thing + "_name"
        if name in self.table.c.keys():
            return self.table.c[name]
        if self.thing in self.table.c.keys():
            return self.table.c[self.thing]
        raise ValueError("No name found in get_name_column", name)
