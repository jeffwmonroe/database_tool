"""
Action is the transfer table that corresponds to tables in the form of action_thing.

Examples include:

* import_artist
* export_artist
* map_artist
* fuzzymatch_artist

These table do not have log data but do have names.

**Note** Some of these tables are empty in the actual Ontology database; however, this
mapping was needed to discover that fact.
"""

import sqlalchemy as sqla

from database_tools.transfer_table.table_base import TableBase, good_column


class Action(TableBase):
    """

    Action is the class that corresponds to an action table from the Ontology database.

    These tables are in the form of action_thing. Examples include:

    * import_artist
    * export_artist
    * map_artist
    * fuzzymatch_artist

    **Note** Some of these tables are empty in the actual Ontology database; however, this
    mapping was needed to discover that fact.
    """

    def __init__(self, thing: str, action: str, table: sqla.Table):
        """
        Initialize the class.

        **Note** it is important to call super().__init__()

        :param thing: thing (actor, artist, venue etc)
        :param action: the action (import, export)
        :param table: the SqlAlchemy table corresponding to the action
        """
        super().__init__(thing, table)
        self.action: str = action

        self.good_ext: bool = False

    def has_name(self) -> bool:
        """
        Return True because action tables have names.

        Overrides base class
        :return: True
        """
        return True

    def validate(self, engine, metadata) -> None:
        """
        Validate the class.

        This method adds validation code that is particular to the action table

        If the self.action is map or fuzzymatch then this table
        should have an ext column for the external foreign key. This method add that test.

        **Note** it is important to call super().validate()

        *See documentation for parent class*

        :param engine: SqlAlchemy Engine
        :param metadata: SqlAlchemy MetaData
        :return: None
        """
        super().validate(engine, metadata)

        if self.action == "map" or self.action == "fuzzymatch":
            self.good_ext = good_column(self.table.c, ["ext"], ["TEXT", "VARCHAR(200)"])
        else:
            self.good_ext = None

    def table_name(self) -> str:
        """
        Return the name of the table.

        For an action table the name is action_thing
        :return: name of the table
        """
        return self.action + "_" + self.thing

    def get_action(self) -> str | None:
        """
        Return the action for the table.

        Only action table have actions
        :return: returns the action
        """
        return self.action
