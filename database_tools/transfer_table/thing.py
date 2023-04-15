"""This module contains that Thing class."""

from collections import namedtuple

import sqlalchemy as sqla

from database_tools.transfer_table.action import Action
from database_tools.transfer_table.vendor_action import VendorAction
from database_tools.transfer_table.table_base import TableBase

ActionKey = namedtuple("vendor", "vend action")


class Thing(TableBase):
    """
    Class that corresponds to a thing in the Ontology Database.

    This is the basic table that holds all the information for the thing.
    Examples include:

    * artist
    * venue
    * actor
    """

    def __init__(self, thing: str, table: sqla.Table):
        """
        Initialize the class.

        This class contains a list of Action classes and a list of VendorAction classes.
        There will be potentially multiple Actions and VendorActions. This hierarchy
        is the main structure of the Ontology Database
        **Note** it is important to call super()..__init__()
        :param thing:
        :param table:
        """
        super().__init__(thing, table)

        self.actions: dict[str, Action] = {}
        self.vendor_actions: dict[ActionKey, VendorAction] = {}

        # ToDo deprecate this information
        self.position: int = 0  # position for iterator
        self.iter_list: list[Action] = []

    def has_log(self) -> bool:
        """
        Check if the table **should** the table have log columns.

        Log information columns are:

        * created_ts
        * created_by
        * updated_ts
        * updated_by

        All Thing tables need to have logistics information.

        :return: True
        """
        return True

    def has_id(self) -> bool:
        """
        Check if the table **should** the table have an id column.

        :return: True
        """
        return True

    def has_name(self) -> bool:
        """
        Check if the table **should** the table have a name column.

        :return: True
        """
        return True

    def add_action(self, action: str, table: sqla.Table) -> None:
        """
        Add an action table to the actions' dictionary.

        :param action: name of the action
        :param table: SqlAlchemy table for the action
        :return: None
        """
        self.actions[action] = Action(self.thing, action, table)

    def add_vendor_action(self, vendor: str, action: str, table: sqla.Table) -> None:
        """
        Add a vendor action table (VendorAction) to the vendor_action dictionary.

        :param vendor: name of the vendor
        :param action: name of the action
        :param table: SqlAlchemy table from the Ontology database
        :return: None
        """
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(self.thing, action, vendor, table)

    def validate(self, engine: sqla.Engine, metadata: sqla.MetaData) -> None:
        """
        Validate the correctness of the thing table and children.

        This method overrides the base class to call validate on the Action
        and VendorAction dictionaries.

        :param engine: SqlAlchemy engine for the old ontology database
        :param metadata: SqlAlchemy metadata object for the old ontology database
        :return: None
        """
        super().validate(engine, metadata)
        for action in self.actions:
            self.actions[action].validate(engine, metadata)
        for vendor_action in self.vendor_actions:
            self.vendor_actions[vendor_action].validate(engine, metadata)

    def validation_data(self) -> list[dict[str, str | bool | int | None]]:
        """
        Return a list of objects (dict) for the validation data.

        This data will be written out to Excel for inspection of the data
        within the database. This overrides the parent base class and calls
        validation_data on the Action and Vendor Action dictionaries.

        :return: list of one dict (table)
        :return:
        """
        result = super().validation_data()

        for action in self.actions:
            result = result + self.actions[action].validation_data()
        for vendor_action in self.vendor_actions:
            result = result + self.vendor_actions[vendor_action].validation_data()

        return result

    def table_name(self) -> str:
        """
        Return the name of the table.

        :return: name of the table
        """
        return self.thing

    # ToDo Deprecate this code
    # OK, this code is a bit weird. The table, thing, action, vendorAction hierarchy was built
    # to store the table structure from the original database so that analysis could be performed
    # to see which tables were important. There is a one-to-one mapping of classes to tables.
    # The iterators are hard wired to only iterate over the tables that we are going to keep in the
    # new format.
    # The new style of code reads in the table structure from an external source (JSON or Excel)
    # The iterators are left behind just in case they are needed in the future.
    # The if action == "map" statement ensures that the iterater only returns tables in the
    # for of: map_thing_vendor. The other tables are ignore.
    # This is really hacky

    def vendor_action_list(self) -> list[Action]:
        """
        Return a list of VendorActionTables. May be deprecated.

        :return: list of Actions
        """
        result: list[Action] = []
        for key in self.vendor_actions.keys():
            # ToDo remove this == 'map' check. This is pretty hacky
            if self.vendor_actions[key].action == "map":
                result.append(self.vendor_actions[key])
        return result

    def __iter__(self):
        self.position = 0
        self.iter_list = []
        for key in self.vendor_actions.keys():
            # ToDo remove this == 'map' check. This is pretty hacky
            if self.vendor_actions[key].action == "map":
                self.iter_list.append(self.vendor_actions[key])
        return self

    def __next__(self):
        # ToDo test for StopIteration
        if len(self.iter_list) == 0:
            raise StopIteration
        return self.iter_list.pop()
