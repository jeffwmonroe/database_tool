"""This module contains the VendorAction class."""
import sqlalchemy

from database_tools.transfer_table.action import Action
from database_tools.transfer_table.table_base import one_to_one_data


class VendorAction(Action):
    """
    The VendorAction class wraps a vendor action table.

    These tables take the form of action_thing_vendor.
    Examples include:

    * map_actor_cheetah
    * fuzzymatch_actor_tapir
    * map_venue_monkey
    """

    def __init__(self, thing: str, action: str, vendor: str, table: sqlalchemy.Table):
        """
        Initialize the instance.

        :param thing: name of the thing.
        :param action: name of the action
        :param vendor: name of the vendor
        :param table: SqlAlchemy table correscponding to the VendorAction
        """
        super().__init__(thing, action, table)
        self.vendor: str = vendor
        self.duplicate_ids: int = 0
        self.duplicate_ext_ids: int = 0

    def has_log(self) -> bool:
        """
        Check if the table should have log information.

        The VendorAction table should have log information if the action is map or fuzzy match
        Log information columns are:

        * created_ts
        * created_by
        * updated_ts
        * updated_by

        :return: True if action is map or fuzzymatch. False otherwise.
        """
        return self.action == "map" or self.action == "fuzzymatch"

    def has_id(self) -> bool:
        """
        Check if the table should have an id.

        The VendorAction table should have log information if the action is map or fuzzy match

        :return: True if action is map or fuzzymatch. False otherwise.
        """
        return self.action == "map" or self.action == "fuzzymatch"

    def has_name(self) -> bool:
        """
        Check if the table should have a name.

        :return: True if action is import. False otherwise.
        """
        return self.action == "import"

    def table_name(self) -> str:
        """
        Return the name of the table.

        :return: table name is: action_thing_vendor
        """
        return self.action + "_" + self.thing + "_" + self.vendor

    def get_vendor(self) -> str:
        """
        Return the vendor.

        Basic C++ style getter.

        :return: name of the vendor (self.vendor)
        """
        return self.vendor

    def get_duplicate_ids(self) -> int:
        """
        Return the duplicate ids.

        Basic C++ style getter.

        :return: number of duplicate ids
        """
        return self.duplicate_ids

    def get_duplicate_ext_ids(self) -> int:
        """
        Return the duplicate ext_ids.

        Basic C++ style getter.

        :return: number of duplicate ext_ids
        """
        return self.duplicate_ext_ids

    def validate(self, engine, metadata) -> None:
        """
        Validate the table.

        This method overrides the baseclass functionality and adds the calculation
        of duplicate ids and duplicate ext_ids. This is important so that the team can
        assess the validity of many_to_one, one_to_many, and many_to_many relationships in
        the Ontology database.

        **Note:** It is important to call the super().validate method.
        :param engine:
        :param metadata:
        :return:
        """
        super().validate(engine, metadata)
        if self.action == "map" or self.action == "fuzzymatch":
            self.duplicate_ids, self.duplicate_ext_ids = one_to_one_data(
                engine, self.table, self.thing + "_id", "ext_id"
            )
