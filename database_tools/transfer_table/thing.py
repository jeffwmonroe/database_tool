from collections import namedtuple

import sqlalchemy as sqla

from database_tools.transfer_table.action import Action, VendorAction
from database_tools.transfer_table.table_base import TableBase

ActionKey = namedtuple("vendor", "vend action")


class Thing(TableBase):
    def __init__(self, thing: str, table: sqla.Table):
        super().__init__(thing, table)

        self.actions: dict[str, Action] = {}
        self.vendor_actions: dict[ActionKey, VendorAction] = {}

        self.position: int = 0  # position for iterator
        self.iter_list: list[Action] = []

    def has_log(self) -> bool:
        return True

    def has_id(self) -> bool:
        return True

    def has_name(self) -> bool:
        return True

    def add_action(self, action: str, table: sqla.Table) -> None:
        self.actions[action] = Action(self.thing, action, table)

    def add_vendor_action(self, vendor: str, action: str, table: sqla.Table) -> None:
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(self.thing, action, vendor, table)

    def validate(self, engine: sqla.Engine, metadata: sqla.MetaData) -> None:
        """
        Thus method performs validation checks on the thing as well as the
        sub tables in actions and vendor actions
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
        result = super().validation_data()

        for action in self.actions:
            result = result + self.actions[action].validation_data()
        for vendor_action in self.vendor_actions:
            result = result + self.vendor_actions[vendor_action].validation_data()

        return result

    def table_name(self) -> str:
        return self.thing

    def vendor_action_list(self) -> list[Action]:
        result = []
        for key in self.vendor_actions.keys():
            # ToDo remove this == 'map' check. This is pretty hacky
            if self.vendor_actions[key].action == "map":
                result.append(self.vendor_actions[key])
        return result

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
