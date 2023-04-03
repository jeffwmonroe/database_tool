from table_base import TableBase
from collections import namedtuple
from action import Action, VendorAction
import sqlalchemy as sqla

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
