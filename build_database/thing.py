from collections import namedtuple
from action import Action, VendorAction
from table_base import TableBase

ActionKey = namedtuple("ThingAction", "vendor action")


class Thing(TableBase):

    def __init__(self, thing, table):
        super().__init__(thing, table)

        self.actions = {}
        self.vendor_actions = {}

    def has_log(self):
        return True

    def has_id(self):
        return True

    def has_name(self):
        return True

    def add_action(self, action, table):
        self.actions[action] = Action(self.thing, action, table)
        # print(f'   action added: {self.thing} - {action}')

    def add_vendor_action(self, vendor, action, table):
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(self.thing, action, vendor, table)

        # print(f'    vendor action added: {self.thing} - {key}')

    def validate(self, engine, metadata):
        super().validate(engine, metadata)
        self.print_validation()

        for action in self.actions:
            self.actions[action].validate(engine, metadata)
            self.actions[action].print_validation()
        for vendor_action in self.vendor_actions:
            self.vendor_actions[vendor_action].validate(engine, metadata)
            self.vendor_actions[vendor_action].print_validation()

    def table_name(self):
        return self.thing
