from collections import namedtuple
from action import Action, VendorAction
from table_base import TableBase

ActionKey = namedtuple("vendor", "vend action")


class Thing(TableBase):

    def __init__(self, thing, table):
        super().__init__(thing, table)

        self.actions = {}
        self.vendor_actions = {}
        self.position = 0  # position for iterator
        self.iter_list = []

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

    def validate(self, engine, metadata):
        super().validate(engine, metadata)
        for action in self.actions:
            self.actions[action].validate(engine, metadata)
        for vendor_action in self.vendor_actions:
            self.vendor_actions[vendor_action].validate(engine, metadata)

    def validation_data(self):
        result = super().validation_data()

        for action in self.actions:
            result = result + self.actions[action].validation_data()
        for vendor_action in self.vendor_actions:
            result = result + self.vendor_actions[vendor_action].validation_data()

        return result

    def table_name(self):
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
