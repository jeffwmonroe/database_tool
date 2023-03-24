from collections import namedtuple
from action import Action, VendorAction

ActionKey = namedtuple("ThingAction", "vendor action")


class Thing:

    def __init__(self, thing, table):
        self.thing = thing
        self.table = table
        self.actions = {}
        self.vendor_actions = {}

    def add_action(self, action, table):
        self.actions[action] = Action(action, table)
        print(f'   action added: {self.thing} - {action}')

    def add_vendor_action(self, vendor, action, table):
        key = ActionKey(vendor, action)
        self.vendor_actions[key] = VendorAction(action, vendor, table)
        print(f'    vendor action added: {self.thing} - {key}')
