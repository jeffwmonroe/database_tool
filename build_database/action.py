class Action:
    def __init__(self, action, table):
        self.action = action
        self.table = table


class VendorAction(Action):
    def __init__(self, action, vendor, table):
        super().__init__(action, table)
        self.vendor = vendor

