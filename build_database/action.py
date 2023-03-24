from sqlalchemy import select, func

class Action:
    def __init__(self, action, table):
        self.action = action
        self.table = table

        self.num_rows = 0
        self.num_cols = 0

    def validate(self, engine, metadata):
        stmt = select(self.table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            self.num_rows = result.rowcount


class VendorAction(Action):
    def __init__(self, action, vendor, table):
        super().__init__(action, table)
        self.vendor = vendor

    def validate(self, engine, metadata):
        super().validate(engine, metadata)
        pass
