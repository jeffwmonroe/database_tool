from table_base import TableBase, good_column, one_to_one_data
import sqlalchemy as sqla


class Action(TableBase):
    def __init__(self, thing: str, action: str, table: sqla.Table):
        super().__init__(thing, table)
        self.action: str = action

        self.good_ext: bool = False

    def has_name(self) -> bool:
        return True

    def validate(self, engine, metadata) -> None:
        super().validate(engine, metadata)

        if self.action == 'map' or self.action == 'fuzzymatch':
            self.good_ext = good_column(self.table.c, ['ext'], ["TEXT", "VARCHAR(200)"])
        else:
            self.good_ext = None

    def table_name(self) -> str:
        return self.action + "_" + self.thing

    def get_action(self) -> str:
        return self.action


class VendorAction(Action):
    def __init__(self, thing, action, vendor, table):
        super().__init__(thing, action, table)
        self.vendor: str = vendor
        self.duplicate_ids: int = 0
        self.duplicate_ext_ids: int = 0

    def has_log(self) -> bool:
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_id(self) -> bool:
        return self.action == 'map' or self.action == 'fuzzymatch'

    def has_name(self) -> bool:
        return self.action == 'import'

    def table_name(self) -> str:
        return self.action + "_" + self.thing + "_" + self.vendor

    def get_vendor(self) -> str:
        return self.vendor

    def get_duplicate_ids(self) -> int:
        return self.duplicate_ids

    def get_duplicate_ext_ids(self) -> int:
        return self.duplicate_ext_ids

    def validate(self, engine, metadata) -> None:
        super().validate(engine, metadata)
        if self.action == 'map' or self.action == 'fuzzymatch':
            self.duplicate_ids, self.duplicate_ext_ids = one_to_one_data(engine,
                                                                         self.table,
                                                                         self.thing + '_id',
                                                                         'ext_id')
