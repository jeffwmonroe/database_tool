from database_tools.transfer_table.action import Action
from database_tools.transfer_table.table_base import one_to_one_data


class VendorAction(Action):
    def __init__(self, thing, action, vendor, table):
        super().__init__(thing, action, table)
        self.vendor: str = vendor
        self.duplicate_ids: int = 0
        self.duplicate_ext_ids: int = 0

    def has_log(self) -> bool:
        return self.action == "map" or self.action == "fuzzymatch"

    def has_id(self) -> bool:
        return self.action == "map" or self.action == "fuzzymatch"

    def has_name(self) -> bool:
        return self.action == "import"

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
        if self.action == "map" or self.action == "fuzzymatch":
            self.duplicate_ids, self.duplicate_ext_ids = one_to_one_data(
                engine, self.table, self.thing + "_id", "ext_id"
            )
