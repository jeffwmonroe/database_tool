import sqlalchemy as sqla


def log_info():
    return [sqla.Column('created_ts', sqla.DateTime(timezone=True)),
            sqla.Column('updated_ts', sqla.DateTime(timezone=True)),
            sqla.Column("created_by", sqla.String(200)),
            sqla.Column("updated_by", sqla.String(200)),
            ]


def type_table_no_data(metadata_obj, name):
    keys = [sqla.Column(name + "_id", sqla.Integer, primary_key=True),
            sqla.Column(name + "_name", sqla.String(200)),
            ]

    log = log_info()

    args = keys + log
    return sqla.Table(name,
                      metadata_obj,
                      *args
                      )


def map_table(metadata_obj, name, vendor):
    # ToDo fix the robo hack
    if name == 'artist' and vendor == 'tapir':
        ext_col = sqla.Column("ext_id", sqla.TEXT)
    else:
        ext_col = sqla.Column("ext_id", sqla.BIGINT)

    keys = [sqla.Column("id", sqla.Integer, primary_key=True), ext_col]
    log = log_info()
    data = [sqla.Column(name + "_id", sqla.ForeignKey(name + "." + name + "_id"), nullable=False),
            ]
    args = keys + log + data
    return sqla.Table("map_" + name + "_" + vendor,
                      metadata_obj,
                      *args
                      )


class OTable:
    def __init__(self, thing):

        self.thing = thing
        self.table = None

    def print_row(self, row):
        print(f'row')

class MapTable(OTable):
    def __init__(self, metadata, thing, vendor, **kwargs):

        super().__init__(thing)
        self.vendor = vendor
        self.table = map_table(metadata, thing, vendor)

    def print_row(self, row):
        print(f'map row = <{row.ext_id}>, {row.artist_id}')


class ThingTable(OTable):
    def __init__(self, metadata, thing):

        super().__init__(thing)
        self.table = type_table_no_data(metadata, thing)
