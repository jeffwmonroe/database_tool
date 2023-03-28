from sqlalchemy import select, func
from new_schema import Status, Action
import time

print('loading utilities ...')
SHORT_LOAD = False


def join_thing_and_map(thing, map):
    subquery1 = select(
        thing.table.c.artist_id,
        thing.table.c.artist_name,
        thing.table.c.updated_ts.label("t_updated_ts"),
        thing.table.c.updated_by.label("t_updated_by"),
        func.rank().over(order_by=thing.table.c.artist_id
                         ).label('rank')
    ).subquery()
    if SHORT_LOAD:
        subquery2 = select(subquery1).filter(subquery1.c.rank < 11).subquery()
    else:
        subquery2 = select(subquery1).subquery()

    subquery3 = select(
        subquery2,
        # map.table.c.id,
        map.table.c.ext_id,
        map.table.c.updated_ts.label("m_updated_ts"),
        map.table.c.updated_by.label("m_updated_by"),
    ).join(map.table, map.table.c.artist_id == subquery2.c.artist_id)
    return subquery3


def fill_thing_table(database, engine, thing, thing_pk_start):
    print('----------------------------------')
    print('          fill_thing_table')
    subquery1 = select(
        thing.table.c.artist_id,
        thing.table.c.artist_name,
        thing.table.c.updated_ts.label("t_updated_ts"),
        thing.table.c.updated_by.label("t_updated_by"),
        func.rank().over(order_by=thing.table.c.artist_id
                         ).label('rank')
    ).subquery()
    if SHORT_LOAD:
        subquery2 = select(subquery1).filter(subquery1.c.rank < 11)
    else:
        subquery2 = select(subquery1)
    stmt = subquery2
    thing_add = []
    bridge_add = []
    bridge = {}
    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        row_num = result.rowcount
        for row in result:
            thing_add.append(
                {
                    'n_id': thing_pk_start + index,
                    'name': row.artist_name,
                    'action': Action.create,
                    'created_ts': row.t_updated_ts,
                    'created_by': row.t_updated_by,
                    'status': Status.draft,
                })
            bridge_add.append(
                {'old_id': row.artist_id,
                 'n_id': thing_pk_start + index,
                 'type': 'artist',
                 })
            bridge[row.artist_id] = thing_pk_start + index
            index += 1
    values = [{'n_id': thing_pk_start + indx, 'type': 'artist'} for indx in range(row_num)]

    with database.engine.connect() as new_connection:
        insert_type = database.type_table.insert()
        new_connection.execute(insert_type, values)

        insert_thing = database.artist_table.insert()
        new_connection.execute(insert_thing, thing_add)

        bridge_ins = database.bridge_table.insert()
        new_connection.execute(bridge_ins, bridge_add)

        new_connection.commit()
    return bridge, thing_pk_start + index


def test_fill(database, engine, thing, map, vendor, bridge):
    vendor_pk = database.add_vendor(vendor)

    print('-------------------------------------------------')
    print('              test_fill')
    # print(f'bridge = {bridge}')
    stmt = join_thing_and_map(thing, map)
    map_add = []

    start_time = time.time()
    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        row_num = result.rowcount
        for row in result:
            old_pk = row.artist_id
            new_pk = bridge[old_pk]
            map_add.append(
                {
                    'n_id': new_pk,
                    'v_id': vendor_pk,
                    'ext_id': row.ext_id,
                    'map_type': 'person',
                    'confidence': 1,
                    'action': Action.create,
                    'created_ts': row.m_updated_ts,
                    'created_by': row.m_updated_by,
                    'status': Status.draft,
                }
            )
            index += 1

    with database.engine.connect() as new_connection:
        insert_map = database.name_map_table.insert()
        new_connection.execute(insert_map, map_add)

        new_connection.commit()
    duration = time.time() - start_time
    print(f'Duration: {duration}')


def test_fill_many(engine, thing, map):
    stmt = select(thing.table.c.ethnicity_id,
                  thing.table.c.ethnicity,
                  )
    # subquery = select(column,
    #                   func.count(column).label("count")
    #                   ).group_by(column).subquery()
    # stmt = select(subquery).filter(subquery.c.count > 1)
    with engine.connect() as connection:
        result = connection.execute(stmt)
        # result = result.rowcount
        for row in result:
            print(f'row = {row}')

    subquery1 = select(
        map.table.c.id,
        map.table.c.ethnicity_id,
        map.table.c.ext_id,
    ).subquery()
    subquery2 = select(
        map.table.c.ethnicity_id,
        func.count().label("count")
    ).group_by(map.table.c.ethnicity_id).subquery()
    subquery3 = select(subquery2).filter(subquery2.c.count < 10).subquery()

    print('----------------------------------------------------')
    subquery4 = select(
        subquery1,
        subquery3.c.count,
        # func.rank().over(order_by=map.table.c.ethnicity_id
        #                  ).label('rank')
    ).join(subquery3,
           subquery1.c.ethnicity_id == subquery3.c.ethnicity_id
           ).subquery()
    stmt = select(
        subquery4,
        func.dense_rank().over(order_by=subquery4.c.id
                               ).label('rank')
    )
    with engine.connect() as connection:
        result = connection.execute(stmt)
        # result = result.rowcount
        for row in result:
            print(f'row = {row}')


def good_column(table, labels, types):
    # I realize that this is horrifying ...
    for label in labels:
        if label in table.keys():
            col = table[label]
            if str(col.type) in types:
                return True

    return False


def good_log_cols(table):
    return (good_column(table, ['created_ts'], ["TIMESTAMP"]) and
            good_column(table, ['updated_ts'], ["TIMESTAMP"]) and
            good_column(table, ['created_by'], ["TEXT", "VARCHAR(200)"]) and
            good_column(table, ['updated_by'], ["TEXT", "VARCHAR(200)"]))


def duplicate_row_query(engine, column):
    subquery = select(column,
                      func.count(column).label("count")
                      ).group_by(column).subquery()
    stmt = select(subquery).filter(subquery.c.count > 1)
    with engine.connect() as connection:
        result = connection.execute(stmt)
        result = result.rowcount
    return result


def one_to_one_data(engine, map_table, id_label, ext_id_label):
    if id_label in map_table.c.keys():
        id_column = map_table.c[id_label]
    else:
        return [None, None]

    if ext_id_label in map_table.c.keys():
        ext_id_column = map_table.c[ext_id_label]
    else:
        return [None, None]

    duplicate_ext_ids = duplicate_row_query(engine, ext_id_column)
    duplicate_ids = duplicate_row_query(engine, id_column)

    return [duplicate_ids, duplicate_ext_ids]
