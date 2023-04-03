import sqlalchemy as sqla
from sqlalchemy import select, func
from new_schema import Status, Action
from thing import Thing
import time
from new_schema import NewDatabaseSchema

print('loading utilities ...')
SHORT_LOAD = False


def join_thing_and_map(thing: Thing, action: Thing) -> sqla.Select:
    subquery1 = select(
        thing.get_id_column().label('old_id'),
        thing.get_name_column().label('name'),
        thing.table.c.updated_ts.label("t_updated_ts"),
        thing.table.c.updated_by.label("t_updated_by"),
        func.rank().over(order_by=thing.get_id_column()
                         ).label('rank')
    ).subquery()
    if SHORT_LOAD:
        subquery2 = select(subquery1).filter(subquery1.c.rank < 11).subquery()
    else:
        subquery2 = select(subquery1).subquery()

    subquery3 = select(
        subquery2,
        # map.table.c.id,
        action.table.c.ext_id,
        action.table.c.updated_ts.label("m_updated_ts"),
        action.table.c.updated_by.label("m_updated_by"),
    ).join(action.table, action.get_id_column() == subquery2.c.old_id)
    return subquery3


def fill_thing_table(database: NewDatabaseSchema,
                     data_table: sqla.Table,
                     engine: sqla.Engine,
                     thing: Thing,
                     thing_pk_start: int,

                     ) -> tuple[dict[int, int], int]:
    print(f'   fill_thing_table:  {thing.thing}')
    subquery1 = select(
        thing.get_id_column().label('old_id'),
        thing.get_name_column().label('name'),
        thing.table.c.updated_ts.label("t_updated_ts"),
        thing.table.c.updated_by.label("t_updated_by"),
        func.rank().over(order_by=thing.get_id_column()
                         ).label('rank')
    ).subquery()
    if SHORT_LOAD:
        subquery2 = select(subquery1).filter(subquery1.c.rank < 11)
    else:
        subquery2 = select(subquery1)
    stmt = subquery2
    thing_add = []
    bridge_add = []
    bridge: dict[int, int] = {}
    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        row_num = result.rowcount
        for row in result:
            thing_add.append(
                {
                    'n_id': thing_pk_start + index,
                    'name': row.name,
                    'action': Action.create,
                    'created_ts': row.t_updated_ts,
                    'created_by': row.t_updated_by,
                    'status': Status.draft,
                })
            bridge_add.append(
                {'old_id': row.old_id,
                 'thing': thing.thing,
                 'n_id': thing_pk_start + index,
                 })
            bridge[row.old_id] = thing_pk_start + index
            index += 1
    # values is used for the type table
    values = [{'n_id': thing_pk_start + index, 'thing': thing.thing} for index in range(row_num)]

    with database.engine.connect() as new_connection:
        # working
        insert_type = database.thing_table.insert()
        new_connection.execute(insert_type, values)

        insert_thing = data_table.insert()
        new_connection.execute(insert_thing, thing_add)

        # working
        bridge_ins = database.bridge_table.insert()
        new_connection.execute(bridge_ins, bridge_add)

        new_connection.commit()
    return bridge, thing_pk_start + index


def test_fill(database: NewDatabaseSchema,
              engine: sqla.Engine,
              thing: Thing,
              action: Thing,
              vendor: str,
              bridge: dict[int, int]
              ) -> None:
    vendor_pk = database.add_vendor(vendor)

    print(f'      test_fill ({action.action}, {thing.thing}, {vendor})')
    stmt = join_thing_and_map(thing, action)
    map_add = []

    start_time = time.time()
    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        # row_num = result.rowcount
        for row in result:
            old_pk = row.old_id
            new_pk = bridge[old_pk]
            # print(f'old-new:  {old_pk} : {new_pk}')
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
    # print('map add len')
    # print(len(map_add))
    if len(map_add) > 0:
        with database.engine.connect() as new_connection:
            insert_map = database.name_map_table.insert()
            new_connection.execute(insert_map, map_add)

            new_connection.commit()
    duration = time.time() - start_time
    print(f'         Duration: {duration}')


def test_fill_many(engine: sqla.Engine, thing: Thing, action: Thing) -> None:
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
        action.table.c.id,
        action.table.c.ethnicity_id,
        action.table.c.ext_id,
    ).subquery()
    subquery2 = select(
        action.table.c.ethnicity_id,
        func.count().label("count")
    ).group_by(action.table.c.ethnicity_id).subquery()
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
