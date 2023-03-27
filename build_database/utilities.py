from sqlalchemy import select, func, insert
from new_schema import Status, Action

def test_fill(database, engine, thing, map):
    # ToDo un-hardcode this
    database.add_vendor("cheetah")
    print('-------------------------------------------------')
    print('              test_fill')
    subquery1 = select(
        thing.table.c.artist_id,
        thing.table.c.artist_name,
        thing.table.c.updated_ts.label("t_updated_ts"),
        thing.table.c.updated_by.label("t_updated_by"),
        func.rank().over(order_by=thing.table.c.artist_id
                         ).label('rank')
    ).subquery()
    subquery2 = select(subquery1).subquery()
    # subquery2 = select(subquery1).filter(subquery1.c.rank < 11).subquery()

    subquery3 = select(
        subquery2,
        map.table.c.id,
        map.table.c.ext_id,
        map.table.c.updated_ts.label("m_updated_ts"),
        map.table.c.updated_by.label("m_updated_by"),
    ).join(map.table)
    stmt = subquery3
    thing_add =[]

    log_start = 2000
    pk_start = 3000
    index = 0
    with engine.connect() as connection:
        result = connection.execute(stmt)
        row_num = result.rowcount
        for row in result:

            thing_add.append(
                {'log_id': log_start + index,
                 'n_id': pk_start + index,
                 'name': row.artist_name,
                 'action': Action.create,
                 'created_ts': row.t_updated_ts,
                 'created_by': row.t_updated_by,
                 'status': Status.draft,
                 })
            index += 1

        # for row in result:
            # print(f'row = {row}')
            # print(f'pk = {row.artist_id}')
            # thing_add.append({'type': 'artist'})
            # database.add_thing_no_transaction(
            #     new_connection,
            # # database.add_thing(
            #     'artist',
            #     'cheetah',
            #     row.artist_id,
            #     row.artist_name,
            #     row.t_updated_by,
            #     row.t_updated_ts,
            #     row.ext_id,
            #     row.m_updated_by,
            #     row.m_updated_ts,
            # )
    values = [
        {'n_id': pk_start+indx, 'type': 'artist'}
        for indx in range(row_num)]
    # print(values)
    # print(thing_add)
    stmt1 = insert(database.type_table).values(values)
    stmt2 = insert(database.artist_table).values(thing_add)
    with database.engine.connect() as new_connection:
        result = new_connection.execute(stmt1)
        result = new_connection.execute(stmt2)
        new_connection.commit()
    # print(thing_add)
    # database.add_things(thing_add)


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
