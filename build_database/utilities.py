from sqlalchemy import select, func


def test_fill(engine, thing, map):
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

    subquery1 = select(map.table.c.id,
                       map.table.c.ethnicity_id,
                       map.table.c.ext_id,
                       func.rank().over(order_by=map.table.c.id
                                        ).label("rank")
                       ).subquery()
    subquery2 = select( map.table.c.ethnicity_id,
                        func.count().label("count")
                        ).group_by(map.table.c.ethnicity_id).subquery()
    subquery3 = select(subquery2).filter(subquery2.c.count < 10).subquery()

    print('----------------------------------------------------')
    stmt = select(subquery1, subquery3).join(subquery3, subquery1.c.ethnicity_id == subquery3.c.ethnicity_id)
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
