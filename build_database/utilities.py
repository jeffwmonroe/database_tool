

def good_column(table, labels, types):
    # I realize that this is horrifying ...
    for label in labels:
        if label in table.keys():
            col = table[label]
            # print(f'    column: {label}, {col.type}')
            if str(col.type) in types:
                return True
            # else:
            #     print(f'    not found!! {col.type} in {types}')
            #     print(str(col.type))

    return False


def good_log_cols(table):
    return (good_column(table, ['created_ts'], ["TIMESTAMP"]) and
            good_column(table, ['updated_ts'], ["TIMESTAMP"]) and
            good_column(table, ['created_by'], ["TEXT", "VARCHAR(200)"]) and
            good_column(table, ['updated_by'], ["TEXT", "VARCHAR(200)"]))
