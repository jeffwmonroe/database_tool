import sqlalchemy
from sqlalchemy import text, insert
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy import select, bindparam
from sqlalchemy import func, cast, literal_column, and_, or_, desc, asc

metadata_obj = MetaData()
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:aqua666@localhost:5432/practice', echo=True)

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)

print(user_table.primary_key)

address_table = Table(
    "address",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("user_id", ForeignKey("user_account.id"), nullable=False),
    Column("email_address", String, nullable=False),
)

metadata_obj.create_all(engine)

some_table = Table("address", metadata_obj, autoload_with=engine)
print(some_table)
print(some_table.c.email_address)

# scalar_subq = (
#     select(user_table.c.id)
#     .where(user_table.c.name == bindparam("username"))
#     .scalar_subquery()
# )
#
# with engine.connect() as conn:
#     result = conn.execute(
#         insert(address_table).values(user_id=scalar_subq),
#         [
#             {
#                 "username": "spongebob",
#                 "email_address": "spongebob@sqlalchemy.org",
#             },
#             {"username": "sandy", "email_address": "sandy@sqlalchemy.org"},
#             {"username": "sandy", "email_address": "sandy@squirrelpower.org"},
#         ],
#     )
#     conn.commit()


insert_stmt = insert(address_table).returning(
    address_table.c.id, address_table.c.email_address
)
print(insert_stmt)

select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
insert_stmt = insert(address_table).from_select(
    ["user_id", "email_address"], select_stmt
)
print(insert_stmt.returning(address_table.c.id, address_table.c.email_address))

print('-----------------------------------------------------------')
print('Select Statements')
stmt = select(user_table).where(user_table.c.name == "spongebob")
print(stmt)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)

print(select(user_table.c["name", "fullname"]))

stmt = select(
    ("Username: " + user_table.c.name).label("username"),
).order_by(user_table.c.name)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(f"{row.username}")

stmt = select(text("'some phrase'"), user_table.c.name).order_by(user_table.c.name)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)

stmt = select(literal_column("'some phrase'").label("p"), user_table.c.name).order_by(
    user_table.c.name
)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(f"{row.p}, {row.name}")

print('----------------------------------------')
print('Where')
print(select(user_table).where(user_table.c.name == "squidward"))

stmt = (select(address_table.c.email_address, user_table.c.name)
        .where(user_table.c.name == "sandy")
        .where(address_table.c.user_id == user_table.c.id)
        )


def connect_and_print(stmt):
    print('-------------')
    print('connect and print')
    print(stmt)
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            print(f"   row = {row}")


stmt = select(address_table.c.email_address, user_table.c.name).where(
    and_( or_(user_table.c.name == "squidward", user_table.c.name == "sandy"),
        address_table.c.user_id == user_table.c.id)
)


connect_and_print(stmt)

stmt = select(user_table).filter_by(name="spongebob", fullname="Spongebob Squarepants")
connect_and_print(stmt)

print(select(user_table.c.name))

stmt = select(user_table.c.name, address_table.c.email_address).join_from(user_table,
                                                                          address_table)
connect_and_print(stmt)

stmt = select(user_table.c.name, address_table.c.email_address).join(address_table)
connect_and_print(stmt)

stmt = (select(func.count("*")).select_from(user_table))
connect_and_print(stmt)

print(
    select(address_table.c.email_address)
    .select_from(user_table)
    .join(address_table, user_table.c.id == address_table.c.user_id)
)

stmt = select(user_table).order_by(user_table.c.name)
connect_and_print(stmt)


count_fn = func.count(user_table.c.id)
stmt = (count_fn)
connect_and_print(stmt)

print('--------------------------------')
print('          Group By')

stmt = (select(user_table.c.id, func.max(address_table.c.id).label("num_addresses"))
        .join(address_table)
        .group_by(user_table.c.id)
        # .having(func.count(address_table.c.id) > 0)
        .order_by(user_table.c.name, desc("num_addresses"))
        )
connect_and_print(stmt)

print("order by")
stmt = (select(address_table.c.user_id, func.count(address_table.c.user_id).label("num_addresses"))
        .group_by("user_id")
        # .having(func.count(address_table.c.id) > 0)
        .order_by(asc("num_addresses"))
        )
connect_and_print(stmt)

user_alias_1 = user_table.alias()
user_alias_2 = user_table.alias()
stmt = (
    select(user_alias_1.c.name, user_alias_2.c.name).join_from(
        user_alias_1, user_alias_2, user_alias_1.c.id > user_alias_2.c.id
    )
)
connect_and_print(stmt)
