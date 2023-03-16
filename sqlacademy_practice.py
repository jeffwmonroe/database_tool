import sqlalchemy
from sqlalchemy import text

engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:aqua666@localhost:5432/practice', echo=True)

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())


# with engine.connect() as conn:
#     conn.execute(text("CREATE TABLE xy_table (x int, y int)"))
#     conn.execute(
#         text("INSERT INTO xy_table (x, y) VALUES (:x, :y)"),
#         [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
#     )
#     conn.commit()

# "begin once"
# with engine.begin() as conn:
#     conn.execute(
#         text("INSERT INTO xy_table (x, y) VALUES (:x, :y)"),
#         [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
#     )

with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM xy_table"))
    for row in result:
        print(f"x: {row.x}  y: {row.y}")