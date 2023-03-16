# import itertools

# import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import text

engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:aqua666@localhost:5432/ontology', echo=True)

column_list = ['artist_id', 'artist_name', 'created_ts', 'updated_ts', 'created_by', 'updated_by']
with engine.connect() as conn:
    result = conn.execute(text("SELECT artist_id, artist_name, created_ts, updated_ts, created_by, updated_by FROM ontology.artist"))
    print(f'   rowcount = {result.rowcount}')

    col = result.fetchall()


print("After the database connection:")
print(f'type(col) = {type(col)}')

# df = pd.DataFrame(data=[], columns=['artist_id, artist_name, created_ts'])
df_dic = {'artist_id': [],
          'artist_name': [],
          'created_ts': [],
          'updated_ts': [],
          'created_by': [],
          'updated_by': []
          }
for i in range(len(col)):
    # print(f'row = {col[i]}')
    ts = col[i].created_ts

    df_dic['artist_id'].append(col[i].artist_id)
    df_dic['artist_name'].append(col[i].artist_name)
    df_dic['created_ts'].append(str(col[i].created_ts))
    df_dic['updated_ts'].append(str(col[i].updated_ts))
    df_dic['created_by'].append(col[i].created_by)
    df_dic['updated_by'].append(col[i].updated_by)

# df = pd.DataFrame(df_dic)
print('-----------------------------------------')

print("after pandas")

print('-----------------------------------------------')
print('dataframe:')
df = pd.DataFrame(df_dic)
print(df.head())


print("connection must be good!")
