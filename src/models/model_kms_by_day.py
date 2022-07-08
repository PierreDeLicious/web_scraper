import sqlite3
import pandas as pd

db_connection = sqlite3.connect('../../data/raw/db_zkill')
db_cursor = db_connection.cursor()

db_cursor.execute('''
          SELECT
          *
          FROM kill_mail
          ''')

df_kms = pd.DataFrame(db_cursor.fetchall(), columns=['km_id', 'km_system', 'km_region', 'km_time'])

db_cursor.execute('''
          SELECT
          *
          FROM km_pilot
          ''')

df_pilots = pd.DataFrame(db_cursor.fetchall(),
                         columns=['km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance'])

db_connection.close()

df_kms['Day'] = df_kms['km_time']

df_kms['km_time'] = pd.to_datetime(df_kms['km_time'])
df_kms['day_of_week'] = df_kms['km_time'].dt.day_name()
df_kms['hour_of_day'] = df_kms['km_time'].dt.hour

# df_grouped = df_kms.groupby(['day_of_week', 'hour_of_day'])['hour_of_day'].count()
# print(df_grouped)

df_pilots.merge(df_kms, how='left', on='km_id')

print(df_kms.columns)
print(df_pilots.columns)
