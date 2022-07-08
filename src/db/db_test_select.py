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
print(df_kms)

db_connection.commit()
db_connection.close()

db_connection = sqlite3.connect('../../data/raw/db_zkill')
db_cursor = db_connection.cursor()

db_cursor.execute('''
          SELECT
          *
          FROM km_pilot
          ''')

df_pilots = pd.DataFrame(db_cursor.fetchall(),
                         columns=['km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance'])
print(df_pilots)

db_connection.commit()
db_connection.close()
