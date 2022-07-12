from sqlite3 import dbapi2
from contextlib import closing
import os
import pandas as pd

_db_conn_path = os.path.abspath('/Users/80932462/repos/web_scraper/data/raw/db_zkill.db')

df_kms = pd.DataFrame(columns=['km_id', 'km_system', 'km_region', 'km_time'])

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as db_connection:
        query = "SELECT * FROM kill_mail"
        df_kms = pd.DataFrame(db_connection.cursor().execute(query), columns=['km_id', 'km_system', 'km_region', 'km_time'])
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

df_pilots = pd.DataFrame(columns=['pilot_km_id', 'km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance'])

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as db_connection:
        query = "SELECT * FROM km_pilot"
        df_pilots = pd.DataFrame(db_connection.cursor().execute(query), columns=['pilot_km_id', 'km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance'])
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

df_pilots_km = pd.DataFrame(columns=['pilot_km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance', 'km_id', 'km_system', 'km_region', 'km_time'])

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as db_connection:
        query = "SELECT * FROM km_pilot_with_kill_mail"
        df_pilots_km = pd.DataFrame(db_connection.cursor().execute(query), columns=['pilot_km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance', 'km_id', 'km_system', 'km_region', 'km_time'])
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

df_kms['Day'] = df_kms['km_time']

df_kms['km_time'] = pd.to_datetime(df_kms['km_time'])
df_kms['day_of_week'] = df_kms['km_time'].dt.day_name()
df_kms['hour_of_day'] = df_kms['km_time'].dt.hour

df_kms['km_id'] = df_kms['km_id'].astype(str)
df_pilots['km_id'] = df_pilots['km_id'].astype(str)


# df_grouped = df_kms.groupby(['day_of_week', 'hour_of_day'])['hour_of_day'].count()
# print(df_grouped)

#df_stats = df_pilots.join(df_kms, on='km_id')

df_stats = df_pilots.merge(df_kms, on='km_id', how='outer')

print(df_kms)
print(df_pilots)
print(df_stats)
print(df_pilots_km)

print('df_kms has a size of: ' + str(df_kms.size) + ' with the following cols: ' + ' '.join(df_kms.columns))
print('df_pilots has a size of: ' + str(df_pilots.size) + ' with the following cols: ' + ' '.join(df_pilots.columns))

print('df_stats has a size of: ' + str(df_stats.size) + ' with the following cols: ' + ' '.join(df_stats.columns))
