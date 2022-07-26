from sqlite3 import dbapi2
from contextlib import closing
import os
import pandas as pd

_db_conn_path = os.path.abspath('/Users/80932462/repos/web_scraper/data/raw/db_zkill.db')

df_pilots_km = pd.DataFrame(
    columns=['pilot_km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation', 'pilot_alliance', 'km_id', 'km_system',
             'km_region', 'km_time'])

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as db_connection:
        query = "SELECT * FROM km_pilot_with_kill_mail"
        df_pilots_km = pd.DataFrame(db_connection.cursor().execute(query),
                                    columns=['pilot_km_id', 'pilot_name', 'pilot_ship', 'pilot_corporation',
                                             'pilot_alliance', 'km_id', 'km_system', 'km_region', 'km_time'])
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

print(df_pilots_km['km_time'].unique())
