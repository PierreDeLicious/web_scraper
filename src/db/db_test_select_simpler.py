from sqlite3 import dbapi2
from contextlib import closing
import os

_db_conn_path = os.path.abspath('/Users/80932462/repos/web_scraper/data/raw/db_zkill.db')
_test_data_special_char_path = os.path.abspath('/Users/80932462/repos/web_scraper/data/test/test_special_chars')

km_id_value = '30'
km_system_value = '4'
km_region_value = open(_test_data_special_char_path, 'r').readlines()
km_time_value = '6'

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as conn:
        query = "INSERT INTO kill_mail VALUES ('" + \
                str(km_id_value) + "', '" + \
                str(km_system_value) + "', '" + \
                str(km_region_value[0]) + "', '" + \
                str(km_time_value) + "');"
        conn.cursor().execute(query)
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

count = 0

try:
    with closing(dbapi2.connect(_db_conn_path)) as conn:
        query = "SELECT COUNT(*) FROM kill_mail"
        count = conn.cursor().execute(query).fetchall()[0][0]
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)


print(str(count))

try:
    with closing(dbapi2.connect(_db_conn_path, isolation_level=None)) as conn:
        query = "DELETE FROM kill_mail;"
        conn.cursor().execute(query)
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

try:
    with closing(dbapi2.connect(_db_conn_path)) as conn:
        query = "SELECT COUNT(*) FROM kill_mail"
        count = conn.cursor().execute(query).fetchall()[0][0]
except dbapi2.Error as exc:
    print("Comdb2 exception encountered: %s" % exc)

print(str(count))
