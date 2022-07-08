import sqlite3

db_connection = sqlite3.connect('../../data/raw/db_zkill')
db_cursor = db_connection.cursor()

db_cursor.execute('''
        CREATE TABLE IF NOT EXISTS kill_mail 
        (   [km_id] TEXT NOT NULL, 
            [km_system] TEXT NOT NULL, 
            [km_region] TEXT NOT NULL, 
            [km_time] TEXT NOT NULL,
            PRIMARY KEY (km_id))
        ''')

db_cursor.execute('''
          CREATE TABLE IF NOT EXISTS km_pilot
          ( [km_id] TEXT NOT NULL, 
            [pilot_name] TEXT NOT NULL, 
            [pilot_ship] TEXT NOT NULL, 
            [pilot_corporation] TEXT NOT NULL, 
            [pilot_alliance] TEXT NOT NULL,
            PRIMARY KEY (km_id, pilot_name),
            FOREIGN KEY (km_id) REFERENCES kill_mail (km_id) 
            ON DELETE CASCADE ON UPDATE NO ACTION)
          ''')

db_connection.commit()
db_connection.close()
