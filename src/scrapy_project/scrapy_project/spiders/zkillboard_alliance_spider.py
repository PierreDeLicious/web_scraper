from io import StringIO
import scrapy
from lxml import etree
import uuid
import os
from sqlite3 import dbapi2
from contextlib import closing


class ZKillboardSpider(scrapy.Spider):
    name = "zkillboard_alliance"
    _url = 'https://zkillboard.com'
    _db_conn_path = os.path.abspath('/Users/80932462/repos/web_scraper/data/raw/db_zkill.db')

    def escape_single_quote(self, string_value):
        return str(string_value.replace("'", "''"))

    def insert_km(self, km_id_value, km_system_value, km_region_value, km_time_value):
        km_already_existed = False

        count_km = 0

        try:
            with closing(dbapi2.connect(self._db_conn_path, isolation_level=None)) as db_connection:
                query_count = "SELECT COUNT(*) FROM kill_mail WHERE km_id = " + str(km_id_value)
                count_km = db_connection.cursor().execute(query_count).fetchall()[0][0]
        except dbapi2.Error as exc:
            self.logger.warn("dbapi2 exception encountered: %s" % exc)

        if count_km < 1:
            try:
                with closing(dbapi2.connect(self._db_conn_path, isolation_level=None)) as db_connection:
                    query_insert_km = "INSERT INTO kill_mail (km_id, km_system, km_region, km_time) VALUES ('" + \
                                        self.escape_single_quote(str(km_id_value)) + "', '" + \
                                        self.escape_single_quote(str(km_system_value)) + "', '" + \
                                        self.escape_single_quote(str(km_region_value)) + "', '" + \
                                        self.escape_single_quote(str(km_time_value)) + "')"
                    db_connection.cursor().execute(query_insert_km)
            except dbapi2.Error as exc:
                self.logger.warn("dbapi2 exception encountered: %s" % exc)
        else:
            self.logger.info('This KM is already recorded %s', km_id_value)
            km_already_existed = True

        return km_already_existed

    def insert_pilot(self, km_id_value, pilot_name_value, pilot_corporation_value, pilot_alliance_value, pilot_ship_value):
        try:
            with closing(dbapi2.connect(self._db_conn_path, isolation_level=None)) as db_connection:
                query_insert_pilot = "INSERT INTO km_pilot (pilot_km_id, km_id, pilot_name, pilot_ship, pilot_corporation, pilot_alliance) VALUES ('" + \
                                        self.escape_single_quote(str(uuid.uuid4())) + "', '" + \
                                        self.escape_single_quote(str(km_id_value)) + "', '" + \
                                        self.escape_single_quote(str(pilot_name_value)) + "', '" + \
                                        self.escape_single_quote(str(pilot_ship_value)) + "', '" + \
                                        self.escape_single_quote(str(pilot_corporation_value)) + "', '" + \
                                        self.escape_single_quote(str(pilot_alliance_value)) + "');"
                db_connection.cursor().execute(query_insert_pilot)
        except dbapi2.Error as exc:
            self.logger.warn("dbapi2 exception encountered: %s" % exc)
            self.logger.warn("pilot name: %s" % pilot_name_value)
            self.logger.warn("km id: %s" % km_id_value)

        return km_id_value

    def insert_pilot_with_km(self, km_id_value, pilot_name_value, pilot_corporation_value, pilot_alliance_value, pilot_ship_value, km_system_value, km_region_value, km_time_value):
        try:
            with closing(dbapi2.connect(self._db_conn_path, isolation_level=None)) as db_connection:
                query_insert_pilot_with_km = "INSERT INTO km_pilot_with_kill_mail (pilot_km_id, pilot_name, pilot_ship, pilot_corporation, pilot_alliance, km_id, km_system, km_region, km_time) VALUES ('" + \
                                                 self.escape_single_quote(str(uuid.uuid4())) + "', '" + \
                                                 self.escape_single_quote(str(pilot_name_value)) + "', '" + \
                                                 self.escape_single_quote(str(pilot_ship_value)) + "', '" + \
                                                 self.escape_single_quote(str(pilot_corporation_value)) + "', '" + \
                                                 self.escape_single_quote(str(pilot_alliance_value)) + "', '" + \
                                                 self.escape_single_quote(str(km_id_value)) + "', '" + \
                                                 self.escape_single_quote(str(km_system_value)) + "', '" + \
                                                 self.escape_single_quote(str(km_region_value)) + "', '" + \
                                                 self.escape_single_quote(str(km_time_value)) + "');"
                db_connection.cursor().execute(query_insert_pilot_with_km)
        except dbapi2.Error as exc:
            self.logger.warn("dbapi2 exception encountered: %s" % exc)

        return km_id_value

    def start_requests(self):
        alliances = [
            '1966049571',
            '701459600',
            '99009569',
        ]

        for i in range(2):
            for alliance in alliances:
                yield scrapy.Request(url=self._url + '/alliance/' + alliance + '/page/' + str(i + 1) + '/',
                                     callback=self.parse_alliance)

    def parse_alliance(self, response):
        for kill_mail in response.xpath('//tr[contains(@class, \'killListRow\')]/td[1]/a/@href').getall():
            yield scrapy.Request(url=self._url + kill_mail, callback=self.parse_kill_mail)

    def parse_kill_mail(self, response):
        system = response.xpath('//th[text()=\'System:\']/../td/a[1]/text()').get()
        region = response.xpath('//th[text()=\'System:\']/../td/a[2]/text()').get()
        time = response.xpath('//td[@class=\'info_kill_dttm\']/text()').get()
        pilots = response.xpath('//tr[@class=\'attacker\']').getall()
        km_id = response.url[28:len(response.url) - 1]

        km_already_existed = self.insert_km(km_id, system, region, time)

        if km_already_existed is False:
            for pilot in pilots:
                parser = etree.XMLParser(recover=True)
                element = etree.parse(StringIO(pilot), parser)

                pilot_infos = element.xpath('//td[@class=\'pilotinfo\']//a/text()')
                if len(pilot_infos) == 9:
                    pilot_name = pilot_infos[0]
                    pilot_corporation = pilot_infos[2]
                    pilot_alliance = pilot_infos[4]
                    pilot_ship = pilot_infos[1]
                else:
                    # this is a npc
                    pilot_name = 'NPC'
                    pilot_corporation = pilot_infos[1]
                    pilot_alliance = pilot_infos[1]
                    pilot_ship = pilot_infos[0]

                self.insert_pilot(km_id, pilot_name, pilot_corporation, pilot_alliance, pilot_ship)
                self.insert_pilot_with_km(km_id, pilot_name, pilot_corporation, pilot_alliance, pilot_ship, system, region, time)
