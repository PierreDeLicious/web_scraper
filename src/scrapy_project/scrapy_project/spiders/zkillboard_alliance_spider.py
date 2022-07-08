from io import StringIO
import scrapy
from lxml import etree
import uuid
import sqlite3


class ZKillboardSpider(scrapy.Spider):
    name = "zkillboard_alliance"
    _url = 'https://zkillboard.com'

    def escape_single_quote(self, string_value):
        return string_value.replace("'", "''")

    def km_insert(self, km_id_value, km_system_value, km_region_value, km_time_value):
        db_connection = sqlite3.connect('/Users/80932462/repos/web_scraper/data/raw/db_zkill')
        db_cursor = db_connection.cursor()
        km_already_existed = False

        count_km = \
            db_cursor.execute('SELECT count(*) FROM kill_mail WHERE km_id = \'' + km_id_value + '\'').fetchall()[0][
                0]

        if count_km < 1:
            db_cursor.execute(
                'INSERT INTO kill_mail (km_id, km_system, km_region, km_time) VALUES (\'' + km_id_value + '\',\'' + self.escape_single_quote(km_system_value) + '\',\'' + self.escape_single_quote(km_region_value) + '\',\'' + self.escape_single_quote(km_time_value) + '\')')
        else:
            self.logger.info('This KM is already recorded %s', km_id_value)
            km_already_existed = True

        db_connection.commit()
        db_connection.close()
        return km_already_existed

    def pilot_insert(self, km_id_value, pilot_name_value, pilot_corporation_value, pilot_alliance_value, pilot_ship_value):
        db_connection = sqlite3.connect('/Users/80932462/repos/web_scraper/data/raw/db_zkill')
        db_cursor = db_connection.cursor()

        db_cursor.execute(
                'INSERT INTO km_pilot (km_id, pilot_name, pilot_ship, pilot_corporation, pilot_alliance) VALUES (\'' + km_id_value + '\',\'' + self.escape_single_quote(pilot_name_value) + '\',\'' + self.escape_single_quote(pilot_ship_value) + '\',\'' + self.escape_single_quote(pilot_corporation_value) + '\',\'' + self.escape_single_quote(pilot_alliance_value) + '\')')

        db_connection.commit()
        db_connection.close()
        return km_id_value

    def start_requests(self):
        alliances = [
            '1966049571',
            '701459600',
            '99009569',
        ]

        for i in range(10):
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

        km_already_existed = self.km_insert(str(km_id), str(system), str(region), str(time))

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

                self.pilot_insert(str(km_id), str(pilot_name), str(pilot_corporation), str(pilot_alliance), str(pilot_ship))
