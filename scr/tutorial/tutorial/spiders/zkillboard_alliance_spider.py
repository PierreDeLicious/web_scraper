from io import StringIO

import scrapy
from lxml import etree

class ZKillboardSpider(scrapy.Spider):
    name = "zkillboard_alliance"
    _url = 'https://zkillboard.com'

    def start_requests(self):
        alliances = [
            '1966049571',
            '701459600',
            '99009569',
        ]

        for i in range(10):
            for alliance in alliances:
                yield scrapy.Request(url=self._url + '/alliance/' + alliance + '/page/' + str(i+1) + '/', callback=self.parse_alliance)

    def parse_alliance(self, response):
        for kill_mail in response.xpath('//tr[contains(@class, \'killListRow\')]/td[1]/a/@href').getall():
            yield scrapy.Request(url=self._url + kill_mail, callback=self.parse_kill_mail)

    def parse_kill_mail(self, response):
        system = response.xpath('//th[text()=\'System:\']/../td/a[1]/text()').get()
        region = response.xpath('//th[text()=\'System:\']/../td/a[2]/text()').get()
        time = response.xpath('//td[@class=\'info_kill_dttm\']/text()').get()
        pilots = response.xpath('//tr[@class=\'attacker\']').getall()
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
                #this is a npc
                pilot_name = 'NPC'
                pilot_corporation = pilot_infos[1]
                pilot_alliance = pilot_infos[1]
                pilot_ship = pilot_infos[0]

            print('name: ' + pilot_name)
            print('corpo: ' + pilot_corporation)
            print('alliance: ' + pilot_alliance)
            print('ship: ' + pilot_ship)
            print('--- **** ---')