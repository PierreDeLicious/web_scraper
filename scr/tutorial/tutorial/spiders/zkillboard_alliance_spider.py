import scrapy


class ZKillboardSpider(scrapy.Spider):
    name = "zkillboard_alliance"
    _url = 'https://zkillboard.com'

    def start_requests(self):
        alliances = [
            '1966049571',
            '701459600',
            '99009569',
        ]
        for alliance in alliances:
            yield scrapy.Request(url=self._url + '/alliance/' + alliance + '/page/1/', callback=self.parse_alliance)

    def parse_alliance(self, response):
        for kill_mail in response.xpath('//tr[contains(@class, \'killListRow\')]/td[1]/a/@href').getall():
            yield scrapy.Request(url=self._url + kill_mail, callback=self.parse_kill_mail)

    def parse_kill_mail(self, response):

        system = response.xpath('//th[text()=\'System:\']/../td/a[1]/text()').get()
        region = response.xpath('//th[text()=\'System:\']/../td/a[2]/text()').get()
        time = response.xpath('//td[@class=\'info_kill_dttm\']/text()').get()
        print('response' + str(type(response)))

        pilots = response.xpath('//tr[@class=\'attacker\']').getall()
        for pilot in pilots:

            print(type(pilot))
            print(pilot)
            print()
            pilot_name = pilot.xpath('//td[@class=\'pilotinfo\']/div[1]/a[1]/text()')
            pilot_corporation = pilot.xpath('//td[@class=\'pilotinfo\']/div[1]/a[3]/text()')
            pilot_alliance = pilot.xpath('//td[@class=\'pilotinfo\']/div[1]/a[4]/text()')
            pilot_ship = pilot.xpath('//td[@class=\'pilotinfo\']/div[1]/a[2]/text()')
            print(pilot_name)