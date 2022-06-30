import scrapy


class ZKillboardSpider(scrapy.Spider):
    name = "zkillboard"
    _url = 'https://zkillboard.com'

    def start_requests(self):
        alliances = [
            '1966049571',
            '701459600',
            '99009569',
        ]
        for alliance in alliances:
            yield scrapy.Request(url=self._url + '/alliance/' + alliance + '/page/1/', callback=self.parse)

    def parse(self, response):

        for a in response.xpath('//tr[contains(@class, \'killListRow\')]/td[1]/a/@href').getall():
            yield {'km': a}
            print('page: ' + self._url + a)
