import scrapy

class Character(scrapy.Item):
    first_name = scrapy.Field()
    last_name = scrapy.Field()

class SimpsonSpider(scrapy.Spider):
    """Spider to scrape name of simpsons character from list on wikipedia"""
    name = 'simpsons'
    start_urls = ["https://en.wikipedia.org/wiki/List_of_The_Simpsons_characters"]

    def parse(self, response):
        table = response.css('table.wikitable')[0]
        for i in table.css('tr td::text').extract():
            if not i=='\n':
                l = i.replace('\n','').split(' ')
                character = Character()
                character['first_name'] = l[0]
                character['last_name'] = l[1] if 1<len(l) else 'Doe'
                yield character