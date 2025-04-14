import json
import scrapy


class DistrictsSpider(scrapy.Spider):
    name = 'districts'
    allowed_domains = ['https://api.divar.ir']
    start_urls = ['https://api.divar.ir/v8/web-search/tehran/buy-residential']

    def parse(self, response):
        data = json.loads(response.body)
        districts = data['input_suggestion']['json_schema']['properties']['districts']['properties']['vacancies']['items']['enum']
        with open('districts.json', 'w') as json_file:
            json.dump(districts, json_file, ensure_ascii=False)
