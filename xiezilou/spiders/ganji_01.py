# -*- coding: utf-8 -*-
import scrapy
import re
import json
import redis
from scrapy.conf import settings


class Ganji01Spider(scrapy.Spider):
    name = 'ganji_01'
    allowed_domains = ['ganji.com']
    start_urls = ['http://www.ganji.com/index.htm']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    def parse(self, response):
        all_cities = response.xpath("//div[@class='all-city']/dl/dd")
        cities_list = []
        for cities in all_cities:
            for city in cities.xpath("./a"):
                city_name = city.xpath("./text()").extract_first()
                city_url = city.xpath("./@href").extract_first()
                cities_list.append({'city': city_name, 'city_url': city_url})
        cities_list_length = len(cities_list)
        for i in range(cities_list_length):
            url = cities_list[i]['city_url']
            city = cities_list[i]['city']
            if city == "北京":
                yield scrapy.Request(url=url, callback=self.parse_entrance,
                                     meta={'city': city})
        # for i in range(0, int(cities_list_length * 0.25)):
        #     city = cities_list[i]['city']
        #     url = cities_list[i]['city_url']
        #     yield scrapy.Request(url=url, callback=self.parse_entrance, meta={'city': city})
        # for i in range(int(cities_list_length * 0.25), int(cities_list_length * 0.5)):
        #     city = cities_list[i]['city']
        #     url = cities_list[i]['city_url']
        #     yield scrapy.Request(url=url, callback=self.parse_district, meta={'city': city})
        # for i in range(int(cities_list_length * 0.5), int(cities_list_length * 0.75)):
        #     city = cities_list[i]['city']
        #     url = cities_list[i]['city_url']
        #     yield scrapy.Request(url=url, callback=self.parse_district, meta={'city': city})
        # for i in range(int(cities_list_length * 0.75), cities_list_length):
        #     city = cities_list[i]['city']
        #     url = cities_list[i]['city_url']
        #     yield scrapy.Request(url=url, callback=self.parse_district, meta={'city': city})

    def parse_entrance(self, response):
        city = response.meta['city']
        chuzu = re.search(".+(<a.+)写字楼出租", response.text).group(1)
        if chuzu:
            url = re.search("href=\"(.+)\" class=", chuzu).group(1)
            entrance_url = response.urljoin(url)
            yield scrapy.Request(url=entrance_url, callback=self.parse_district, meta={'city': city})

    def parse_district(self, response):
        city = response.meta['city']
        districts = response.xpath("//ul[@class='f-clear']/li")
        housing_list = response.xpath("//div[@class='f-list-item ershoufang-list']")
        if housing_list:
            for i in range(len(districts)):
                if i == 0:
                    pass
                else:
                    district_name = districts[i].xpath("./a/text()").extract_first().strip()
                    district_url = "http:" + districts[i].xpath("./a/@href").extract_first()
                    # print(district_url)
                    yield scrapy.Request(url=district_url, callback=self.parse_street, meta={'city': city, 'district': district_name})

    def parse_street(self, response):
        city = response.meta['city']
        district = response.meta['district']
        streets = response.xpath("//div[@class='fou-list f-clear']/a")
        housing_list = response.xpath("//div[@class='f-list-item ershoufang-list']")
        if housing_list:
            for street in streets:
                street_name = street.xpath("./text()").extract_first().strip()
                street_url = "http:" + street.xpath("./@href").extract_first()
                yield scrapy.Request(url=street_url, callback=self.parse_type, meta={'city': city, 'district': district, 'street': street_name})

    def parse_type(self, response):
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        housing_list = response.xpath("//div[@class='f-list-item ershoufang-list']")
        if housing_list:
            xzl_types = response.xpath("//dd[@class='info f-clear']/a")
            for item in xzl_types:
                item_name = item.xpath("./text()").extract_first().strip()
                item_url = "http:" + item.xpath("./@href").extract_first()
                if item_name == "纯写字楼":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                if item_name == "商业综合体":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                if item_name == "商务公寓":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                if item_name == "商务酒店":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})

    def parse_flag(self, response):
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xzl_type = response.meta['xzl_type']
        housing_list = response.xpath("//div[@class='f-list-item ershoufang-list']")
        if housing_list:
            flags = response.xpath("//ul[@class='f-f-title f-clear']/li")
            for item in flags:
                label = item.xpath("./a/text()").extract_first()
                item_url = "http:" + item.xpath("./a/@href").extract_first()
                if label == "写字楼出租":
                    flag_url = item_url
                    yield scrapy.Request(url=flag_url, callback=self.parse_list,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type, 'flag': label})
                if label == "写字楼出售":
                    flag_url = item_url
                    yield scrapy.Request(url=flag_url, callback=self.parse_list,
                                         meta={'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type,
                                               'flag': label})

    def parse_list(self, response):
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xzl_type = response.meta['xzl_type']
        flag = response.meta['flag']
        housing_list = response.xpath("//div[@class='f-list-item ershoufang-list']")
        if housing_list:
            for item in housing_list:
                housing = item.xpath("./dl/dd[@class='dd-item title']/a")
                housing_url = "http:" + housing.xpath("./@href").extract_first()
                base_key = re.search("(.+)_\d+", Ganji01Spider.name).group(1)
                hash_key = base_key + "_xzl_detail_url_hashtable"
                set_key = base_key + "_xzl_zset"
                data = {'city': city, 'district': district, 'street': street,
                        'xzl_type': xzl_type, 'referer': response.url, 'housing_url': housing_url, 'flag': flag}
                self.redis.hset(hash_key, housing_url, json.dumps(data))
                self.redis.zadd(set_key, {housing_url: 1})
            pagination = response.xpath("//a[@class='next']")
            if pagination:
                next_url = pagination.xpath("./@href").extract_first()
                yield scrapy.Request(url=next_url, callback=self.parse_list,
                                     meta={'city': city, 'district': district, 'street': street,
                                           'xzl_type': xzl_type, 'flag': flag})


