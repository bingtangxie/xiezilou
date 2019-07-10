# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import re
import json
from scrapy.conf import settings


class Tongcheng58Spider(scrapy.Spider):
    name = 'tongcheng58_01'
    allowed_domains = ['58.com']
    start_urls = ['https://www.58.com/changecity.html']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = cls(*args, **kwargs)
    #     spider._set_crawler(crawler)
    #     redis_host = spider.settings['REDIS_HOST']
    #     redis_port = spider.settings['REDIS_PORT']
    #     redis_db = spider.settings['REDIS_DB']
    #     redis_password = spider.settings['REDIS_PASS']
    #     mongo_host = spider.settings['MONGO_HOST']
    #     mongo_port = spider.settings['MONGO_PORT']
    #     mongo_db = spider.settings['MONGO_DB']
    #     spider.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
    #     spider.db = pymongo.MongoClient(host=mongo_host, port=mongo_port)[mongo_db]
    #     return spider

    def parse(self, response):
        res = response.text
        independent_city = re.search("independentCityList = (.+?)}", res, re.S).group(1) + "}"
        cities = re.search("cityList = (.+?)<\/script>", res, re.S).group(1)
        city_codes = json.loads(cities)
        independent_city_codes = json.loads(independent_city)
        city_codes.pop("其他")
        city_codes.pop("海外")
        all_keys = list(city_codes.keys())
        all_keys_length = len(all_keys)
        for i in range(0, int(all_keys_length * 0.25)):
            province = all_keys[i]
            for city in city_codes[province]:
                code = city_codes[province][city].split("|")[0]
                url = "https://{code}.58.com".format(code=code)
                yield scrapy.Request(url=url, callback=self.entrance, meta={'province': province, 'city': city.strip()})

        # for i in range(int(all_keys_length * 0.25), int(all_keys_length * 0.5)):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.entrance, meta={'province': province, 'city': city.strip()})
        #
        # for i in range(int(all_keys_length * 0.5), int(all_keys_length * 0.75)):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.entrance, meta={'province': province, 'city': city.strip()})

        # for i in range(int(all_keys_length * 0.75), all_keys_length):
        #     province = all_keys[i]
        #     for city in city_codes[province]:
        #         code = city_codes[province][city].split("|")[0]
        #         url = "https://{code}.58.com".format(code=code)
        #         yield scrapy.Request(url=url, callback=self.entrance, meta={'province': province, 'city': city.strip()})

        # for indep_city in independent_city_codes:
        #     in_code = independent_city_codes[indep_city].split("|")[0]
        #     in_url = "https://{code}.58.com".format(code=in_code)
        #     if in_code == "cq":
        #         yield scrapy.Request(url=in_url, callback=self.entrance,
        #                              meta={'province': indep_city.strip(), 'city': indep_city.strip()})

    def entrance(self, response):
        province = response.meta['province']
        city = response.meta['city']
        chuzu = response.xpath("//a[@tongji_tag='pc_home_fc_xzlcz']")
        if chuzu:
            search_type = "chuzu"
            chuzu_uri = chuzu.xpath("./@href").extract_first()
            chuzu_url = response.urljoin(chuzu_uri)
            yield scrapy.Request(url=chuzu_url, callback=self.parse_district,
                                 meta={'province': province, 'city': city, 'search_type': search_type})

    def parse_district(self, response):
        province = response.meta['province']
        city = response.meta['city']
        housing_list = response.xpath("//ul[@class='house-list-wrap']/li")
        if housing_list:
            distrcits = response.xpath("//div[@id='qySelectFirst']/a[@name='b_link']")
            for district in distrcits:
                district_name = district.xpath("./text()").extract_first()
                district_url = district.xpath("./@href").extract_first()
                yield scrapy.Request(url=district_url, callback=self.parse_street,
                                     meta={'province': province, 'city': city, 'district': district_name,
                                           })

    def parse_street(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        housing_list = response.xpath("//ul[@class='house-list-wrap']/li")
        if housing_list:
            streets = response.xpath("//div[@id='qySelectSecond']/a")
            if streets:
                for street in streets:
                    street_name = street.xpath("./text()").extract_first()
                    street_url = street.xpath("./@href").extract_first()
                    yield scrapy.Request(url=street_url, callback=self.parse_type,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street_name})

    def parse_type(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xzl_types = response.xpath("//dl[@class='secitem']")[0].xpath("./dd/a")
        housing_list = response.xpath("//ul[@class='house-list-wrap']/li")
        if housing_list:
            # 如果street下有返回列表，再进行写字楼分类
            for item in xzl_types:
                item_name = item.xpath("./text()").extract_first().strip()
                item_url = item.xpath("./@href").extract_first().strip()
                if item_name == "纯写字楼":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                # if item_name == "商业综合体":
                #     xzl_type = item_name
                #     yield scrapy.Request(url=item_url, callback=self.parse_list,
                #                          meta={'province': province, 'city': city, 'district': district,
                #                                'street': street,
                #                                'xzl_type': xzl_type, 'flag': 'chuzu'})
                #     # 这种方式会被去重处理
                #     print("next")
                #     yield scrapy.Request(url=item_url, callback=self.parse_flag,
                #                          meta={'province': province, 'city': city, 'district': district,
                #                                'street': street, 'xzl_type': xzl_type, 'flag': 'chushou'})
                #     print("finished")
                if item_name == "商业综合体":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                if item_name == "商务公寓":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})
                if item_name == "商务酒店":
                    xzl_type = item_name
                    yield scrapy.Request(url=item_url, callback=self.parse_flag,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type})

    def parse_flag(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xzl_type = response.meta['xzl_type']
        housing_list = response.xpath("//ul[@class='house-list-wrap']/li")
        if housing_list:
            flags = response.xpath("//ul[@class='house-nav-tab']/li")
            for item in flags:
                label = item.xpath("./a/text()").extract_first()
                item_url = item.xpath("./a/@href").extract_first()
                if label == "出租":
                    flag_url = item_url
                    yield scrapy.Request(url=flag_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type, 'flag': label})
                if label == "出售":
                    flag_url = item_url
                    yield scrapy.Request(url=flag_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'xzl_type': xzl_type,
                                               'flag': label})

    def parse_list(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        xzl_type = response.meta['xzl_type']
        flag = response.meta['flag']
        housing_list = response.xpath("//ul[@class='house-list-wrap']/li")
        if housing_list:
            for item in housing_list:
                housing = item.xpath("./div[@class='list-info']/h2/a")
                housing_url = housing.xpath("./@href").extract_first()
                base_key = re.search("(.+)_\d+", Tongcheng58Spider.name).group(1)
                hash_key = base_key + "_xzl_detail_url_hashtable"
                set_key = base_key + "_xzl_zset"
                data = {'province': province, 'city': city, 'district': district, 'street': street,
                        'xzl_type': xzl_type, 'referer': response.url, 'housing_url': housing_url, 'flag': flag}
                self.redis.hset(hash_key, housing_url, json.dumps(data))
                self.redis.zadd(set_key, {housing_url: 1})
            pagination = response.xpath("//a[@class='next']")
            if pagination:
                next_url = pagination.xpath("./@href").extract_first()
                yield scrapy.Request(url=next_url, callback=self.parse_list,
                                     meta={'province': province, 'city': city, 'district': district, 'street': street,
                                           'xzl_type': xzl_type, 'flag': flag})
