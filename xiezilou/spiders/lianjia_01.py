# -*- coding: utf-8 -*-
import scrapy
import re
import redis
import json
from scrapy.conf import settings


class Lianjia01Spider(scrapy.Spider):
    name = 'lianjia_01'
    allowed_domains = ['lianjia.com']
    start_urls = ['https://www.lianjia.com/city/']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        self.total = 0

    def parse(self, response):
        cities = response.xpath("//li[@class='city_list_li city_list_li_selected']")
        cities_list = []
        for item in cities:
            provinces_cities = item.xpath("./div[@class='city_list']/div[@class='city_province']")
            for province_cities in provinces_cities:
                province = province_cities.xpath("./div[@class='city_list_tit c_b']/text()").extract_first()
                cities = province_cities.xpath("./ul/li")
                for raw_city in cities:
                    city_name = raw_city.xpath("./a/text()").extract_first()
                    city_url = raw_city.xpath("./a/@href").extract_first()
                    cities_list.append({"province": province, "city": city_name, "city_url": city_url})
        cities_list_length = len(cities_list)
        for i in range(0, int(cities_list_length * 0.25)):
            url = cities_list[i]['city_url']
            province = cities_list[i]['province']
            city = cities_list[i]['city']
            yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province, "city": city})
        # for i in range(int(cities_list_length * 0.25), int(cities_list_length * 0.5)):
        #     url = cities_list[i]['city_url']
        #     province = cities_list[i]['province']
        #     city = cities_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province, "city": city})
        # for i in range(int(cities_list_length * 0.5), int(cities_list_length * 0.75)):
        #     url = cities_list[i]['city_url']
        #     province = cities_list[i]['province']
        #     city = cities_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province, "city": city})
        # for i in range(int(cities_list_length * 0.75), cities_list_length):
        #     url = cities_list[i]['city_url']
        #     province = cities_list[i]['province']
        #     city = cities_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"province": province, "city": city})

    def parse_city(self, response):
        province = response.meta['province']
        city = response.meta['city']
        navs = response.xpath("//a[@class='']")
        for item in navs:
            label_name = item.xpath("./text()").extract_first().strip()
            label_url = item.xpath("./@href").extract_first()
            if label_name == "商业办公":
                yield scrapy.Request(url=label_url, callback=self.parse_flag, meta={"province": province, "city": city})

    def parse_flag(self, response):
        province = response.meta["province"]
        city = response.meta["city"]
        flags = response.xpath("//ul[@class='search__nav']/li")
        for item in flags:
            label_name = item.xpath("./a/text()").extract_first().strip()
            label_url = response.urljoin(item.xpath("./@href").extract_first())
            if label_name == "写字楼租赁":
                districts = response.xpath("//ul[@class='filter__item-level1 cf']/li")
                for district in districts:
                    district_name = district.xpath("./text()").extract_first().strip()
                    district_url = response.url + "?district={dn}&diType={dt}".format(dn=district_name, dt="区域")
                    yield scrapy.Request(url=district_url, callback=self.parse_street, meta={"province": province, "city": city, "flag": label_name, "district": district_name})
            if label_name == "写字楼买卖":
                label_url = label_url.replace("rent", "buy")
                yield scrapy.Request(url=label_url, callback=self.parse_district, meta={"province": province, "city": city, "flag": label_name})

    def parse_district(self, response):
        province = response.meta["province"]
        city = response.meta["city"]
        flag = response.meta["flag"]
        districts = response.xpath("//ul[@class='filter__item-level1 cf']/li")
        for district in districts:
            district_name = district.xpath("./text()").extract_first().strip()
            district_url = response.url + "?district={dn}&diType={dt}".format(dn=district_name, dt="区域")
            yield scrapy.Request(url=district_url, callback=self.parse_street, meta={"province": province, "city": city, "flag": flag, "district": district_name})

    def parse_street(self, response):
        province = response.meta["province"]
        city = response.meta["city"]
        district = response.meta["district"]
        flag = response.meta["flag"]
        housing_list = response.xpath("//div[@class='result__ul']")
        if housing_list:
            streets = response.xpath("//ul[@class='filter__item-level2 cf']/li")
            for street in streets:
                street_name = street.xpath("./text()").extract_first().strip()
                if street_name == "不限":
                    pass
                else:
                    street_url = response.url + "&bizcircle={s}".format(s=street_name)
                    yield scrapy.Request(url=street_url, callback=self.parse_list, meta={"province": province, "city": city, "district": district, "street": street_name, "flag": flag})

    def parse_list(self, response):
        province = response.meta["province"]
        city = response.meta["city"]
        district = response.meta["district"]
        street = response.meta['street']
        flag = response.meta["flag"]
        housing_list = response.xpath("//div[@class='result__ul']")
        if housing_list:
            self.total += 1
            print(self.total)
            for housing in housing_list:
                housing_url = response.urljoin(housing.xpath("./a[@class='result__li']/@href").extract_first())
                # data = {'province': province, 'city': city, 'district': district, 'street': street,
                #         'referer': response.url, 'housing_url': housing_url, 'flag': flag}
                # base_key = re.search("(.+)_\d+", Lianjia01Spider.name).group(1)
                # hash_key = base_key + "_xzl_detail_url_hashtable"
                # set_key = base_key + "_xzl_zset"
                # self.redis.hset(hash_key, housing_url, json.dumps(data))
                # self.redis.zadd(set_key, {housing_url: 1})
            pagination = response.xpath("//a[@class='result__page-next js-page']")
            if pagination:
                base_url = response.url
                try:
                    search = re.search(".+(&page=\d+)", response.url).group(1)
                    if search:
                        base_url = response.url.replace(search, "")
                except Exception as e:
                    pass
                next_page = pagination.xpath("./@data-page").extract_first()
                next_page_url = base_url + "&page={p}".format(p=next_page)
                yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta={"province": province, "city": city, "district": district, "street": street, "flag": flag})


