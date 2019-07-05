# -*- coding: utf-8 -*-
import scrapy
import redis
import re
import json
from scrapy.conf import settings


class Anjuke01Spider(scrapy.Spider):
    name = 'anjuke_01'
    allowed_domains = ['anjuke.com']
    start_urls = ['https://www.anjuke.com/sy-city.html']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    def parse(self, response):
        content = response.xpath('//div[@class="letter_city"]/ul/li')
        entrance_list = []
        for block in content:
            label = block.xpath("./label/text()").extract_first()
            cities = block.xpath("./div[@class='city_list']/a")
            if label == "其他":
                pass
            else:
                for city in cities:
                    city_name = city.xpath("./text()").extract_first()
                    city_url = city.xpath("./@href").extract_first()
                    entrance_list.append({"city_url": city_url, "city": city_name})
        entrance_list_length = len(entrance_list)
        for i in range(entrance_list_length):
            url = entrance_list[i]['city_url']
            city = entrance_list[i]['city']
            if city == "北京":
                yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(0, int(entrance_list_length * 0.25)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(int(entrance_list_length * 0.25), int(entrance_list_length * 0.5)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(int(entrance_list_length * 0.5), int(entrance_list_length * 0.75)):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

        # for i in range(int(entrance_list_length * 0.75), entrance_list_length):
        #     url = entrance_list[i]['city_url']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city, meta={"city": city})

    def parse_city(self, response):
        data = response.meta.copy()
        navs = response.xpath("//li[@class='li_single li_itemsnew li_unselected']")
        for entry in navs:
            item = entry.xpath("./a[@class='a_navnew']/text()").extract_first()
            if item == "商铺写字楼":
                nav1 = entry.xpath("./div[@class='sec_divnew ']")
                nav2 = entry.xpath("./div[@class='sec_divnew']/div[@class='sec_navlist']")
                if nav1:
                    links = nav1.xpath("./a")
                    for link in links:
                        label = link.xpath("./text()").extract_first().strip()
                        if label == "写字楼出租":
                            label_url = link.xpath("./@href").extract_first().strip()
                            data['flag'] = label
                            yield scrapy.Request(url=label_url, callback=self.parse_district, meta=data.copy())
                        if label == "写字楼出售":
                            label_url = link.xpath("./@href").extract_first().strip()
                            data['flag'] = label
                            yield scrapy.Request(url=label_url, callback=self.parse_district, meta=data.copy())
                if nav2:
                    links = nav2.xpath("./div[@class='third_navlist']")
                    for link in links:
                        label = link.xpath("./div[@class='third_nav_title']/text()").extract_first()
                        if label == "写字楼":
                            aas = link.xpath("./a")
                            for a in aas:
                                a_name = a.xpath("./text()").extract_first().strip()
                                if a_name == "写字楼出租":
                                    a_url = a.xpath("./@href").extract_first()
                                    data['flag'] = a_name
                                    yield scrapy.Request(url=a_url, callback=self.parse_district, meta=data.copy())
                                if a_name == "写字楼出售":
                                    a_url = a.xpath("./@href").extract_first()
                                    data['flag'] = a_name
                                    yield scrapy.Request(url=a_url, callback=self.parse_district, meta=data.copy())

            else:
                pass

    def parse_district(self, response):
        data = response.meta.copy()
        housing_list = response.xpath("//div[@class='list-item']")
        if housing_list:
            secs = response.xpath("//div[@class='items']")
            for sec in secs:
                sec_name = sec.xpath("./label[@class='item-title']/text()").extract_first().strip("：")
                if sec_name == "区域":
                    districts = sec.xpath("./div[@class='elems-l']/a")
                    for district in districts:
                        district_name = district.xpath("./text()").extract_first()
                        if district_name == "全部":
                            pass
                        else:
                            district_url = district.xpath("./@href").extract_first()
                            data["district"] = district_name
                            yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())

    def parse_street(self, response):
        data = response.meta.copy()
        housing_list = response.xpath("//div[@class='list-item']")
        if housing_list:
            streets = response.xpath("//div[@class='sub-items']/a")
            for street in streets:
                street_name = street.xpath("./text()").extract_first()
                street_url = street.xpath("./@href").extract_first()
                if street_name == "全部":
                    pass
                else:
                    data["street"] = street_name
                    yield scrapy.Request(url=street_url, callback=self.parse_list, meta=data.copy())

    def parse_list(self, response):
        data = response.meta.copy()
        housing_list = response.xpath("//div[@class='list-item']")
        if housing_list:
            for housing in housing_list:
                housing_url = housing.xpath("./@link").extract_first()
                data['housing_url'] = housing_url
                data['referer'] = response.url
                base_key = re.search("(.+)_\d+", Anjuke01Spider.name).group(1)
                hash_key = base_key + "_xzl_detail_url_hashtable"
                set_key = base_key + "_xzl_zset"
                self.redis.hset(hash_key, housing_url, json.dumps(data))
                self.redis.zadd(set_key, {housing_url: 1})
            pagination = response.xpath("//a[@class='aNxt']")
            if pagination:
                next_page_url = pagination.xpath("./@href").extract_first()
                yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())
