# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Ganji02Spider(scrapy.Spider):
    name = 'ganji_02'
    allowed_domains = ['ganji.com']
    # start_urls = ['http://ganji.com/']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        redis_host = spider.settings['REDIS_HOST']
        redis_port = spider.settings['REDIS_PORT']
        redis_db = spider.settings['REDIS_DB']
        redis_password = spider.settings['REDIS_PASS']
        mongo_host = spider.settings['MONGO_HOST']
        mongo_port = spider.settings['MONGO_PORT']
        mongo_db = spider.settings['MONGO_DB']
        spider.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        spider.db = pymongo.MongoClient(host=mongo_host, port=mongo_port)[mongo_db]
        return spider

    def start_requests(self):
        base_key = re.search("(.+)_\d+", Ganji02Spider.name).group(1)
        zs_key = base_key + "_xzl_zset"
        h_key = base_key + "_xzl_detail_url_hashtable"
        urls = set(self.redis.zrevrangebyscore(zs_key, 1, 1))
        while True:
            url = urls.pop().decode()
            if self.redis.zscore(zs_key, url) == 1:
                self.redis.zadd(zs_key, {url: 2})
                data = json.loads(self.redis.hget(h_key, url))
                data['zs_key'] = zs_key
                yield scrapy.Request(url=url, callback=self.parse_detail, meta=data)
            total = len(urls)
            if total == 0:
                break

    def parse_detail(self, response):
        items = XiezilouItem()
        data = response.meta.copy()
        items['publish_time'] = response.xpath("//li[@class='date']/text()").extract_first().lstrip("更新于")
        items['housing_name'] = response.xpath("//p[@class='card-title']/i/text()").extract_first().strip()
        price_unit = response.xpath("//div[@class='price-wrap']/text()")[1].extract()
        price1 = response.xpath("//span[@class='price strongbox']") or response.xpath("//span[@class='price']")
        price2 = response.xpath("//span[@class='unit strongbox']") or response.xpath("//span[@class='unit']")
        items['housing_price1'] = price1.xpath("./text()").extract_first() + price_unit
        items['housing_price2'] = price2.xpath("./text()").extract_first().strip(" |")
        housing_info = response.xpath("//li[@class='item f-fl']")
        for item in housing_info:
            label = "".join(item.xpath("./span[@class='t']/text()").extract()).strip("：")
            content = item.xpath("./span[@class='content']/text()").extract_first().strip()
            if label == "面积":
                items['housing_area'] = content
            if label == "楼层":
                items['housing_floor'] = content
            if label == "装修":
                items["housing_decor"] = content
            if label == "租期":
                items["rent_lease"] = content
            if label == "区域":
                items["business_circle"] = content
        address = response.xpath("//li[@class='er-item f-fl']/span[@class='t2']")
        if address:
            items['building_address'] = response.xpath("//li[@class='er-item f-fl']/span[@class='content']/text()").extract_first().strip()
        items['agent'] = response.xpath("//div[@class='name']/a[@class='name']/text()").extract_first()
        items['agent_phone'] = response.xpath("//a[@class='phone_num js_person_phone']/text()").extract_first()
        if response.xpath("//div[@class='user_other']"):
            items['agent_company'] = response.xpath("//div[@class='user_other']")[0].xpath("./span[@class='company']/text()").extract_first()
        suites = response.xpath("//ul[@class='collocation f-clear']/li[@class='item']")
        peitao_raw = []
        if suites:
            for item in suites:
                label = item.xpath("./p[@class='text']/text()").extract_first()
                peitao_raw.append(label)
                if label == "中央空调":
                    items['central_air_condition'] = "有"
        items['peitao'] = ",".join(peitao_raw)
        items['city'] = data['city']
        items['district'] = data['district']
        items['street'] = data['street']
        items['xzl_type'] = data['xzl_type']
        items['housing_url'] = data['housing_url']
        items['flag'] = data['flag']
        zs_key = data['zs_key']
        if self.redis.zscore(zs_key, data['housing_url']) == 2:
            yield items



