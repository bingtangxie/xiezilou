# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Fangtianxia02Spider(scrapy.Spider):
    name = 'fangtianxia_02'
    allowed_domains = ['fang.com']
    # start_urls = ['http://fang.com/']

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
        base_key = re.search("(.+)_\d+", Fangtianxia02Spider.name).group(1)
        zs_key = base_key + "_xzl_zset"
        h_key = base_key + "_xzl_detail_url_hashtable"
        urls = set(self.redis.zrevrangebyscore(zs_key, 1, 1))
        while True:
            url = urls.pop().decode()
            if self.redis.zscore(zs_key, url) == 1:
                # self.redis.zadd(zs_key, {url: 2})
                data = json.loads(self.redis.hget(h_key, url))
                data['zs_key'] = zs_key
                yield scrapy.Request(url=url, callback=self.parse_detail, meta=data)
            total = len(urls)
            if total == 0:
                break

    def parse_detail(self, response):
        items = XiezilouItem()
        data = response.meta.copy()
        items['publish_time'] = response.xpath("//li[@class='date']").extract_first().lstrip("更新于")
        items['housing_name'] = response.xpath("//p[@class='card-title']/i/text()").extract_first().strip()
        items['housing_price1'] = response.xpath("//span[@class='price']/text()").extract_first()
        items['housing_price2'] = response.xpath("//span[@class='unit']/text()").extract_first().strip(" |")
        print(items)

