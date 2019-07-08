# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Haozu02Spider(scrapy.Spider):
    name = 'haozu_02'
    allowed_domains = ['haozu.com']
    # start_urls = ['http://haozu.com/']

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
        base_key = re.search("(.+)_\d+", Haozu02Spider.name).group(1)
        zs_key = base_key + "_xzl_zset"
        h_key = base_key + "_xzl_detail_url_hashtable"
        urls = set(self.redis.zrevrangebyscore(zs_key, 1, 1))
        # total = len(urls)
        # for i in range(0, total):
        #     url = urls.pop().decode()
        #     if self.redis.zscore(zs_key, url) == 1:
        #         data = json.loads(self.redis.hget(h_key, url))
        #         yield scrapy.Request(url=url, callback=self.parse_detail, meta=data)
        #     else:
        #         pass
        while True:
            url = urls.pop().decode()
            if self.redis.zscore(zs_key, url) == 1:
                self.redis.zadd(zs_key, {url: 2})
                data = json.loads(self.redis.hget(h_key, url))
                data['h_key'] = h_key
                data['zs_key'] = zs_key
                yield scrapy.Request(url=url, callback=self.parse_detail, meta=data.copy())
            total = len(urls)
            if total == 0:
                break

    def parse_detail(self, response):
        data=response.meta.copy()
        flag = data['flag']
        if 'xzl_type' in data:
            xzl_type = data['xzl_type']
        if 'share' in data:
            share = data['share']
        if flag == "写字楼出租":
            if xzl_type == "共享办公" and share == "y":
                pass
            else:
                pass
        if flag == "写字楼出售":
            pass
