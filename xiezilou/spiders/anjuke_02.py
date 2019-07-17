# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Anjuke02Spider(scrapy.Spider):
    name = 'anjuke_02'
    allowed_domains = ['anjuke.com']
    # start_urls = ['http://anjuke.com/']

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
        base_key = re.search("(.+)_\d+", Anjuke02Spider.name).group(1)
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
        items = XiezilouItem()
        data = response.meta.copy()
        items["housing_name"] = response.xpath("//h1[@class='tit-name']/span/text()").extract_first()
        housing_info = response.xpath("//div[@id='fy_info']/ul")
        for branch in housing_info:
            for item in branch.xpath("./li"):
                label_name = item.xpath("./span")[0].xpath("./text()").extract_first().strip()
                label_value = item.xpath("./span")[2].xpath("./text()").extract_first().strip()
                if label_name == "类型":
                    items["housing_type"] = label_value
                if label_name == "日租金":
                    items["housing_price1"] = label_value
                if label_name == "月租金":
                    items["housing_price2"] = label_value
                if label_name == "押付":
                    items["pay_method"] = label_value
                if label_name == "楼盘":
                    label_value = item.xpath("./span[@class='desc']/a/text()").extract_first()
                    if label_value:
                        items["loupan"] = label_value.strip()
                    else:
                        label_value = item.xpath("./span[@class='desc']/text()").extract_first()
                        if label_value:
                            items["loupan"] = label_value.strip()
                if label_name == "地址":
                    items["building_address"] = label_value
                if label_name == "面积":
                    items["housing_area"] = label_value
                if label_name == "起租期":
                    items["rent_lease"] = label_value
                if label_name == "使用率":
                    items["housing_use_rate"] = label_value
                if label_name == "工位数":
                    items["housing_workplace"] = label_value
                if label_name == "物业费":
                    items["property_fee"] = label_value
                if label_name == "注册":
                    items["corp_reged"] = label_value
                if label_name == "楼层":
                    items["housing_floor"] = label_value
                if label_name == "装修":
                    items["housing_decor"] = label_value
                if label_name == "单价":
                    items["housing_price1"] = label_value
                if label_name == "售价":
                    items["housing_price2"] = label_value
        peitao = []
        items["central_air_condition"] = "无"
        suites = response.xpath("//ul[@class='mod-peitao clearfix']/li[@class='']")
        for item in suites:
            ss = item.xpath("./p/text()").extract_first()
            if ss == "中央空调":
                items["central_air_condition"] = "有"
            if ss:
                peitao.append(ss)
        items["peitao"] = ",".join(peitao)
        trains = []
        planes = []
        train_info = response.xpath("//dl[@class='train_box clearfix']/dd[@class='clearfix']/div")
        plane_info = response.xpath("//dl[@class='plane_box clearfix']/dd[@class='clearfix']/div")
        for item in train_info:
            label_name = item.xpath("./span")[0].xpath("./text()").extract_first()
            label_value = item.xpath("./span")[1].xpath("./text()").extract_first()
            trains.append(label_name + " " + label_value)
        for item in plane_info:
            if item.xpath("./span"):
                label_name = item.xpath("./span")[0].xpath("./text()").extract_first()
                label_value = item.xpath("./span")[1].xpath("./text()").extract_first()
                planes.append(label_name + " " + label_value)
        items["traffic"] = ",".join(trains) + "； " + ",".join(planes)
        items["publish_time"] = response.xpath("//div[@class='hd-sub']/text()")[1].extract().strip()
        items["agent"] = response.xpath("//div[@class='bro-info clearfix']/h5[@class='name']/text()").extract_first().strip()
        items["agent_phone"] = response.xpath("//div[@class='broker_tel']/text()").extract_first().strip()
        items["agent_company"] = response.xpath("//p[@class='comp_info']/a/text()").extract_first().strip()
        items["city"] = data["city"]
        items["district"] = data["district"]
        items["street"] = data["street"]
        items["flag"] = data["flag"]
        items["housing_url"] = data["housing_url"]
        zs_key = data['zs_key']
        if self.redis.zscore(zs_key, data['housing_url']) == 2:
            yield items
