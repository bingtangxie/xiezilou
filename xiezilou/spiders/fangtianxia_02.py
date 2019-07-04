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
                self.redis.zadd(zs_key, {url: 2})
                data = json.loads(self.redis.hget(h_key, url))
                data['zs_key'] = zs_key
                yield scrapy.Request(url=url, callback=self.parse_detail, meta=data)
            total = len(urls)
            if total == 0:
                break

    def parse_detail(self, response):
        data = response.meta.copy()
        noresult = response.xpath("//dl[@class='searchNoInfo']")
        if not noresult:
            data['publish_time'] = response.xpath("//p[@class='gray9']/span[@class='mr10']")[2].xpath("./text()").extract_first().lstrip("发布时间：")
            data['housing_name'] = response.xpath("//div[@class='title']/h1/text()").extract_first().strip()
            price1 = response.xpath("//dt[@class='gray6 zongjia1']")
            if price1:
                data['housing_price1'] = price1.xpath("./span[@class='red20b']/text()").extract_first() + price1.xpath("./span[@class='black']/text()").extract_first()
                if len(price1[0].xpath("./text()")) > 2:
                    data['housing_price1'] += price1[0].xpath("./text()")[2].extract()
            price2 = response.xpath("//dt[@style='margin-left: 0px']")
            if price2:
                data['housing_price2'] = price2.xpath("./span/span[@class='red']/text()").extract_first() + price2.xpath("./span[@class='black']/text()").extract_first().strip() + price2.xpath("./text()").extract_first().strip()
            # data['housing_area'] = response.xpath("//dd[@class='gray6']/span[@class='black']/text()").extract_first()
            data['phone'] = response.xpath("//label[@id='mobilecode']/text()").extract_first()
            housing_detail_url = "https:" + response.xpath("//div[@class='report_fy clearfix']/a/@href").extract_first()
            if housing_detail_url:
                data['housing_detail_url'] = housing_detail_url
                house_info = re.search("https://report.fang.com/office/(.+)_(\d+).+", housing_detail_url)
                house_type = house_info.group(1)
                house_id = house_info.group(2)
                data["house_type"] = house_type
                data["house_id"] = house_id
                params = {
                    "customizedParam": {
                        "houseid": str(house_id),
                        "houseProperty": "office",
                        "houseType": house_type,
                        "city": data["city"]
                    },
                    "isUpdateData": "0",
                    "ownerId": "shop",
                    "reportType": "office",
                    "moduleId": "278"
                }
                headers = {
                    'Content-Type': 'application/json'
                }
                yield scrapy.FormRequest(url="https://report.fang.com/proxy/report/1.0/module/templateModuleResult", method='POST', headers=headers, callback=self.parse_detail_info_278, body=json.dumps(params), meta=data.copy())
            else:
                self.redis.zadd(data["zs_key"], {data["housing_url"]: 4})

    def parse_detail_info_278(self, response):
        data = response.meta.copy()
        res = json.loads(response.text)
        data["agent"] = res["data"]["data"]["result"]["managername"]
        data["agent_phone"] = res["data"]["data"]["result"]["phone"]
        data["agent_company"] = res["data"]["data"]["result"]["comname"]
        params = {
                    "customizedParam": {
                        "houseid": str(data["house_id"]),
                        "houseProperty": "office",
                        "houseType": data["house_type"],
                        "city": data["city"]
                    },
                    "isUpdateData": "0",
                    "ownerId": "shop",
                    "reportType": "office",
                    "moduleId": "281"
                }
        headers = {
            'Content-Type': 'application/json'
        }
        yield scrapy.FormRequest(url="https://report.fang.com/proxy/report/1.0/module/templateModuleResult", method="POST", body=json.dumps(params), headers=headers, callback=self.parse_detail_info_281, meta=data.copy())

    def parse_detail_info_281(self, response):
        data = response.meta.copy()
        res = json.loads(response.text)["data"]["data"]["result"]
        # data["housing_price1"] = res["price"]
        data["pay_method"] = res["paydetail"]
        data["business_circle"] = res["comarea"]
        data["loupan"] = res["projname"]
        data["housing_area"] = res["allacreage"] + "㎡"
        data["housing_floor"] = res["floor"]
        data["building_address"] = res["address"]
        data["property_level"] = res["propertygrade"]
        data["housing_decor"] = res["fitment"]
        data["property_fee"] = res["wuyefei"] + "元/平米·月"
        params = {
            "customizedParam": {
                "houseid": str(data["house_id"]),
                "houseProperty": "office",
                "houseType": data["house_type"],
                "city": data["city"]
            },
            "isUpdateData": "0",
            "ownerId": "shop",
            "reportType": "office",
            "moduleId": "205"
        }
        headers = {
            'Content-Type': 'application/json'
        }
        yield scrapy.FormRequest(url="https://report.fang.com/proxy/report/1.0/module/templateModuleResult",
                                 method="POST", body=json.dumps(params), headers=headers,
                                 callback=self.parse_detail_info_205, meta=data.copy())

    def parse_detail_info_205(self, response):
        data = response.meta.copy()
        res = json.loads(response.text)["data"]["data"]["result"]
        zs_key = data["zs_key"]
        items = XiezilouItem()
        data["bangong"] = res["shopInfoMap"]["办公"]
        data["zb_suite"] = res["shopInfoMap"]["配套"]
        data["traffic"] = res["shopInfoMap"]["交通"]
        data["place"] = res["shopInfoMap"]["地段"]
        if "housing_price2" in data:
            items["province"] = data["province"]
        if "city" in data:
            items["city"] = data["city"]
        if "district" in data:
            items["district"] = data["district"]
        if "street" in data:
            items["street"] = data["street"]
        if "xzl_type" in data:
            items["xzl_type"] = data["xzl_type"]
        if "flag" in data:
            items["flag"] = data["flag"]
        if "housing_url" in data:
            items["housing_url"] = data["housing_url"]
        if "publish_time" in data:
            items["publish_time"] = data["publish_time"]
        if "housing_name" in data:
            items["housing_name"] = data["housing_name"]
        if "housing_price1" in data:
            items["housing_price1"] = data["housing_price1"]
        if "housing_price2" in data:
            items["housing_price2"] = data["housing_price2"]
        if "pay_method" in data:
            items["pay_method"] = data["pay_method"]
        if "business_circle" in data:
            items["business_circle"] = data["business_circle"]
        if "loupan" in data:
            items["loupan"] = data["loupan"]
        if "housing_floor" in data:
            items["housing_floor"] = data["housing_floor"]
        if "building_address" in data:
            items["building_address"] = data["building_address"]
        if "agent" in data:
            items["agent"] = data["agent"]
        if "agent_phone" in data:
            items["agent_phone"] = data["agent_phone"]
        if "agent_company" in data:
            items["agent_company"] = data["agent_company"]
        if "property_level" in data:
            items["property_level"] = data["property_level"]
        if "housing_decor" in data:
            items["housing_decor"] = data["housing_decor"]
        if "property_fee" in data:
            items["property_fee"] = data["property_fee"]
        if "bangong" in data:
            items["bangong"] = data["bangong"]
        if "zb_suite" in data:
            items["zb_suite"] = data["zb_suite"]
        if "traffic" in data:
            items["traffic"] = data["traffic"]
        if "place" in data:
            items["place"] = data["place"]
        if "housing_detail_url" in data:
            items["housing_detail_url"] = data["housing_detail_url"]
        if "phone" in data:
            items["phone"] = data["phone"]
        if "housing_area" in data:
            items["housing_area"] = data["housing_area"]
        if self.redis.zscore(zs_key, data['housing_url']) == 2:
            yield items