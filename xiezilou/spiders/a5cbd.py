# -*- coding: utf-8 -*-
import scrapy
import redis
import re
import json
import pymongo
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class A5cbdSpider(scrapy.Spider):
    name = 'a5cbd'
    allowed_domains = ['5acbd.com']
    # start_urls = ['http://www.5acbd.com/office.html']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)
        self.sum = 0

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
        urls = {
            "http://www.5acbd.com/cbdsearch.html?d=2807": "上城",
            "http://www.5acbd.com/cbdsearch.html?d=2808": "下城",
            "http://www.5acbd.com/cbdsearch.html?d=2809": "江干",
            "http://www.5acbd.com/cbdsearch.html?d=2810": "拱墅",
            "http://www.5acbd.com/cbdsearch.html?d=2811": "西湖",
            "http://www.5acbd.com/cbdsearch.html?d=2812": "滨江",
            "http://www.5acbd.com/cbdsearch.html?d=2814": "萧山",
            "http://www.5acbd.com/cbdsearch.html?d=2815": "余杭",
            "http://www.5acbd.com/cbdsearch.html?d=2816": "桐庐",
            "http://www.5acbd.com/cbdsearch.html?d=2817": "淳安",
            "http://www.5acbd.com/cbdsearch.html?d=2818": "建德",
            "http://www.5acbd.com/cbdsearch.html?d=2819": "富阳",
            "http://www.5acbd.com/cbdsearch.html?d=2820": "临安",
            "http://www.5acbd.com/cbdsearch.html?d=3239": "其他",
            "http://www.5acbd.com/cbdsearch.html?d=3247": "安吉",
            "http://www.5acbd.com/cbdsearch.html?d=3248": "大江东",
            "http://www.5acbd.com/cbdsearch.html?d=3249": "湖州",
            "http://www.5acbd.com/cbdsearch.html?d=3250": "钱塘新"
        }
        for url in urls:
            district = urls[url]
            yield scrapy.Request(url=url, callback=self.parse_list, meta={"city": "杭州", "district": district})

    def parse_list(self, response):
        data = response.meta.copy()
        housing_list = response.xpath("""//tr[@onmouseover="this.style.backgroundColor='#f7f7ff'"]""")
        if housing_list:
            for housing in housing_list:
                data["housing_name"] = housing.xpath("./td")[0].xpath("./a/text()").extract_first().strip()
                housing_info = housing.xpath("./td")[0].xpath("./text()")[2].extract().strip().split()
                data["housing_price1"] = housing.xpath("./td")[1].xpath("./text()").extract_first().strip()
                data["housing_area"] = housing.xpath("./td")[2].xpath("./text()").extract_first().strip()
                data["flag"] = housing.xpath("./td")[3].xpath("./span/text()").extract_first().strip()
                data["property_type"] = housing.xpath("./td")[4].xpath("./text()").extract_first().strip()
                data["publish_time"] = housing.xpath("./td")[5].xpath("./text()").extract_first()
                data["xzl_type"] = housing_info[0].split("：")[1]
                data["business_circle"] = housing_info[1].split("：")[1]
                data["housing_address"] = housing.xpath("./td")[0].xpath("./text()")[1].extract().strip()
                housing_url = response.urljoin(housing.xpath("./td")[0].xpath("./a/@href").extract_first())
                data["housing_url"] = housing_url
                data['referer'] = response.url
                # base_key = re.search("(.+)_\d+", A5cbdSpider_01.name).group(1)
                # hash_key = base_key + "_xzl_detail_url_hashtable"
                # set_key = base_key + "_xzl_zset"
                # self.redis.hset(hash_key, housing_url, json.dumps(data))
                # self.redis.zadd(set_key, {housing_url: 1})
                yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta=data.copy())
                # time.sleep(1)
            pagination = response.xpath("//div[@id='ctl00_cph_AspNetPager1']/table/tr/td")[1].xpath("./a")
            if pagination:
                for item in pagination:
                    label = item.xpath("./text()").extract_first().strip()
                    if label == "【下一页】":
                        next_page_uri = item.xpath("./@href").extract_first()
                        next_page_url = response.urljoin(next_page_uri)
                        yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())

    def parse_detail(self, response):
        data = response.meta.copy()
        items = XiezilouItem()
        housing_info = response.xpath("//table")[0].xpath("./tr")
        for td in housing_info:
            td00 = td.xpath("./td")[0].xpath("./text()").extract_first()
            if td00 == "楼层":
                td01 = td.xpath("./td")[1].xpath("./text()").extract_first()
                if td01:
                    items["housing_floor"] = td01.strip()
        peitao_info = response.xpath("//table[@id='li-12']/tr")
        for entry in peitao_info:
            label_name_01 = entry.xpath("./td")[0].xpath("./text()").extract_first()
            label_value_01 = entry.xpath("./td")[1].xpath("./text()").extract_first()
            label_name_02 = entry.xpath("./td")[2].xpath("./text()").extract_first()
            label_value_02 = entry.xpath("./td")[3].xpath("./text()").extract_first()
            if label_value_01:
                label_value_01 = label_value_01.strip()
            if label_value_02:
                label_value_02 = label_value_02.strip()
            if label_name_01 == "空调：":
                items["air_condition"] = label_value_01
            if label_name_01 == "装修情况：":
                items["housing_decor"] = label_value_01
            if label_name_01 == "公交站点：":
                items["gj_site"] = label_value_01
            if label_name_01 == "物业公司：":
                items["property_company"] = label_value_01
            if label_name_01 == "地铁线：":
                items["subway_line"] = label_value_01
            if label_name_01 == "电梯：":
                items["elevator"] = label_value_01
            if label_name_01 == "标准层高：":
                items["layer_height"] = label_value_01
            if label_name_02 == "车位：":
                items["parking_place"] = label_value_02
            if label_name_02 == "交付时间：":
                items["built_in"] = label_value_02
            if label_name_02 == "公交路线：":
                items["gj_line"] = label_value_02
            if label_name_02 == "物业费用：":
                items["property_fee"] = label_value_02
            if label_name_02 == "地铁站点：":
                items["subway_site"] = label_value_02
            if label_name_02 == "土地性质：":
                items["land_property"] = label_value_02
        items["housing_name"] = data["housing_name"]
        items["housing_price1"] = data["housing_price1"]
        items["housing_area"] = data["housing_area"]
        items["flag"] = data["flag"]
        items["property_type"] = data["property_type"]
        items["publish_time"] = data["publish_time"]
        items["xzl_type"] = data["xzl_type"]
        items["business_circle"] = data["business_circle"]
        items["building_address"] = data["housing_address"]
        items["housing_url"] = data["housing_url"]
        items["city"] = data["city"]
        items["district"] = data["district"]
        if not self.redis.sismember(A5cbdSpider.name + "xzl_set", data["housing_url"]):
            yield items
