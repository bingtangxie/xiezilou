# -*- coding: utf-8 -*-
import scrapy
import re
import redis
import pymongo
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Officese01Spider(scrapy.Spider):
    name = 'officese_01'
    allowed_domains = ['officese.com']
    # start_urls = ['http://officese.com/']

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
        uris = {
            "beijing": "北京",
            "tianjin": "天津",
            "sjz": "石家庄",
            "langfang": "廊坊",
            "baoding": "保定",
            "tangshan": "唐山",
            "dalian": "大连",
            "shenyang": "沈阳",
            "nanjing": "南京",
            "suzhou": "苏州",
            "wuxi": "无锡",
            "jinan": "济南",
            "qingdao": "青岛",
            "hangzhou": "杭州",
            "fuzhou": "福州",
            "hefei": "合肥",
            "huaian": "淮安",
            "guangzhou": "广州",
            "shenzhen": "深圳",
            "changsha": "长沙",
            "xiamen": "厦门",
            "chengdu": "成都",
            "chongqing": "重庆",
            "wuhan": "武汉",
            "zhengzhou": "郑州",
            "xian": "西安",
            "shanghai": "上海"
        }
        for uri in uris:
            url = "http://{uri}.officese.com/rent".format(uri=uri)
            city = uris[uri]
            if city == "上海":
                yield scrapy.Request(url=url, callback=self.parse_district, meta={"city": city})

    def parse_district(self, response):
        data = response.meta.copy()
        districts = response.xpath("//dl[@class='quxian']/dd/a")
        for district in districts:
            district_name = district.xpath("./text()").extract_first()
            district_url = response.urljoin(district.xpath("./@href").extract_first())
            if district_name != "不限":
                data["district"] = district_name
                yield scrapy.Request(url=district_url, callback=self.parse_list, meta=data.copy())

    def parse_list(self, response):
        data = response.meta.copy()
        housing_list = response.xpath("//div[@class='house']")
        if housing_list:
            for housing in housing_list:
                housing_url = housing.xpath("./dl/dt/p/a/@href").extract_first()
                data["housing_url"] = housing_url
                data["housing_name"] = housing.xpath("./dl/dt/p")[1].xpath("./text()").extract_first().strip()
                data["housing_area"] = housing.xpath("./dl/dd[@class='area']/span/text()").extract_first()
                data["publish_time"] = housing.xpath("./dl/dd[@class='office']/span[@class='black']/text()").extract_first().strip()
                data["housing_price1"] = housing.xpath("./dl/dd[@class='office']/strong/text()").extract_first() + housing.xpath("./dl/dd[@class='office']/span")[0].xpath("./text()").extract_first()
                data["refer"] = response.url
                yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta=data.copy())
            pagination = response.xpath("//td[@valign='bottom']/a")
            if pagination:
                for entry in pagination:
                    content = entry.xpath("./font[@color='red']/font[@face='webdings']")
                    if content:
                        nr = content.xpath("./text()").extract_first().strip()
                        if nr == "8":
                            next_page_url = response.urljoin(entry.xpath("./@href").extract_first())
                            yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())

    def parse_detail(self, response):
        data = response.meta.copy()
        items = XiezilouItem()
        items["building_address"] = response.xpath("//div[@class='base_info']/dl[@class='borderb mb10']/dt")[2].xpath("./text()").extract_first().strip()
        items["flag"] = response.xpath("//div[@class='base_info']/dl[@class='borderb mb10']/dd")[0].xpath("./text()").extract()[1]
        items["xzl_type"] = response.xpath("//div[@class='base_info']/dl[@class='borderb mb10']/dd")[1].xpath("./span/text()").extract()[1].strip("型：")
        items["corp_reged"] = response.xpath("//div[@class='base_info']/dl[@class='borderb mb10']/dd")[2].xpath("./span/text()").extract_first().split("：")[1].strip()
        info = response.xpath("//dl[@class='info_c']/dd")[0].extract()
        if not re.search("个人", info):
            items["agent"] = response.xpath("//span[@id='agentname']/text()").extract_first()
            items["agent_company"] = response.xpath("//dl[@class='info_c']/dd[@class='black']/a/b/text()").extract_first()
            items["agent_phone"] = response.xpath("//dl[@class='info_c']/dd[@class='gray6']/span/text()").extract_first()
        else:
            raw_agent_phone = response.xpath("//dl[@class='info_c']/dd[@class='gray6']/span/b/text()").extract_first()
            items["agent_phone"] = re.search(".+(\d{11})", raw_agent_phone, re.M).group(1)
        items["city"] = data["city"]
        items["district"] = data["district"]
        items["housing_url"] = data["housing_url"]
        items["housing_name"] = data["housing_name"]
        items["housing_area"] = data["housing_area"]
        items["publish_time"] = data["publish_time"]
        items["housing_price1"] = data["housing_price1"]
        if not self.redis.sismember(Officese01Spider.name + "_xzl_set", data["housing_url"]):
            yield items

