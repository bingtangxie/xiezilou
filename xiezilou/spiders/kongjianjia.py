# -*- coding: utf-8 -*-
import scrapy
import redis
import re
import json
import pymongo
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class KongjianjiaSpider(scrapy.Spider):
    name = 'kongjianjia'
    allowed_domains = ['kongjianjia.com']
    # start_urls = ['http://kongjianjia.com/']

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
            "http://bj.kongjianjia.com/xiezilou": "北京",
            "http://dl.kongjianjia.com/xiezilou": "大连",
            "http://hz.kongjianjia.com/xiezilou": "杭州",
            "http://lf.kongjianjia.com/xiezilou": "廊坊",
            "http://sz.kongjianjia.com/xiezilou": "深圳",
            "http://sjz.kongjianjia.com/xiezilou": "石家庄",
            "http://sy.kongjianjia.com/xiezilou": "沈阳",
            "http://sh.kongjianjia.com/xiezilou": "上海",
            "http://wh.kongjianjia.com/xiezilou": "武汉",
            "http://cd.kongjianjia.com/xiezilou": "成都",
            "http://cq.kongjianjia.com/xiezilou": "重庆",
            "http://gz.kongjianjia.com/xiezilou": "广州",
            "http://jn.kongjianjia.com/xiezilou": "济南",
            "http://nj.kongjianjia.com/xiezilou": "南京",
            "http://tj.kongjianjia.com/xiezilou": "天津",
            "http://zz.kongjianjia.com/xiezilou": "郑州"
        }
        information = {
            "http://bj.kongjianjia.com/xiezilou": {"place1": "52",
                                                   "center": "116.40196900000000000,39.91648500000000000",
                                                   "city": "北京"},
            "http://dl.kongjianjia.com/xiezilou": {"place1": "245",
                                                   "center": "121.62133100000000000,38.91966500000000000",
                                                   "city": "大连"},
            "http://hz.kongjianjia.com/xiezilou": {"place1": "383",
                                                   "center": "120.16164300000000000,30.27970000000000000",
                                                   "city": "杭州"},
            "http://lf.kongjianjia.com/xiezilou": {"place1": "144",
                                                   "center": "116.69048400000000000,39.54407700000000000",
                                                   "city": "廊坊"},
            "http://sz.kongjianjia.com/xiezilou": {"place1": "77",
                                                   "center": "114.06601300000000000,22.54940500000000000",
                                                   "city": "深圳"},
            "http://sjz.kongjianjia.com/xiezilou": {"place1": "138",
                                                    "center": "114.52131100000000000,38.04823600000000000",
                                                    "city": "石家庄"},
            "http://sy.kongjianjia.com/xiezilou": {"place1": "244",
                                                   "center": "114.52131100000000000,38.04823600000000000",
                                                   "city": "沈阳"},
            "http://sh.kongjianjia.com/xiezilou": {"place1": "321",
                                                   "center": "121.48789900000000000,31.24916200000000000",
                                                   "city": "上海"},
            "http://wh.kongjianjia.com/xiezilou": {"place1": "180",
                                                   "center": "114.31183100000000000,30.59842800000000000",
                                                   "city": "武汉"},
            "http://cd.kongjianjia.com/xiezilou": {"place1": "322",
                                                   "center": "104.07297400000000000,30.57831100000000000",
                                                   "city": "成都"},
            "http://cq.kongjianjia.com/xiezilou": {"place1": "394",
                                                   "center": "106.55823400000000000,29.56904500000000000",
                                                   "city": "重庆"},
            "http://gz.kongjianjia.com/xiezilou": {"place1": "76",
                                                   "center": "113.27079300000000000,23.13530800000000000",
                                                   "city": "广州"},
            "http://jn.kongjianjia.com/xiezilou": {"place1": "283",
                                                   "center": "117.12648800000000000,36.65819400000000000",
                                                   "city": "济南"},
            "http://nj.kongjianjia.com/xiezilou": {"place1": "220",
                                                   "center": "117.12648800000000000,36.65819400000000000",
                                                   "city": "南京"},
            "http://tj.kongjianjia.com/xiezilou": {"place1": "343",
                                                   "center": "117.12648800000000000,36.65819400000000000",
                                                   "city": "天津"},
            "http://zz.kongjianjia.com/xiezilou": {"place1": "149",
                                                   "center": "113.63176800000000000,34.75343300000000000",
                                                   "city": "郑州"},
        }
        for url in information:
            city = information[url]["city"]
            place1 = information[url]["place1"]
            center = information[url]["center"]
            params = {
                "place1": place1,
                "typeId": "1",
                "area": "0",
                "price": "0",
                "des_state": "0",
                "center": center,
                "yixiang": "1",
                "currentPage": "1",
                "user_id": "0",
                "source": "web"
            }
            headers = {
                'Content-Type': 'application/json'
            }
            post_url = "http://service.kongjianjia.com/index/Search/search"
            yield scrapy.FormRequest(url=post_url, callback=self.parse_list, body=json.dumps(params), method="POST", headers=headers, meta={"city": city, "base_url": url, "place1": place1, "center": center})

    def parse_list(self, response):
        raw_data = response.meta.copy()
        res = json.loads(response.text)
        if res["code"] == "0":
            data = res["data"]
            page = res["page"]
            for entry in data:
                housing_id = entry["id"]
                housing_name = entry["title"]
                district = entry["place2Name"]
                street = entry["place3Name"]
                housing_address = entry["address"]
                housing_url = raw_data["base_url"] + "/{housing_id}".format(housing_id=housing_id)
                raw_data["district"] = district
                raw_data["street"] = street
                raw_data["housing_address"] = housing_address
                raw_data["housing_name"] = housing_name
                raw_data["housing_url"] = housing_url
                yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta=raw_data.copy())
            page_count = page["pageCount"]
            current_page = page["currentPage"]
            if page_count > current_page:
                post_url = "http://service.kongjianjia.com/index/Search/search"
                params = {
                    "place1": raw_data["place1"],
                    "typeId": "1",
                    "area": "0",
                    "price": "0",
                    "des_state": "0",
                    "center": raw_data["center"],
                    "yixiang": "1",
                    "currentPage": str(current_page + 1),
                    "user_id": "0",
                    "source": "web"
                }
                headers = {
                    'Content-Type': 'application/json'
                }
                yield scrapy.FormRequest(url=post_url, callback=self.parse_list, body=json.dumps(params), method="POST",
                                         headers=headers, meta=raw_data.copy())
        # housing_list = response.xpath("//div[@class='list_container']/ul/li")
        # if housing_list:
        #     for housing in housing_list:
        #         data["housing_name"] = housing.xpath(".//span[@class='item_title']/text()").extract_first()
        #         data["housing_url"] = response.urljoin(housing.xpath(".//div[@class='listbox_title']/a/@href").extract_first())
        #         data["district"] = housing.xpath(".//span[@class='site_text']/a")[0].xpath("./text()").extract_first()
        #         data["street"] = housing.xpath(".//span[@class='site_text']/a")[1].xpath("./text()").extract_first()
        #         data["housing_address"] = housing.xpath(".//span[@class='site_text']/text()").extract()[2].strip("】 ")
        #         data["housing_price"] = housing.xpath(".//span[@class='price']/text()").extract_first() + housing.xpath(".//span[@class='price_unit']/text()").extract_first()
        #         yield scrapy.Request(url=data['housing_url'], callback=self.parse_detail, meta=data.copy())

    def parse_detail(self, response):
        items = XiezilouItem()
        data = response.meta.copy()
        housing_price1 = response.xpath("//div[@class='rentBox']/span[@class='rentNum num']/text()").extract_first() + response.xpath("//div[@class='rentBox']/span[@class='rentUnit']/text()").extract_first()
        housing_price_test = response.xpath("//div[@class='saleBox m-l-30']/span[@class='noSalePrice fl']").extract()
        if not housing_price_test:
            housing_price2 = response.xpath("//div[@class='saleBox m-l-30']/span[@class='saleNum num']/text()").extract_first() + response.xpath("//div[@class='saleBox m-l-30']/span[@class='saleUnit']/text()").extract_first()
        else:
            housing_price2 = "暂无售价"
        housing_source = response.xpath("//div[@class='fangyuanBox box']/span[@class='fysl']/text()").extract_first()
        housing_area = response.xpath("//div[@class='xsmjBox box']/span[@class='fysl']/text()").extract_first()
        built_in = response.xpath("//div[@class='basicMessage container clearfix']/div[@class='messageContent']/span[@class='yearsText text firstRow']/text()").extract_first()
        green_rate = response.xpath("//div[@class='basicMessage container clearfix']/div[@class='messageContent']/span[@class='greeningText text']/text()").extract_first()
        building_info = response.xpath("//div[@class='buildMessage container clearfix']")
        building_area = response.xpath("//span[@class='zongmianjiNum text firstRow']/text()").extract_first()
        building_arch = response.xpath("//span[@class='jiegouText text']/text()").extract_first()
        building_total = response.xpath("//span[@class='blocks text']/text()").extract_first()
        elevator_num = response.xpath("//span[@class='ketiNum text lastRow']/text()").extract_first()
        lift_num = response.xpath("//span[@class='huotiNum text firstRow']/text()").extract_first()
        layer_height = response.xpath("//span[@class='cenggaoText text']/text()").extract_first()
        property_level = response.xpath("//span[@class='dengjiText text']/text()").extract_first()
        property_company = response.xpath("//span[@class='wygs text firstRow']/text()").extract_first()
        property_fee = response.xpath("//span[@class='wyf text']/text()").extract_first()
        parking_place = response.xpath("//span[@class='carport text']/text()").extract_first()
        heat_supply = response.xpath("//span[@class='cnfs text lastRow']/text()").extract_first()
        air_condition = response.xpath("//span[@class='ktlx text firstRow']/text()").extract_first()
        power_voltage = response.xpath("//span[@class='dianyaText text']/text()").extract_first()
        items["city"] = data["city"]
        items["district"] = data["district"]
        items["street"] = data["street"]
        items["housing_name"] = data["housing_name"]
        items["housing_url"] = data["housing_url"]
        items["building_address"] = data["housing_address"]
        items["housing_price1"] = housing_price1
        items["housing_price2"] = housing_price2
        items["housing_source"] = housing_source
        items["housing_area"] = housing_area
        items["built_in"] = built_in
        items["green_rate"] = green_rate
        items["building_area"] = building_area
        items["building_arch"] = building_arch
        items["building_total"] = building_total
        items["elevator_num"] = elevator_num
        items["lift_num"] = lift_num
        items["layer_height"] = layer_height
        items["property_company"] = property_company
        items["property_fee"] = property_fee
        items["parking_place"] = parking_place
        items["heat_supply"] = heat_supply
        items["air_condition"] = air_condition
        items["power_voltage"] = power_voltage
        items["property_level"] = property_level
        if not self.redis.sismember(KongjianjiaSpider.name + "_xzl_set", data["housing_url"]):
            yield items
