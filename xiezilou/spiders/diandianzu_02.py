# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Diandianzu02Spider(scrapy.Spider):
    name = 'diandianzu_02'
    allowed_domains = ['diandianzu.com']
    # start_urls = ['http://diandianzu.com/']

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
        base_key = re.search("(.+)_\d+", Diandianzu02Spider.name).group(1)
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
        xzl_type = data["xzl_type"]
        items["publish_time"] = response.xpath("//span[@class='ddz-timestamp']/text()").extract_first().strip()
        items["housing_name"] = response.xpath("//h1[@class='fl']/text()").extract_first().strip()
        housing_price = response.xpath("//div[@class='top-price fr']")[0]
        housing_price_num = housing_price.xpath("./span[@class='price-num']/text()").extract_first()
        housing_price_unit = housing_price.xpath("./text()").extract_first().strip()
        items["housing_price1"] = housing_price_num + housing_price_unit
        housing_rent = response.xpath("//div[@class='fbody']/div/a")
        housing_rentings = []
        if xzl_type == "写字楼":
            for housing in housing_rent:
                area = housing.xpath("./div[@class='tj-pc-listingDetail-house-click f-area f-item']")
                area_num = area.xpath("./font/text()").extract_first()
                area_unit = area.xpath("./span/text()").extract_first()
                if area_unit:
                    hr_area = area_num + area_unit
                else:
                    hr_area = area_num
                price = housing.xpath("./div[@class='tj-pc-listingDetail-house-click f-price f-item']/div[@class='tj-pc-listingDetail-house-click unit-show']")
                hr_price = price.xpath("./span[@class='tj-pc-listingDetail-house-click price-num']/text()").extract_first() + price.xpath("./span[@class='price-unit']/text()").extract_first()
                hr_floor = housing.xpath("./div[@class='tj-pc-listingDetail-house-click f-floor f-item']/text()").extract_first()
                hr_decor = housing.xpath("./div[@class='tj-pc-listingDetail-house-click f-decoraion f-item']/text()").extract_first()
                hr_update = housing.xpath("./div[@class='tj-pc-listingDetail-house-click f-update f-item ddz-timestamp']/text()").extract_first()
                housing_rentings.append("面积： {hr_area}， 单价： {hr_price}， 楼层： {hr_floor}， 装修： {hr_decor}， 更新： {hr_update}".format(
                    hr_area=hr_area,
                    hr_price=hr_price,
                    hr_floor=hr_floor,
                    hr_decor=hr_decor,
                    hr_update=hr_update
                ))
            housing_base_info = response.xpath("//div[@class='clearfix donetime-address']/ul/li")
            for item in housing_base_info:
                label_name = item.xpath("./span[@class='f-title']/text()").extract_first()
                label_value = item.xpath("./span[@class='f-con']/text()").extract_first()
                if label_name == "地理位置":
                    label_value = item.xpath("./span[@class='f-con']/a/text()").extract_first()
                    items["building_address"]  = label_value
                if label_name == "竣工时间":
                    items["built_in"] = label_value
            housing_infos = response.xpath("//div[@class='clearfix ul-layer']")
            for entry in housing_infos:
                lis = entry.xpath("./ul/li")
                for item in lis:
                    label_name = item.xpath("./span[@class='f-title']/text()").extract_first()
                    label_value = item.xpath("./span[@class='f-con']/text()").extract_first()
                    if label_name == "层高":
                        items["layer_height"] = label_value
                    if label_name == "层数":
                        items["building_height"] = label_value
                    if label_name == "物业":
                        items["property"] = label_value
                    if label_name == "物业费":
                        items["property_fee"] = label_value
                    if label_name == "车位":
                        items["parking_place"] = label_value
                    if label_name == "车位月租金":
                        items["parking_fee"] = label_value
                    if label_name == "空调":
                        items["air_condition"] = label_value
                    if label_name == "空调费":
                        items["air_condition_fee"] = label_value
                    if label_name == "空调开放时长":
                        items["air_condition_time"] = label_value
                    if label_name == "电梯":
                        items["elevator"] = label_value
                    if label_name == "网络":
                        items["network"] = label_value
                    if label_name == "入驻企业":
                        items['settled_enterprise'] = label_value
        else:
            pass
        items["city"] = data["city"]
        items["district"] = data["district"]
        items["street"] = data["street"]
        items["xzl_type"] = data["xzl_type"]
        items["housing_url"] = data["housing_url"]
        zs_key = data["zs_key"]
        if self.redis.zscore(zs_key, data['housing_url']) == 2:
            yield items

