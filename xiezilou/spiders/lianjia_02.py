# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Lianjia02Spider(scrapy.Spider):
    name = 'lianjia_02'
    allowed_domains = ['lianjia.com']
    # start_urls = ['http://lianjia.com/']

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
        base_key = re.search("(.+)_\d+", Lianjia02Spider.name).group(1)
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
                yield scrapy.Request(url=url, callback=self.parse_detail, meta=data)
            total = len(urls)
            if total == 0:
                break

    def parse_detail(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        flag = response.meta['flag']
        building_url = response.meta['building_url']
        building_name = response.meta['building_name']
        area_extent = response.meta['area_extent']
        building_description = response.meta['building_description']
        price_extent = response.meta['price_extent']
        buiding_agent = response.xpath("//span[@class='detail__agent-name']/text()").extract_first().strip()
        buiding_agent_phone = response.xpath("//p[@class='detail__agent-phone']/text()").extract_first().strip()
        building_info = response.xpath("//div[@class='detail__info slist__info']/p")
        h_key = response.meta['h_key']
        for item in building_info:
            label = item.xpath("./span/text()").extract_first().strip("：")
            label_value = item.xpath("./text()").extract_first().strip()
            if label == "位置":
                building_address = label_value
            if label == "楼层":
                building_height = label_value
            if label == "客梯":
                building_elevator = label_value
            if label == "商圈":
                business_circle = label_value
            if label == "开发商":
                developer = label_value
        housing_list = response.xpath("//div[@class='result__ul']/a")
        for housing in housing_list:
            housing_name = housing.xpath("./div[@class='result__li-right']/p[@class='result__li-title']/text()").extract_first().strip()
            housing_description = housing.xpath("./div[@class='result__li-right']/p[@class='result__li-features']/text()").extract_first().strip()
            housing_url = response.urljoin(housing.xpath("./@href").extract_first())
            data = {
                "province": province,
                "city": city,
                "district": district,
                "street": street,
                "flag": flag,
                "building_url": building_url,
                "building_name": building_name,
                "area_extent": area_extent,
                "building_description": building_description,
                "price_extent": price_extent,
                "buiding_agent": buiding_agent,
                "buiding_agent_phone": buiding_agent_phone,
                "building_address": building_address or "",
                "building_height": building_height or "",
                "building_elevator": building_elevator or "",
                "business_circle": business_circle or "",
                "developer": developer or "",
                "housing_name": housing_name,
                "housing_description": housing_description,
                "housing_url": housing_url,
                "h_key": h_key
            }
            yield scrapy.Request(url=housing_url, callback=self.parse_detail_info, meta=data)

    def parse_detail_info(self, response):
        items = XiezilouItem()
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        flag = response.meta['flag']
        building_url = response.meta['building_url']
        building_name = response.meta['building_name']
        area_extent = response.meta['area_extent']
        building_description = response.meta['building_description']
        price_extent = response.meta['price_extent']
        buiding_agent = response.meta['buiding_agent']
        buiding_agent_phone = response.meta['buiding_agent_phone']
        building_address = response.meta['building_address']
        building_height = response.meta['building_height']
        building_elevator = response.meta['building_elevator']
        business_circle = response.meta['business_circle']
        developer = response.meta['developer']
        housing_name = response.meta['housing_name']
        housing_description = response.meta['housing_description']
        housing_url = response.meta['housing_url']
        h_key = response.meta['h_key']
        traffic = ""
        zb_suite = ""
        other = ""
        housing_area = ""
        housing_floor = ""
        housing_workplace = ""
        publish_time = ""
        housing_features = response.xpath("//div[@class='detail__feature-ul cf']/p[@class='detail__feature-text']")
        if housing_features:
            for feature in housing_features:
                label_name = feature.xpath("./strong/text()").extract_first().strip("：")
                label_value = feature.xpath("./text()").extract_first().strip()
                if label_name == "交通出行":
                    traffic = label_value
                if label_name == "周边配套":
                    zb_suite = label_value
                if label_name == "其他":
                    other = label_value
        housing_price = response.xpath("//p[@class='detail__price']")
        housing_price1 = housing_price.xpath("./label/span/text()").extract_first() + housing_price.xpath("./label/text()").extract_first()
        housing_price2 = response.xpath("//div[@class='detail__priceunit']/text()").extract_first()
        detail_info = response.xpath("//div[@class='detail__info']/p")
        for item in detail_info:
            label_name = item.xpath("./span/text()").extract_first().strip("：")
            label_value = item.xpath("./text()").extract_first()
            if label_name == "面积":
                housing_area = label_value
            if label_name == "楼层":
                housing_floor = label_value
            if label_name == "工位":
                housing_workplace = label_value
            if label_name == "时间":
                publish_time = label_value
        detail_agent = response.xpath("//div[@class='detail__agent']")
        agents = []
        for agent in detail_agent:
            agent_name = agent.xpath("./div[@class='detail__agent-info']/div[@class='detail__agent-name']/div[@class='detail__agent-top']/span[@class='detail__agent-name']/text()").extract_first()
            agent_phone = agent.xpath("./p[@class='detail__agent-phone']/text()").extract_first()
            agents.append(agent_name + ": " + agent_phone)
        housing_agents_info = ",".join(agents)
        items['province'] = province
        items['city'] = city
        items['district'] = district
        items['street'] = street
        items['flag'] = flag
        items['building_url'] = building_url
        items['building_name'] = building_name
        items['area_extent'] = area_extent
        items['building_description'] = building_description
        items['price_extent'] = price_extent
        items['buiding_agent'] = buiding_agent
        items['buiding_agent_phone'] = buiding_agent_phone
        items['building_address'] = building_address
        items['building_height'] = building_height
        items['building_elevator'] = building_elevator
        items['business_circle'] = business_circle
        items['developer'] = developer
        items['housing_name'] = housing_name
        items['housing_description'] = housing_description
        items['housing_url'] = housing_url
        items['traffic'] = traffic or ""
        items['zb_suite'] = zb_suite or ""
        items['other'] = other or ""
        items['housing_price1'] = housing_price1
        items['housing_price2'] = housing_price2
        items['housing_area'] = housing_area or ""
        items['housing_floor'] = housing_floor or ""
        items['housing_workplace'] = housing_workplace or ""
        items['publish_time'] = publish_time or ""
        items['housing_agents_info'] = housing_agents_info
        if self.redis.hexists(h_key, building_url) and not self.redis.hexists("lianjia_xzl_housing_finished_hashtable", housing_url):
            yield items



