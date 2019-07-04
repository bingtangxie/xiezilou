# -*- coding: utf-8 -*-
import scrapy
import redis
import pymongo
import json
import re
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class Tongcheng5802Spider(scrapy.Spider):
    name = 'tongcheng58_02'
    allowed_domains = ['58.com']
    # start_urls = ['http://58.com/']

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
        base_key = re.search("(.+)_\d+", Tongcheng5802Spider.name).group(1)
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
        house_update = response.xpath("//p[@class='house-update-info']/span[@class='up']")
        if house_update:
            items['publish_time'] = re.search(".+(2\d+-\d+-\d+)", house_update[0].xpath("./text()").extract_first()).group(1)
        general_info1 = response.xpath("//ul[@class='general-item-left']/li")
        general_info2 = response.xpath("//ul[@class='general-item-right']/li")
        general_info = general_info1 + general_info2
        flag = data['flag']
        for item in general_info:
            label_name = item.xpath("./span[@class='mr_25 c_999']/text()").extract_first().strip("：")
            lable_value = item.xpath("./span[@class='c_000']/text()").extract_first().strip()
            # if flag == "出租":
            #     if label_name == "写字楼租金":
            #         items['housing_price1'] = lable_value
            # if flag == "出售":
            #     if label_name == "写字楼售价":
            #         items['housing_price1'] = lable_value
            if label_name == "建筑面积":
                items['housing_area'] = lable_value
            if label_name == "可注册公司":
                items['corp_reged'] = lable_value
            if label_name == "起租期":
                items['rent_lease'] = lable_value
            if label_name == "物业费":
                items['property_fee'] = lable_value
            if label_name == "所在楼层":
                items['housing_floor'] = lable_value
            if label_name == "装修情况":
                items['housing_decor'] = lable_value
            if label_name == "使用率":
                items['housing_use_rate'] = lable_value
            if label_name == "付款方式":
                items['pay_method'] = lable_value
            if label_name == "参考容纳工位数":
                items['housing_workplace'] = lable_value
        suites = response.xpath("//li[@class='peitao-on']")
        peitao = []
        for item in suites:
            label = item.xpath("./text()").extract_first()
            peitao.append(label)
            if label == "中央空调":
                items['central_air_condition'] = "有"
        items['peitao'] = ",".join(peitao)
        if response.xpath("//span[@class=' house_basic_title_money_mianyi ']"):
            items['housing_price2'] = "面议"
            items['housing_price1'] = "面议"
        else:
            if response.xpath("//span[@class='house_basic_title_money_num']"):
                items['housing_price1'] = response.xpath(
                    "//span[@class='house_basic_title_money_num']/text()").extract_first() + response.xpath(
                    "//span[@class='house_basic_title_money_unit']/text()").extract_first()
            if response.xpath("//span[@class='house_basic_title_money_num_chushou']"):
                items['housing_price2'] = response.xpath("//span[@class='house_basic_title_money_num_chushou']/text()").extract_first()
            elif response.xpath("//span[@class=' house_basic_title_money_num_chuzu ']"):
                items['housing_price2'] = response.xpath("//span[@class=' house_basic_title_money_num_chuzu ']/text()").extract_first() + response.xpath("//span[@class='house_basic_title_money_unit_chuzu']/text()").extract_first()
        housing_info1 = response.xpath("//div[@class='house-basic-item2']/p")
        housing_info2 = response.xpath("//ul[@class='house-basic-item3']/li")
        for item in housing_info2:
            item_name = item.xpath("./span[@class='c_999']/text()").extract_first().strip("：")
            if item_name == "楼盘":
                items['loupan'] = item.xpath("./span[@class='c_000 mr_10']/span[@class='c_000']/text()").extract_first().strip()
            if item_name == "详细地址":
                address = []
                if item.xpath("./span[@class='c_000 mr_10']"):
                    for sec in item.xpath("./span[@class='c_000 mr_10']/a"):
                        address.append(sec.xpath("./text()").extract_first().strip())
                    address.append(item.xpath("./span[@class='c_000 mr_10']/span/text()").extract_first().strip())
                    if address:
                        items['building_address'] = " ".join(address)
            # if item_name == "可注册公司":
            #     items[''] = item.xpath("./span[@class='c_000 fou']").extract_first().strip()
        if data['xzl_type'] == "纯写字楼":
            items['property_level'] = response.xpath("//div[@class='house-basic-item2']/p[@class='item3']/span[@class='sub']/text()").extract_first()
        items['agent'] = response.xpath("//div[@class='jjr-name f14 c_555']/a[@class='c_000 jjr-name-txt']/text()").extract_first()
        items['agent_phone'] = response.xpath("//p[@class='phone-num']/text()").extract_first()
        agent_company_raw = response.xpath("//p[@class='jr-item jjr-belong']")
        if agent_company_raw:
            items['agent_company'] = agent_company_raw.xpath("./span[@class='c_000']/text()").extract_first()
        items['province'] = data['province']
        items['city'] = data['city']
        items['district'] = data['district']
        items['street'] = data['street']
        items['flag'] = data['flag']
        items['xzl_type'] = data['xzl_type']
        items['housing_url'] = data['housing_url']
        zs_key = data['zs_key']
        if self.redis.zscore(zs_key, data['housing_url']) == 2:
            yield items

