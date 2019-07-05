# -*- coding: utf-8 -*-
import scrapy
import redis
import json
import re
from scrapy.conf import settings


class Diandianzu01Spider(scrapy.Spider):
    name = 'diandianzu_01'
    allowed_domains = ['diandianzu.com']
    # start_urls = ['http://diandianzu.com/']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    def start_requests(self):
        uris = {
            "bj": "北京",
            "su": "苏州",
            "nj": "南京",
            "nb": "宁波",
            "hf": "合肥",
            "cd": "成都",
            "xa": "西安",
            "hz": "杭州",
            "sz": "深圳",
            "gz": "广州",
            "sh": "上海"
        }
        for uri in uris:
            url = "https://{uri}.diandianzu.com/listing".format(uri=uri)
            city = uris[uri]
            if city == "北京":
                yield scrapy.Request(url=url, callback=self.parse, meta={"city": city})

    def parse(self, response):
        data = response.meta.copy()
        navs = response.xpath("//div[@class='nav']/a")
        for nav in navs:
            label = nav.xpath("./text()").extract_first().strip()
            label_url = response.urljoin(nav.xpath("./@href").extract_first())
            label_style = nav.xpath("./@class").extract_first()
            if re.search("current", label_style):
                data['xzl_type'] = label
                districts = response.xpath("//div[@class='level2 item-level item-region item-level2-0 ']/a")
                for district in districts:
                    district_name = district.xpath("./text()").extract_first().strip()
                    district_url = response.urljoin(district.xpath("./@href").extract_first())
                    data['district'] = district_name
                    data['district_id'] = district.xpath("./@data-id").extract_first()
                    if district_name != "全市区域":
                        yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())
            else:
                data['xzl_type'] = label
                yield scrapy.Request(url=label_url, callback=self.parse_district, meta=data.copy())

    def parse_district(self, response):
        data = response.meta.copy()
        districts = response.xpath("//div[@class='level2 item-level item-region item-level2-0 ']/a")
        for district in districts:
            district_name = district.xpath("./text()").extract_first().strip()
            district_url = response.urljoin(district.xpath("./@href").extract_first())
            data['district'] = district_name
            data['district_id'] = district.xpath("./@data-id").extract_first()
            if district_name != "全市区域":
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())

    def parse_street(self, response):
        data = response.meta.copy()
        district_id = data['district_id']
        no_result = response.xpath("//div[@class='empty-recommend clearfix']")
        if not no_result:
            search_str = "'level3 item-level item-region sub-region sub-region{district_id} level3-show'".format(district_id=district_id)
            streets = response.xpath("//div[@class=" + search_str + "]/a")
            for street in streets:
                street_name = street.xpath("./text()").extract_first().strip()
                street_url = response.urljoin(street.xpath("./@href").extract_first())
                data['street'] = street_name
                yield scrapy.Request(url=street_url, callback=self.parse_list, meta=data.copy())

    def parse_list(self, response):
        data = response.meta.copy()
        no_result = response.xpath("//div[@class='empty-recommend clearfix']")
        if not no_result:
            housing_list = response.xpath("//a[@class='tj-pc-listingList-title-click']")
            for housing in housing_list:
                housing_url = response.urljoin(housing.xpath("./@href").extract_first())
                data['housing_url'] = housing_url
                data['referer'] = response.url
                base_key = re.search("(.+)_\d+", Diandianzu01Spider.name).group(1)
                hash_key = base_key + "_xzl_detail_url_hashtable"
                set_key = base_key + "_xzl_zset"
                self.redis.hset(hash_key, housing_url, json.dumps(data))
                self.redis.zadd(set_key, {housing_url: 1})
            pagination = response.xpath("//a[class='next tj-pc-listingList-page-click']")
            if pagination:
                next_page_url = pagination.xpath("./@href").extract_first()
                yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())

