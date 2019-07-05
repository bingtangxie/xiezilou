# -*- coding: utf-8 -*-
import scrapy
import redis
import re
import json
from scrapy.conf import settings


class Haozu01Spider(scrapy.Spider):
    name = 'haozu_01'
    allowed_domains = ['haozu.com']
    # start_urls = ['http://haozu.com/']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    def start_requests(self):
        uris = {
            'bj': '北京',
            'sh': '上海',
            'gz': '广州',
            'sz': '深圳',
            'cd': '成都',
            'wh': '武汉',
            'hz': '杭州',
            'xa': '西安',
            'tj': '天津',
            'nj': '南京',
            'cq': '重庆',
            'cs': '长沙',
            'dl': '大连',
            'fz': '福州',
            'sy': '沈阳',
            'zz': '郑州',
            'wx': '无锡',
            'hf': '合肥',
            'su': '苏州',
            'km': '昆明',
            'sjz': '石家庄',
            'jian': '吉安',
            'nb': '宁波',
            'jn': '济南'
        }
        for uri in uris:
            url = "https://www.haozu.com/{uri}".format(uri=uri)
            city = uris[uri]
            yield scrapy.Request(url=url, callback=self.parse, meta={"city": city})

    def parse(self, response):
        data = response.meta.copy()
        navs = response.xpath("//ul[@class='headNav-list']/li")[0].xpath("./a")
        share = response.xpath("//ul[@class='headNav-list']/li")[1].xpath("./a/text()").extract_first()
        if share == "共享办公":
            data['share'] = 'y'
        else:
            data['share'] = 'n'
        for nav in navs:
            label = nav.xpath("./text()").extract_first().strip()
            if label == "写字楼出租":
                label_url = response.urljoin(nav.xpath("./@href").extract_first())
                data['flag'] = label
                yield scrapy.Request(url=label_url, callback=self.parse_type, meta=data.copy())
            if label == "·出售":
                label_url = response.urljoin(nav.xpath("./@href").extract_first())
                data['flag'] = "写字楼出售"
                yield scrapy.Request(url=label_url, callback=self.parse_district, meta=data.copy())

    def parse_type(self, response):
        data = response.meta.copy()
        if data['flag'] == "写字楼出租":
            secs = response.xpath("//div[@class='filter']/div[@class='warp']/dl[@class='clearfix']")
            for sec in secs:
                sec_label = sec.xpath("./dt/text()").extract_first().strip("：")
                if sec_label == "类型":
                    types = sec.xpath("./dd/div[@class='clearfix']/a")
                    for item in types:
                        item_name = item.xpath("./text()").extract_first().strip()
                        item_uri = item.xpath("./@href").extract_first()
                        url = response.urljoin(item_uri)
                        data['xzl_type'] = item_name
                        yield scrapy.Request(url=url, callback=self.parse_district, meta=data.copy())

    def parse_district(self, response):
        data = response.meta.copy()
        flag = data['flag']
        if 'xzl_type' in data:
            xzl_type = data['xzl_type']
        if 'share' in data:
            share = data['share']
        if flag == "写字楼出租":
            if xzl_type == "共享办公" and share == "y":
                districts = response.xpath("//div[@class='filter']/div[@class='warp']/dl[@class='clearfix']")[0].xpath("./dd/div[@class='area clearfix']/a")
                for district in districts:
                    district_name = district.xpath("./text()").extract_first().strip()
                    district_url = response.urljoin(district.xpath("./@href").extract_first())
                    if district_name == "全部":
                        pass
                    else:
                        data['district'] = district_name
                        yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())
            else:
                districts = response.xpath("//div[@class='secsel menu_area']/div[@class='area clearfix']/a")
                for district in districts:
                    district_name = district.xpath("./text()").extract_first().strip()
                    district_url = response.urljoin(district.xpath("./@href").extract_first())
                    if district_name == "全部":
                        pass
                    else:
                        pass
                        data['district'] = district_name
                        yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())

        if flag == "写字楼出售":
            districts = response.xpath("//div[@class='mapFind-seltion-cont']/dl[@class='clearfix']")[0].xpath("./dd/div[@class='area clearfix']/a")
            for district in districts:
                district_name = district.xpath("./text()").extract_first()
                district_url = response.urljoin(district.xpath("./@href").extract_first())
                if district_name == "全部":
                    pass
                else:
                    data['district'] = district_name
                    yield scrapy.Request(url=district_url, callback=self.parse_street, meta=data.copy())

    def parse_street(self, response):
        data = response.meta.copy()
        flag = data['flag']
        if 'xzl_type' in data:
            xzl_type = data['xzl_type']
        if 'share' in data:
            share = data['share']
        if flag == "写字楼出租":
            if xzl_type == "共享办公" and share == "y":
                no_result = response.xpath("//p[@class='result-p1']")
                if not no_result:
                    streets = response.xpath("//div[@class='subarea clearfix']/a")
                    for street in streets:
                        street_name = street.xpath("./text()").extract_first().strip()
                        street_url = response.urljoin(street.xpath("./@href").extract_first())
                        if street_name == "全部":
                            pass
                        else:
                            data['street'] = street_name
                            yield scrapy.Request(url=street_url, callback=self.parse_list, meta=data.copy())
            else:
                no_result = response.xpath("//p[@class='result-p1']")
                if not no_result:
                    streets = response.xpath("//div[@class='subarea clearfix']/a")
                    for street in streets:
                        street_name = street.xpath("./text()").extract_first().strip()
                        street_url = response.urljoin(street.xpath("./@href").extract_first())
                        if street_name == "全部":
                            pass
                        else:
                            data['street'] = street_name
                            yield scrapy.Request(url=street_url, callback=self.parse_list, meta=data.copy())
        if flag == "写字楼出售":
            no_result = response.xpath("//p[@class='result-p1']")
            if not no_result:
                streets = response.xpath("//div[@class='tabcontent subarea clearfix']/div/a")
                for street in streets:
                    street_name = street.xpath("./text()").extract_first().strip()
                    street_url = response.urljoin(street.xpath("./@href").extract_first())
                    if street_name == "全部":
                        pass
                    else:
                        data['street'] = street_name
                        yield scrapy.Request(url=street_url, callback=self.parse_list, meta=data.copy())

    def parse_list(self, response):
        data = response.meta.copy()
        flag = data['flag']
        if 'xzl_type' in data:
            xzl_type = data['xzl_type']
        if 'share' in data:
            share = data['share']
        if flag == "写字楼出租":
            if xzl_type == "共享办公" and share == "y":
                no_result = response.xpath("//p[@class='result-p1']")
                if not no_result:
                    housing_list = response.xpath("//div[@class='list-content']")
                    for item in housing_list:
                        housing_url = "https:" + item.xpath("./h1/a/@href").extract_first()
                        data['housing_url'] = housing_url
                        data['referer'] = response.url
                        base_key = re.search("(.+)_\d+", Haozu01Spider.name).group(1)
                        hash_key = base_key + "_xzl_detail_url_hashtable"
                        set_key = base_key + "_xzl_zset"
                        self.redis.hset(hash_key, housing_url, json.dumps(data))
                        self.redis.zadd(set_key, {housing_url: 1})
                    pagination = response.xpath("//a[@class='next']")
                    if pagination:
                        next_page_url = response.urljoin(pagination.xpath("./@href").extract_first())
                        yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())
            else:
                no_result = response.xpath("//p[@class='result-p1']")
                if not no_result:
                    housing_list = response.xpath("//div[@class='list-content fl']")
                    for item in housing_list:
                        housing_url = item.xpath("./h1/a/@href").extract_first()
                        data['housing_url'] = housing_url
                        data['referer'] = response.url
                        base_key = re.search("(.+)_\d+", Haozu01Spider.name).group(1)
                        hash_key = base_key + "_xzl_detail_url_hashtable"
                        set_key = base_key + "_xzl_zset"
                        self.redis.hset(hash_key, housing_url, json.dumps(data))
                        self.redis.zadd(set_key, {housing_url: 1})
                    pagination = response.xpath("//a[@class='next']")
                    if pagination:
                        next_page_url = response.urljoin(pagination.xpath("./@href").extract_first())
                        yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())

        if flag == "写字楼出售":
            if flag == "写字楼出售":
                no_result = response.xpath("//p[@class='result-p1']")
                if not no_result:
                    housing_list = response.xpath("//div[@class='mapFind-list-title clearfix']")
                    for housing in housing_list:
                        housing_url = response.urljoin(housing.xpath("./h1/a/@href").extract_first())
                        data['housing_url'] = housing_url
                        data['referer'] = response.url
                        base_key = re.search("(.+)_\d+", Haozu01Spider.name).group(1)
                        hash_key = base_key + "_xzl_detail_url_hashtable"
                        set_key = base_key + "_xzl_zset"
                        self.redis.hset(hash_key, housing_url, json.dumps(data))
                        self.redis.zadd(set_key, {housing_url: 1})
                    pagination = response.xpath("//a[@class='next']")
                    if pagination:
                        next_page_url = response.urljoin(pagination.xpath("./@href").extract_first())
                        yield scrapy.Request(url=next_page_url, callback=self.parse_list, meta=data.copy())

