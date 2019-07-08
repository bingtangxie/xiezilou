# -*- coding: utf-8 -*-
import scrapy
import redis
import json
import re
from scrapy.conf import settings


class Fangtianxia01Spider(scrapy.Spider):
    name = 'fangtianxia_01'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

    def __init__(self):
        super().__init__()
        redis_host = settings['REDIS_HOST']
        redis_port = settings['REDIS_PORT']
        redis_db = settings['REDIS_DB']
        redis_password = settings['REDIS_PASS']
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    def parse(self, response):
        content = response.xpath("//table[@id='senfe']/tr")
        province = None
        entrance_list = []
        for item in content:
            ss = "".join(item.xpath("./td")[1].xpath("./strong/text()").extract())
            cities = item.xpath("./td")[2].xpath("./a")
            if ss:
                if ss == "直辖市":
                    province = ""
                elif ss == '\xa0':
                    pass
                else:
                    province = ss
            else:
                pass
            for city in cities:
                city_name = city.xpath("./text()").extract()[0]
                city_url = city.xpath("./@href").extract()[0]
                if province != "其它":
                    if city_name in ["北京", "上海", "重庆", "天津"]:
                        province = city_name
                    entrance_list.append({"province": province, "city": city_name, "city_url": city_url})
        entrance_list_length = len(entrance_list)
        # for i in range(entrance_list_length):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     if city == "北京":
        #         yield scrapy.Request(url=url, callback=self.parse_city,
        #                              meta={'province': province, 'city': city})

        # for i in range(int(entrance_list_length * 0.25)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     if city != "北京":
        #         yield scrapy.Request(url=url, callback=self.parse_city,
        #                              meta={'province': province, 'city': city})

        # for i in range(int(entrance_list_length * 0.25), int(entrance_list_length * 0.5)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     if city != "北京":
        #         yield scrapy.Request(url=url, callback=self.parse_city,
        #                              meta={'province': province, 'city': city})

        # for i in range(int(entrance_list_length * 0.5), int(entrance_list_length * 0.75)):
        #     url = entrance_list[i]['city_url']
        #     province = entrance_list[i]['province']
        #     city = entrance_list[i]['city']
        #     yield scrapy.Request(url=url, callback=self.parse_city,
        #                          meta={'province': province, 'city': city})

        for i in range(int(entrance_list_length * 0.75), entrance_list_length):
            url = entrance_list[i]['city_url']
            province = entrance_list[i]['province']
            city = entrance_list[i]['city']
            yield scrapy.Request(url=url, callback=self.parse_city,
                                 meta={'province': province, 'city': city})

    def parse_city(self, response):
        province = response.meta['province']
        city = response.meta['city']
        xzl = response.xpath("//div[@track-id='office']/div[@class='listBox']/ul/li")
        for flag in xzl:
            flag_name = flag.xpath("./a/text()").extract_first().strip()
            flag_url = flag.xpath("./a/@href").extract_first()
            if flag_name == "写字楼出售" or "写字楼出租":
                yield scrapy.Request(url=flag_url, callback=self.parse_district,
                                     meta={'province': province, 'city': city, 'flag': flag_name})

        # 获取方式有问题, 例如： https://zhaozhou.fang.com/

        # chuzu = response.xpath("//a[@id='dsy_D01_43']")
        # chushou = response.xpath("//a[@id='dsy_D01_44']")
        # if chuzu:
        #     flag = chuzu.xpath("./text()").extract_first().strip()
        #     chuzu_url = chuzu.xpath("./@href").extract_first()
        #     yield scrapy.Request(url=chuzu_url, callback=self.parse_district, meta={'province': province, 'city': city, 'flag': flag})
        # if chushou:
        #     flag = chushou.xpath("./text()").extract_first().strip()
        #     chushou_url = chushou.xpath("./@href").extract_first()
        #     yield scrapy.Request(url=chushou_url, callback=self.parse_district, meta={'province': province, 'city': city, 'flag': flag})

    def parse_district(self, response):
        province = response.meta['province']
        city = response.meta['city']
        flag = response.meta['flag']
        districts = response.xpath("//div[@class='qxName']/a")
        list_none = response.xpath("//div[@class='list-none mt10']")
        if not list_none:
            for district in districts:
                district_name = district.xpath("./text()").extract_first().strip()
                district_url = response.urljoin(district.xpath("./@href").extract_first())
                if district_name == "不限":
                    pass
                else:
                    yield scrapy.Request(url=district_url, callback=self.parse_street,
                                         meta={'province': province, 'city': city, 'district': district_name,
                                               'flag': flag})

    def parse_street(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        flag = response.meta['flag']
        streets = response.xpath("//p[@class='contain']/a")
        list_none = response.xpath("//div[@class='list-none mt10']")
        if not list_none:
            for street in streets:
                street_name = street.xpath("./text()").extract_first().strip()
                street_url = response.urljoin(street.xpath("./@href").extract_first())
                if street_name == "不限":
                    pass
                else:
                    yield scrapy.Request(url=street_url, callback=self.parse_type,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street_name, 'flag': flag})

    def parse_type(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        flag = response.meta['flag']
        xzl_types = response.xpath("//ul[@class='info ml25']/li")[2].xpath("./a")
        list_none = response.xpath("//div[@class='list-none mt10']")
        if not list_none:
            for item in xzl_types:
                xzl_type = item.xpath("./text()").extract_first().strip()
                xzl_type_url = response.urljoin(item.xpath("./@href").extract_first())
                if xzl_type == "纯写字楼":
                    yield scrapy.Request(url=xzl_type_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'flag': flag, 'xzl_type': xzl_type})
                if xzl_type == "商业综合体楼":
                    yield scrapy.Request(url=xzl_type_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street,
                                               'flag': flag, 'xzl_type': xzl_type})
                if xzl_type == "酒店写字楼":
                    yield scrapy.Request(url=xzl_type_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street,
                                               'flag': flag, 'xzl_type': xzl_type})

    def parse_list(self, response):
        province = response.meta['province']
        city = response.meta['city']
        district = response.meta['district']
        street = response.meta['street']
        flag = response.meta['flag']
        xzl_type = response.meta['xzl_type']
        list_none = response.xpath("//div[@class='list-none mt10']")
        if not list_none:
            housing_list = response.xpath("//div[@class='houseList']/dl")
            if housing_list:
                for housing in housing_list:
                    housing_url = response.urljoin(
                        housing.xpath("./dd[@class='info rel floatr']/p[@class='title']/a/@href").extract_first())
                    data = {'province': province, 'city': city, 'district': district, 'street': street,
                            'xzl_type': xzl_type, 'referer': response.url, 'housing_url': housing_url, 'flag': flag}
                    base_key = re.search("(.+)_\d+", Fangtianxia01Spider.name).group(1)
                    hash_key = base_key + "_xzl_detail_url_hashtable"
                    set_key = base_key + "_xzl_zset"
                    self.redis.hset(hash_key, housing_url, json.dumps(data))
                    self.redis.zadd(set_key, {housing_url: 1})
                pagination = response.xpath("//a[@id='PageControl1_hlk_next']")
                if pagination:
                    next_url = response.urljoin(pagination.xpath("./@href").extract_first())
                    yield scrapy.Request(url=next_url, callback=self.parse_list,
                                         meta={'province': province, 'city': city, 'district': district,
                                               'street': street, 'flag': flag, 'xzl_type': xzl_type})
