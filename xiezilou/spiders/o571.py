# -*- coding: utf-8 -*-
import scrapy
import re
import redis
import pymongo
from scrapy.conf import settings
from xiezilou.items import XiezilouItem


class O571Spider(scrapy.Spider):
    name = 'o571'
    allowed_domains = ['o571.com']
    # start_urls = ['http://o571.com/lease', 'http://o571.com/sale']

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
        # urls = ['http://o571.com/lease', 'http://o571.com/sale']
        # for url in urls:
        #     yield scrapy.Request(url=url, callback=self.parse)
        url = "http://www.o571.com/lease"
        xflag = "lease"
        yield scrapy.Request(url=url, callback=self.parse, meta={"xflag": xflag})
        url = "http://www.o571.com/sale"
        xflag = "sale"
        yield scrapy.Request(url=url, callback=self.parse, meta={"xflag": xflag})

    def parse(self, response):
        xflag = response.meta["xflag"]
        if xflag == "lease":
            districts = response.xpath("//p[@class='main_nav box1_b']/a")
            for district in districts:
                district_name = district.xpath("./text()").extract_first()
                district_url = response.urljoin(district.xpath("./@href").extract_first())
                if district_name == "城区商圈图":
                    pass
                elif district_name == "写字楼图库":
                    pass
                else:
                    yield scrapy.Request(url=district_url, callback=self.parse_list, meta={"city": "杭州", "district": district_name, "xflag": xflag})
        if xflag == "sale":
            districts = response.xpath("//dl[@class='navsec']/dd/a")
            for district in districts:
                district_name = district.xpath("./text()").extract_first()
                district_url = response.urljoin(district.xpath("./@href").extract_first())
                yield scrapy.Request(url=district_url, callback=self.parse_list, meta={"city": "杭州", "district": district_name, "xflag": xflag})

    def parse_list(self, response):
        data = response.meta.copy()
        xflag = data["xflag"]
        district = data["district"]
        if district == "高新(滨江)区":
            housing_list = response.xpath("//ul[@class='downDashed nowrap clearfix']")
            if housing_list:
                for housing in housing_list:
                    housing_url = housing.xpath("./li[@class='txt']/p[@class='name']/a/@href").extract_first()
                    data["housing_url"] = housing_url
                    for p in housing.xpath("./li[@class='txt']/p"):
                        entry = p.xpath("./text()").extract_first()
                        if re.search("租售价格.+", entry):
                            data["housing_price1"] = entry.split("：")[1].strip()
                    yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta=data.copy())
                paginations = response.xpath("//ul[@class='clearfix']")
                pagination_params = []
                total_page = 0
                for pagination in paginations:
                    params = re.search("showpage\((.+)\);", pagination.xpath("./li[@class='bgcolor1 w tr lh28']/script")[1].xpath("./text()").extract_first()).group(
                        1).split(",")
                    total = int(params[5].strip().strip("'"))
                    if total > total_page:
                        pagination_params = params
                if len(pagination_params) != 0:
                    current_page = pagination_params[6].strip().strip("'")
                    total_page = pagination_params[5].strip().strip("'")
                    key = pagination_params[3].strip().strip("'")
                    ex_param = pagination_params[2].strip().strip("'")
                    param = pagination_params[1].strip().strip("'")
                    if total_page > current_page:
                        next_page = int(current_page) + 1
                        url = "http://www.o571.com/{xflag}/{param}?{key}={next_page}{ex_param}".format(
                            xflag=xflag,
                            param=param,
                            key=key,
                            next_page=next_page,
                            ex_param=ex_param
                        )
                        yield scrapy.Request(url=url, callback=self.parse_list, meta=data.copy())
        else:
            housing_list = response.xpath("//ul[@class='downDashed nowrap clearfix']")
            if housing_list:
                for housing in housing_list:
                    housing_url = housing.xpath("./li[@class='txt']/p[@class='name']/a/@href").extract_first()
                    data["housing_url"] = housing_url
                    for p in housing.xpath("./li[@class='txt']/p"):
                        entry = p.xpath("./text()").extract_first()
                        if re.search("租售价格.+", entry):
                            data["housing_price1"] = entry.split("：")[1].strip()
                    yield scrapy.Request(url=housing_url, callback=self.parse_detail, meta=data.copy())
                pagination = response.xpath("//ul[@class='clearfix']/li[@class='bgcolor1 w tr lh28']/script")
                if pagination:
                    params = re.search("showpage\((.+)\);", pagination[1].xpath("./text()").extract_first()).group(1).split(",")
                    current_page = params[6].strip().strip("'")
                    total_page = params[5].strip().strip("'")
                    key = params[3].strip().strip("'")
                    ex_param = params[2].strip().strip("'")
                    param = params[1].strip().strip("'")
                    if total_page > current_page:
                        next_page = int(current_page) + 1
                        url = "http://www.o571.com/{xflag}/{param}?{key}={next_page}{ex_param}".format(
                            xflag=xflag,
                            param=param,
                            key=key,
                            next_page=next_page,
                            ex_param=ex_param
                        )
                        yield scrapy.Request(url=url, callback=self.parse_list, meta=data.copy())

    def parse_detail(self, response):
        items = XiezilouItem()
        data = response.meta.copy()
        items["housing_price1"] = data["housing_price1"]
        items["publish_time"] = response.xpath("//span[@class='fr pr20']/font[@class='color8']/text()").extract_first()
        publish_type = response.xpath("//span[@class='bis_user fl']/font[@class='color2']/text()").extract_first().strip("[").strip("]")
        if publish_type == "中介":
            items["publish_type"] = publish_type
            items["publisher"] = response.xpath("//span[@class='bis_user fl']/a")[0].xpath("./text()").extract_first()
            base_info = response.xpath("//div[@class='bis_actinfo clearfix']/ul/li")
            for i in range(len(base_info)):
                if i % 2 == 0:
                    label_name = base_info[i].xpath("./text()").extract_first().strip()
                    label_value = base_info[i + 1].xpath("./text()").extract_first()
                    if label_name == "楼盘名称:":
                        items["loupan"] = label_value
                    if label_name == "楼盘类型:":
                        items["xzl_type"] = label_value
                    if label_name == "城区商圈:":
                        items["business_circle"] = label_value
                    if label_name == "楼盘地址:":
                        items["building_address"] = label_value
                    if label_name == "门店经理:":
                        items["agent"] = label_value
                    if label_name == "门店固话:":
                        items["agent_phone"] = label_value
            peitao_info = response.xpath("//ul[@class='bis_table clearfix']/li")
            for i in range(len(peitao_info)):
                if i % 2 == 0:
                    label_name = peitao_info[i].xpath("./text()").extract_first()
                    label_value = peitao_info[i + 1].xpath("./text()").extract_first()
                    if label_name == "物管公司":
                        items["property"] = label_value
                    if label_name == "楼层状况":
                        items["housing_floor"] = label_value
                    if label_name == "总 建 面":
                        items["building_area"] = label_value
                    if label_name == "物 管 费":
                        items["property_fee"] = label_value
                    if label_name == "标准层高":
                        items["layer_height"] = label_value
                    if label_name == "空　　调":
                        items["central_air_condition"] = label_value
                    if label_name == "车位数量":
                        items["parking_place"] = label_value
                    if label_name == "标准层建面":
                        items["housing_area"] = label_value
                    if label_name == "电　　梯":
                        items["elevator"] = label_value
                    if label_name == "车 位 费":
                        items["parking_fee"] = label_value
                    if label_name == "开间建面":
                        items["kaijian_area"] = label_value
                    if label_name == "员工餐厅":
                        items["employee_restaurant"] = label_value
                    if label_name == "交通站点":
                        items["traffic_site"] = label_value
                    if label_name == "轨道公交":
                        items["traffic"] = label_value
        if publish_type == "非中介":
            items["publish_type"] = publish_type
            items["publisher"] = response.xpath("//span[@class='bis_user fl']/text()").extract_first()
            base_info = response.xpath("//div[@class='box1 bis_info bgcolor1 clearfix']/ul/li")
            for i in range(len(base_info)):
                if i % 2 == 0:
                    label_name = base_info[i].xpath("./text()").extract_first().strip()
                    label_value = base_info[i + 1].xpath("./text()").extract_first()
                    if label_name == "楼盘类型:":
                        items["xzl_type"] = label_value
                    if label_name == "所在楼层:":
                        items["housing_floor"] = label_value
                    if label_name == "招商面积:":
                        items["building_area"] = label_value
                    if label_name == "租售价格:":
                        items["housing_price1"] = label_value
                    if label_name == "付款方式:":
                        items["pay_method"] = label_value
                    if label_name == "装修状况:":
                        items["housing_decor"] = label_value
                    if label_name == "基本租期:":
                        items["rent_lease"] = label_value
                    if label_name == "楼盘地址:":
                        items["building_address"] = label_value
            peitao_info = response.xpath("//ul[@class='bis_table clearfix']/li")
            for i in range(len(peitao_info)):
                if i % 2 == 0:
                    label_name = peitao_info[i].xpath("./text()").extract_first()
                    label_value = peitao_info[i + 1].xpath("./text()").extract_first()
                    if label_name == "物管公司":
                        items["property"] = label_value
                    if label_name == "楼层状况":
                        items["housing_floor"] = label_value
                    if label_name == "总 建 面":
                        items["building_area"] = label_value
                    if label_name == "物 管 费":
                        items["property_fee"] = label_value
                    if label_name == "标准层高":
                        items["layer_height"] = label_value
                    if label_name == "空　　调":
                        items["central_air_condition"] = label_value
                    if label_name == "车位数量":
                        items["parking_place"] = label_value
                    if label_name == "标准层建面":
                        items["housing_area"] = label_value
                    if label_name == "电　　梯":
                        items["elevator"] = label_value
                    if label_name == "车 位 费":
                        items["parking_fee"] = label_value
                    if label_name == "开间建面":
                        items["kaijian_area"] = label_value
                    if label_name == "员工餐厅":
                        items["employee_restaurant"] = label_value
                    if label_name == "交通站点":
                        items["traffic_site"] = label_value
                    if label_name == "轨道公交":
                        items["traffic"] = label_value
        items["city"] = data["city"]
        items["district"] = data["district"]
        items["housing_url"] = data["housing_url"]
        if not self.redis.sismember(O571Spider.name + "xzl_set", data["housing_url"]):
            yield items
