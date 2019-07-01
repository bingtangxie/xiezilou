# -*- coding: utf-8 -*-
import scrapy


class Sofang01Spider(scrapy.Spider):
    name = 'sofang_01'
    allowed_domains = ['sofang.com']
    start_urls = ['http://bj.sofang.com/city/citysList']

    def parse(self, response):
        provinces_cities = response.xpath("//div[@class='citys']/ul[@class='current']/li")
        for item in provinces_cities:
            province = item.xpath("./label/i/text()").extract_first().strip()
            cities = item.xpath("./p/a")
            for city in cities:
                city_name = city.xpath("./text()").extract_first().strip()
                city_url = city.xpath("./@href").extract_first()
                if province == "直辖市":
                    province = city_name
                    yield scrapy.Request(url=city_url, callback=self.parse_flag, meta={"province": province, "city": city_name})
                if province == "其它":
                    pass
                else:
                    yield scrapy.Request(url=city_url, callback=self.parse_flag, meta={"province": province, "city": city_name})

    def parse_flag(self, response):
        province = response.meta["province"]
        city = response.meta['city']
        navs = response.xpath("//ul[@class='fl nav']/li")
        for nav in navs:
            label = nav.xpath("./a/text()").extract_first().strip()
            if label == "写字楼":
                flags = nav.xpath("./div[@class='nav_list']/ul/li")
                for flag in flags:
                    flag_name = flag.xpath("./a/text()").extract_first().strip()
                    flag_url = response.urljoin(flag.xpath("./a/@href").extract_first())
                    if flag_name == "写字楼出租":
                        yield scrapy.Request(url=flag_url, callback=self.parse_district, meta={"province": province, "city": city, "flag": flag_name})
                    if flag_name == "写字楼出售":
                        yield scrapy.Request(url=flag_url, callback=self.parse_district, meta={"province": province, "city": city, "flag": flag_name})

    def parse_district(self, response):
        province = response.meta["province"]
        city = response.meta['city']
        flag = response.meta['flag']
        districts = response.xpath("//div[@class='search_info']/dl")[0].xpath("./dd/a")
        for district in districts:
            district_name = district.xpath("./text()").extract_first().strip()
            district_url = response.urljoin(district.xpath("./@href").extract_first())
            if district_name == "不限":
                pass
            else:
                yield scrapy.Request(url=district_url, callback=self.parse_street, meta={"province": province, "city": city, "flag": flag, "district": district_name})

    def parse_street(self, response):
        province = response.meta["province"]
        city = response.meta['city']
        district = response.meta['district']
        flag = response.meta['flag']
        housing_list = response.xpath("//p[@class='name']")
        if housing_list:
            streets = response.xpath("//dd[@class='subnav']/a")
            for street in streets:
                street_name = street.xpath("./text()").extract_first().strip()
                street_uri = street.xpath("./@href").extract_first()
                if street_name == "不限":
                    pass
                else:
                    street_url = response.urljoin(street_uri)
                    yield scrapy.Request(url=street_url, callback=self.parse_type, meta={"province": province, "city": city, "flag": flag, "district": district, "street": street})

    def parse_type(self, response):
        province = response.meta["province"]
        city = response.meta['city']
        district = response.meta['district']
        flag = response.meta['flag']
        street = response.meta['street']
        housing_list = response.xpath("//p[@class='name']")
        if housing_list:
            raw_items = response.xpath("//div[@class='search_info']/dl")
            for item in raw_items:
                dt = item.xpath("./dt/text()").extract_first().strip().rstrip("：")
                print(dt)