# -*- coding: utf-8 -*-

# Define here the models for your scraped Fields
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/Fields.html

import scrapy


class XiezilouItem(scrapy.Item):
    # define the fields for your Field here like:
    # name = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    district = scrapy.Field()
    street = scrapy.Field()
    flag = scrapy.Field()
    building_url = scrapy.Field()
    building_name = scrapy.Field()
    area_extent = scrapy.Field()
    building_description = scrapy.Field()
    price_extent = scrapy.Field()
    buiding_agent = scrapy.Field()
    buiding_agent_phone = scrapy.Field()
    building_address = scrapy.Field()
    building_height = scrapy.Field()
    building_elevator = scrapy.Field()
    business_circle = scrapy.Field()
    developer = scrapy.Field()
    housing_name = scrapy.Field()
    housing_description = scrapy.Field()
    housing_price = scrapy.Field()
    housing_url = scrapy.Field()
    traffic = scrapy.Field()
    zb_suite = scrapy.Field()
    other = scrapy.Field()
    housing_price1 = scrapy.Field()
    housing_price2 = scrapy.Field()
    housing_area = scrapy.Field()
    housing_floor = scrapy.Field()
    housing_workplace = scrapy.Field()
    publish_time = scrapy.Field()
    housing_agents_info = scrapy.Field()

    corp_reged = scrapy.Field()
    rent_lease = scrapy.Field()
    property_fee = scrapy.Field()
    housing_decor = scrapy.Field()
    housing_use_rate = scrapy.Field()
    pay_method = scrapy.Field()
    central_air_condition = scrapy.Field()
    peitao = scrapy.Field()
    loupan = scrapy.Field()
    property_level = scrapy.Field()
    agent = scrapy.Field()
    agent_phone = scrapy.Field()
    agent_company = scrapy.Field()
    xzl_type = scrapy.Field()
    phone = scrapy.Field()
    bangong = scrapy.Field()
    traffic = scrapy.Field()
    place = scrapy.Field()
    housing_detail_url = scrapy.Field()
    housing_type = scrapy.Field()

    built_in = scrapy.Field()
    layer_height = scrapy.Field()
    property = scrapy.Field()
    parking_place = scrapy.Field()
    parking_fee = scrapy.Field()
    air_condition = scrapy.Field()
    air_condition_fee = scrapy.Field()
    air_condition_time = scrapy.Field()
    elevator = scrapy.Field()
    network = scrapy.Field()
    settled_enterprise = scrapy.Field()

