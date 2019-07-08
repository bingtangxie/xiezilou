# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime
import re


class XiezilouPipeline(object):
    def process_item(self, item, spider):
        if spider.name == "lianjia_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                'province',
                'city',
                'district',
                'street',
                'flag',
                'building_url',
                'building_name',
                'area_extent',
                'building_description',
                'price_extent',
                'buiding_agent',
                'buiding_agent_phone',
                'building_address',
                'building_height',
                'building_elevator',
                'business_circle',
                'developer',
                'housing_name',
                'housing_description',
                'housing_url',
                'traffic',
                'zb_suite',
                'other',
                'housing_price1',
                'housing_price2',
                'housing_area',
                'housing_floor',
                'housing_workplace',
                'publish_time',
                'housing_agents_info',
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            spider.db[base_key].insert(data)
            spider.redis.hset("lianjia_xzl_housing_finished_hashtable", data['housing_url'], 1)

        if spider.name == "tongcheng58_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                'province',
                'city',
                'district',
                'street',
                'flag',
                'housing_url',
                'housing_price1',
                'housing_price2',
                'housing_area',
                'housing_floor',
                'housing_workplace',
                'publish_time',
                'property_fee',
                'housing_decor',
                'housing_use_rate',
                'corp_reged',
                'pay_method',
                'central_air_condition',
                'peitao',
                'property_level',
                'agent',
                'agent_phone',
                'xzl_type',
                'rent_lease',
                'loupan',
                'building_address'
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            zs_key = base_key + "_xzl_zset"
            spider.db[base_key].insert(data)
            spider.redis.zadd(zs_key, {data['housing_url']: 3})

        if spider.name == "ganji_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                'city',
                'district',
                'street',
                'flag',
                'housing_url',
                'housing_price1',
                'housing_price2',
                'housing_area',
                'housing_floor',
                'publish_time',
                'housing_decor',
                'central_air_condition',
                'peitao',
                'agent',
                'agent_phone',
                'agent_company',
                'xzl_type',
                'rent_lease',
                'business_circle',
                'building_address',
                'housing_name'
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            zs_key = base_key + "_xzl_zset"
            spider.db[base_key].insert(data)
            spider.redis.zadd(zs_key, {data['housing_url']: 3})

        if spider.name == "fangtianxia_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                "province",
                "city",
                "district",
                "street",
                "xzl_type",
                "flag",
                "housing_url",
                "publish_time",
                "housing_name",
                "housing_price1",
                "housing_price2",
                "pay_method",
                "business_circle",
                "loupan",
                "housing_floor",
                "building_address",
                "agent",
                "agent_phone",
                "agent_company",
                "property_level",
                "housing_decor",
                "property_fee",
                "bangong",
                "zb_suite",
                "traffic",
                "place",
                "housing_detail_url",
                "phone",
                "housing_area"
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            zs_key = base_key + "_xzl_zset"
            spider.db[base_key].insert(data)
            spider.redis.zadd(zs_key, {data['housing_url']: 3})

        if spider.name == "anjuke_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                "province",
                "city",
                "district",
                "street",
                "flag",
                "housing_url",
                "publish_time",
                "housing_name",
                "housing_price1",
                "housing_price2",
                "pay_method",
                "loupan",
                "housing_floor",
                "building_address",
                "agent",
                "agent_phone",
                "agent_company",
                "housing_decor",
                "property_fee",
                "traffic",
                "housing_area",
                "peitao",
                "central_air_condition",
                "corp_reged",
                "housing_workplace",
                "housing_use_rate",
                "rent_lease",
                "housing_type"
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            zs_key = base_key + "_xzl_zset"
            spider.db[base_key].insert(data)
            spider.redis.zadd(zs_key, {data['housing_url']: 3})

        if spider.name == "diandianzu_02":
            data = dict(item)
            data['created'] = datetime.now()
            fields = [
                "province",
                "city",
                "district",
                "street",
                "housing_url",
                "publish_time",
                "housing_name",
                "housing_price1",
                "building_address",
                "built_in",
                "layer_height",
                "building_height",
                "property",
                "property_fee",
                "parking_place",
                "parking_fee",
                "air_condition",
                "air_condition_fee",
                "air_condition_time",
                "elevator",
                "network",
                "settled_enterprise"
            ]
            for i in range(len(fields)):
                if fields[i] not in data:
                    data[fields[i]] = ""
            base_key = re.search("(.+)_\d+", spider.name).group(1)
            zs_key = base_key + "_xzl_zset"
            spider.db[base_key].insert(data)
            spider.redis.zadd(zs_key, {data['housing_url']: 3})