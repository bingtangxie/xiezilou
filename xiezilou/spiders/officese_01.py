# -*- coding: utf-8 -*-
import scrapy


class Officese01Spider(scrapy.Spider):
    name = 'officese_01'
    allowed_domains = ['officese.com']
    # start_urls = ['http://officese.com/']

    def start_requests(self):
        uris = {
            "beijing": "北京",
            "tianjin": "天津",
            "sjz": "石家庄",
            "langfang": "廊坊",
            "baoding": "保定",
            "tangshan": "唐山",
            "dalian": "大连",
            "shenyang": "沈阳",
            "nanjing": "南京",
            "suzhou": "苏州",
            "wuxi": "无锡",
            "jinan": "济南",
            "qingdao": "青岛",
            "hangzhou": "杭州",
            "fuzhou": "福州",
            "hefei": "合肥",
            "huaian": "淮安",
            "guangzhou": "广州",
            "shenzhen": "深圳",
            "changsha": "长沙",
            "xiamen": "厦门",
            "chengdu": "成都",
            "chongqing": "重庆",
            "wuhan": "武汉",
            "zhengzhou": "郑州",
            "xian": "西安",
            "shanghai": "上海"
        }
        for uri in uris:
            url = "http://{uri}.officese.com/rent".format(uri=uri)
            city = uris[uri]
            yield scrapy.Request(url=url, callback=self.parse_type, meta={"city": city})

    def parse_type(self, response):
        data = response.meta.copy()
        types = response.xpath("//ul[@class='floatl']/li")[3]
        print(types)

