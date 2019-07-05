# -*- coding: utf-8 -*-
import scrapy


class Anjuke02Spider(scrapy.Spider):
    name = 'anjuke_02'
    allowed_domains = ['anjuke.com']
    start_urls = ['http://anjuke.com/']

    def parse(self, response):
        pass
