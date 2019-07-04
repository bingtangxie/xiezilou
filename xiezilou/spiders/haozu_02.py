# -*- coding: utf-8 -*-
import scrapy


class Haozu02Spider(scrapy.Spider):
    name = 'haozu_02'
    allowed_domains = ['haozu.com']
    start_urls = ['http://haozu.com/']

    def parse(self, response):
        pass
