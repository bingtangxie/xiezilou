# -*- coding: utf-8 -*-
import scrapy


class Haozu01Spider(scrapy.Spider):
    name = 'haozu_01'
    allowed_domains = ['haozu.com']
    start_urls = ['http://haozu.com/']

    def parse(self, response):
        navs = response.xpath("//ul[@class='headNav-list']/li")
