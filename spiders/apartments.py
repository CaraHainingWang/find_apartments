# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule, Request
from scrapy.linkextractors import LinkExtractor
import re
import math
from datetime import date
import json

months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
    'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

def formatted_string_to_int(num_string):
    if num_string:
        return int(num_string.replace(",", ""))
    else:
        return None

def parse_rent(rent_string):
    min_rent_re = re.search('\$([\d,]+)', rent_string)
    max_rent_re = re.search('.* - ([\w,]+)', rent_string)
    min_rent = math.inf
    max_rent = math.inf
    if min_rent_re:
        min_rent = formatted_string_to_int(min_rent_re.group(1))
    if max_rent_re:
        max_rent = formatted_string_to_int(max_rent_re.group(1))
    elif min_rent_re:
        max_rent = min_rent
    return min_rent, max_rent

def parse_available_date(date_list):
    if "Now" in date_list[1]:
        return date.today()
    if date_list[0] in months:
        return date(date.today().year, months[date_list[0]], int(date_list[1]))
    else:
        return date.max

def get_first_from_list(l):
    if l and len(l) > 0:
        return l[0]
    else:
        return None

def convert_bath_num_with_default(value_string, default_value=0):
    if '\u00bd' in value_string and len(value_string) == 2:
        return float(value_string[0]) + 0.5
    elif len(value_string) == 1:
        return float(value_string)
    else:
        return default_value

class ApartmentsSpider(CrawlSpider):
    name = 'apartments'
    allowed_domains = ['www.apartments.com']

    def start_requests(self):
        url = "https://www.apartments.com/"
        input_file_dir = getattr(self, 'input', None)
        with open(input_file_dir) as data_file:
            data = json.load(data_file)
        if data:
            for query in data['queries']:
                area_url = "".join([url, query['area']])
                yield Request(area_url, self.parse)
        else:
            yield Request(url, self.parse)

    def parse(self, response):
        #apartment_urls = response.xpath(
        #    '//article/section/div/div/a[contains(@href, "www")]/@href').extract()
        #if apartment_urls.empty():
        apartment_urls = response.xpath('//article[contains(@data-url, "www.apartments.com")]/@data-url').extract()
        for url in apartment_urls:
            yield(Request(url, callback=self.parse_apartment))

    def parse_apartment(self, response):
        apt_name = " ".join(response.xpath('//h1[contains(@class, "propertyName")]/text()').re(r'(\w+)'))
        street_addr = response.xpath('//span[@itemprop="streetAddress"]/text()').extract_first()
        addr_locality = response.xpath('//span[@itemprop="addressLocality"]/text()').extract_first()
        addr_region = response.xpath('//span[@itemprop="addressRegion"]/text()').extract_first()
        postal_code = response.xpath('//span[@itemprop="postalCode"]/text()').extract_first()

        apt_address = ' '.join(filter(None, [street_addr, addr_locality, addr_region, postal_code]))

        table_entries = response.xpath('//div[contains(@class, "active")]/*/*/*/tr')
        feature = response.xpath('//h3[contains(.//text(), "Features")]')
        phone = response.xpath('//span[@class="contactPhone"]/text()').extract_first()

        for entry in table_entries:
            rent_string = entry.xpath(
                './td[contains(@class, "rent")]/text()').extract_first()
            min_rent, max_rent = parse_rent(rent_string)
            avail_date = entry.xpath('./td[contains(@class, "avail")]/text()').re(r'(\w+)')
            bathroom_num = get_first_from_list(entry.xpath('./td[contains(@class, "bath")]/span[contains(@class, "short")]').re(r'([\d½]+) BA'))
            bedroom_num = get_first_from_list(entry.xpath('./td[contains(@class, "bed")]/span[contains(@class,"short")]').re(r'(\d) BR'))
            yield {
                'name': apt_name,
                'address': apt_address,
                'bathroom_num': convert_bath_num_with_default(bathroom_num),
                'bedroom_num': int(bedroom_num) if bedroom_num else 0,
                'min_rent': min_rent,
                'max_rent': max_rent,
                'unit': entry.xpath('./td[contains(@class, "unit")]/text()').extract_first(),
                'sqrt_foot': formatted_string_to_int(get_first_from_list(
                    entry.xpath('./td[contains(@class, "sqft")]/text()').re(r'([\w,]+) Sq Ft'))),
                'avail_date': parse_available_date(
                    entry.xpath('./td[contains(@class, "avail")]/text()').re(r'(\w+)')
                ),
                'phone': phone,
                'url': response.url,
                'feature_list': feature.xpath('../ul/li/text()').extract(),
            }
        next_page = response.xpath('//a[@class="next "]/@href').extract_first()
        if next_page:
            yield Request(next_page, callback=self.parse_apartment)
