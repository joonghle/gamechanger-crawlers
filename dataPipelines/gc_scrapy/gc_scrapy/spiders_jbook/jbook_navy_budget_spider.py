# JBOOK CRAWLER
# Navy Budget Spider

import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest

class JBOOKNavyBudgetSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Financial Management & Comptroller" site.
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the jbook_navy_budget "spider".
    '''

    name = 'jbook_navy_budget'  # Crawler name
    display_org = "Dept. of Defense"  # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Navy Financial Management & Comptroller Budget Materials"  # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Navy Budget"  # Level 3 filter

    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['secnav.navy.mil']  # Domains the spider is allowed to crawl
    start_urls = [
        'https://www.secnav.navy.mil/fmc/fmb/Documents/Forms/AllItems.aspx'
    ]  # URL where the spider begins crawling

    file_type = "pdf"  # Define filetype for the spider to identify.

    @staticmethod
    def clean(text):
        '''
        This function forces text into the ASCII characters set, ignoring errors
        '''
        return text.encode('ascii', 'ignore').decode('ascii').strip()

    def parse(self, response):
        year_buttons = response.css('td[class="ms-cellstyle ms-vb-title"]')
        for year_button in year_buttons:
            link = year_button.css('a::attr(href)').get()
            text = year_button.css('a::text').get()

            # If we are looking at a folder we want to parse
            if 'pres' in text:
                if text[0] == '9':
                    year = '19' + text[0:2]
                else:
                    year = '20' + text[0:2]

                if int(year) >= 2014:
                    yield response.follow(url=link, callback=self.parse_page, meta={"year": year})

    def parse_page(self, response):
        year = response.meta["year"]

        pattern = r'\bvar\s+WPQ2ListData\s*=\s*\{[\s\S]*?\}\n\]'
        table_data = response.css('script::text').re_first(pattern)
        try:
            table_data = table_data.split('],"FirstRow"')[0].split(': \n[')[1]
        except:
            print('ERROR: Blocked from site.')
            yield DocItem()
        for doc_string in table_data.split(',{'):
            if not doc_string[0] == '{':
                doc_string = '{' + doc_string
            doc_dict = json.loads(doc_string)

            doc_url = doc_dict['FileRef'].replace('\u002f', '/')
            doc_title = doc_dict['Title']

            if doc_title is '':
                doc_title = doc_dict['FileLeafRef'].replace('.pdf', '')

            is_revoked = False

            publication_date = doc_dict['Modified'].replace('\u002f', '/')

            doc_type = 'procurement' if 'PROCUREMENT' in doc_dict["Section"] else 'rdte'
            doc_name = doc_dict['FileLeafRef'].replace('.pdf', '')
            doc_name = f'{doc_type};{year};{doc_name}'

            download_url = urljoin(response.url, doc_url)
            downloadable_items = [
                {
                    "doc_type": "pdf",
                    "download_url": download_url,
                    "compression_type": None
                }
            ]

            version_hash_fields = {
                "item_currency": downloadable_items[0]["download_url"].split('/')[-1],
                "document_title": doc_title,
                "publication_date": publication_date,
            }

            doc_item = self.populate_doc_item(doc_name, doc_type, doc_title, publication_date, download_url, downloadable_items, version_hash_fields, response.url, is_revoked)
            yield doc_item

    def populate_doc_item(self, doc_name, doc_type, doc_title, publication_date, download_url, downloadable_items, version_hash_fields, source_page_url, is_revoked):
        '''
        This function provides both hardcoded and computed values for the variables
        in the imported DocItem object and returns the populated metadata object
        '''
        display_doc_type = doc_type.upper()
        display_source = self.data_source + " - " + self.source_title
        display_title = doc_name + ": " + doc_title
        source_fqdn = urlparse(source_page_url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=self.ascii_clean(doc_title),
            doc_type=self.ascii_clean(doc_type),
            display_doc_type=display_doc_type,
            publication_date=publication_date,
            cac_login_required=self.cac_login_required,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=source_page_url,
            source_fqdn=source_fqdn,
            download_url=download_url,
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=self.display_org,
            data_source=self.data_source,
            source_title=self.source_title,
            display_source=display_source,
            display_title=display_title,
            file_ext="pdf",
            is_revoked=is_revoked,
        )