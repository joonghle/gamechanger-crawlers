import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import re
import time
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest, get_pub_date

from selenium import webdriver

class JBOOKAirForceBudgetSpider(GCSeleniumSpider):
    '''
    Class defines the behavior for crawling and extracting text-based documents from the "Army Financial Management & Comptroller" site. 
    This class inherits the 'GCSeleniumSpider' class from GCSeleniumSpider.py. The GCSeleniumSpider class applies Selenium settings to the standard
    parse method used in Scrapy crawlers in order to return a Selenium response instead of a standard Scrapy response.

    This class and its methods = the jbook_air_force_budget "spider".
    '''

    name = 'jbook_air_force_budget' # Crawler name
    display_org = "Dept. of the Air Force" # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "Army Financial Management & Comptroller" # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unlisted Source" # Level 3 filter

    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['saffm.hq.af.mil'] # Domains the spider is allowed to crawl
    root_url = 'https://www.saffm.hq.af.mil/FM-Resources/Budget/'
    urls = [
        'https://www.saffm.hq.af.mil/FM-Resources/Budget/{}'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.
    years = range(2010-2000, datetime.today().year + 2 - 2000)

    def clean_text(self, text):
        '''
        This function cleans text to ensure it is web safe.
        '''
        return re.sub(r'[^a-zA-Z0-9 ()\\-]', '', text.strip())

    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    
    def start_requests(self):
        current_year = datetime.now().year
        start_year = 2014  # Adjust start year as needed
        for year in range(start_year, current_year + 1):
            year_suffix = str(year)[-2:]  # Get last two digits
            url = f'https://www.saffm.hq.af.mil/FM-Resources/Budget/Air-Force-Presidents-Budget-FY{year_suffix}/'
            yield scrapy.Request(url=url, callback=self.parse, meta={'year': year})

    def parse(self, response):
        # driver = response.meta["driver"]
        # driver.get(response.url)
        time.sleep(5)  # Ensure the page has loaded completely
        # print("Parse URL: ", response.url)
        # print("Response: ", response)
        year = response.meta['year']
        webpage = response #Selector(text=driver.page_source)
        content_sections = webpage.css('div[class="DNNModuleContent ModICGModulesExpandableTextHtmlC"] a')
        # print(content_sections)
        for content in content_sections:
            doc_url = content.css('a::attr(href)').get()
            doc_title = content.css('a::text').get()

            if doc_url is None or doc_title is None or 'javascript' in doc_url:
                continue

            is_rdte_document = ('Research, Development, Test and Evaluation' in doc_title or 'RDT&E' in doc_title or
                        'RDTE' in doc_url or 'RDT_E' in doc_url)
            is_procurement_document = ("PROCUREMENT" in doc_url or '/Proc/' in doc_url or ("Procurement" in doc_title and "Procurement" != doc_title))

            doc_type = 'rdte' if is_rdte_document else "procurement"
            doc_name = urljoin(response.url, doc_url).split('/')[-1].replace('.pdf', '').replace('%20', ' ')
            doc_name = f'{doc_type};{year};{self.clean_text(doc_name)}'
            web_url = urljoin(response.url, doc_url)

            fields = {
                'doc_name': doc_name,
                'doc_title': self.clean_text(doc_title),
                'doc_type': doc_type,
                'publication_date': year,
                'download_url': web_url,
                'source_page_url': response.url
            }

            yield self.populate_doc_item(fields)
        # try:
        #     self.parse_page(response)
        # except TimeoutException as e:
        #     self.logger.error(f'Timeout while trying to interact with year buttons: {str(e)}')
        # except NoSuchElementException as e:
        #     self.logger.error(f'Element not found: {str(e)}')

    def parse_page(self, response):
        '''
        Parse individual pages to extract document details.
        '''

        year = response.meta['year']
        webpage = response #Selector(text=driver.page_source)
        content_sections = webpage.css('div[class="DNNModuleContent ModICGModulesExpandableTextHtmlC"] a')

        for content in content_sections:
            doc_url = content.css('a::attr(href)').get()
            doc_title = content.css('a::text').get()

            if doc_url is None or doc_title is None or 'javascript' in doc_url:
                continue

            is_rdte_document = ('Research, Development, Test and Evaluation' in doc_title or 'RDT&E' in doc_title or
                        'RDTE' in doc_url or 'RDT_E' in doc_url)
            is_procurement_document = ("PROCUREMENT" in doc_url or '/Proc/' in doc_url or ("Procurement" in doc_title and "Procurement" != doc_title))

            doc_type = 'rdte' if is_rdte_document else "procurement"
            doc_name = urljoin(response.url, doc_url).split('/')[-1].replace('.pdf', '').replace('%20', ' ')
            doc_name = f'{doc_type};{year};{self.clean_text(doc_name)}'
            web_url = urljoin(response.url, doc_url)

            fields = {
                'doc_name': doc_name,
                'doc_title': self.clean_text(doc_title),
                'doc_type': doc_type,
                'publication_date': year,
                'download_url': web_url,
                'source_page_url': response.url
            }

            yield self.populate_doc_item(fields)

    def populate_doc_item(self, fields):
        '''
        Populate and return a DocItem object with metadata.
        '''
        display_doc_type = "Document"
        display_source = self.data_source + " - " + self.source_title
        display_title = fields['doc_type'] + " " + fields['doc_name'] + ": " + fields['doc_title']
        is_revoked = False

        downloadable_items = [{
            "doc_type": self.file_type,
            "download_url": fields['download_url'],
            "compression_type": None
        }]

        version_hash_fields = {
            "item_currency": fields['download_url'].split('/')[-1],
            "document_title": fields['doc_title'],
            "publication_date": fields['publication_date'],
        }
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=fields['doc_name'],
            doc_title=fields['doc_title'],
            doc_type=fields['doc_type'],
            publication_date=get_pub_date(fields['publication_date']),
            cac_login_required=self.cac_login_required,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=fields['source_page_url'],
            source_fqdn=urlparse(fields['source_page_url']).netloc,
            download_url=fields['download_url'],
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=self.display_org,
            data_source=self.data_source,
            source_title=self.source_title,
            display_source=display_source,
            display_title=display_title,
            file_ext=self.file_type,
            is_revoked=is_revoked,
        )
