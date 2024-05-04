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
    start_urls = [
        'https://www.saffm.hq.af.mil/FM-Resources/Budget/'
    ] # URL where the spider begins crawling

    file_type = "pdf" # Define filetype for the spider to identify.

    def clean_text(self, text):
        '''
        This function cleans text to ensure it is web safe.
        '''
        return re.sub(r'[^a-zA-Z0-9 ()\\-]', '', text.strip())

    def parse(self, response):
        '''
        Parses the initial page to identify available budget documents.
        '''
        driver: Chrome = response.meta["driver"]
        driver.get(response.url)
        time.sleep(5)

        year_buttons = Selector(text=driver.page_source).css('div[id="dnn_ctr44627_View_AccordionContainer"] a')

        for year_button in year_buttons:
            link = year_button.css('a::attr(href)').get()
            text = year_button.css('a::text').get()
            year = text[-4:]
            if int(year) >= 2014:
                driver.execute_script("arguments[0].click();", WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, text))))
                self.parse_page(driver, year)

    def parse_page(self, driver, year):
        '''
        Parse individual pages to extract document details.
        '''
        webpage = Selector(text=driver.page_source)
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
            doc_name = urljoin(driver.current_url, doc_url).split('/')[-1].replace('.pdf', '').replace('%20', ' ')
            doc_name = f'{doc_type};{year};{self.clean_text(doc_name)}'
            web_url = urljoin(driver.current_url, doc_url)

            fields = {
                'doc_name': doc_name,
                'doc_title': self.clean_text(doc_title),
                'doc_type': doc_type,
                'publication_date': year,
                'download_url': web_url,
                'source_page_url': driver.current_url
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
