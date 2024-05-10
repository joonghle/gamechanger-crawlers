import scrapy
from scrapy import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import re
import os
from urllib.parse import urljoin, urlparse
from datetime import datetime

from dataPipelines.gc_scrapy.gc_scrapy.middleware_utils.selenium_request import SeleniumRequest
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSeleniumSpider import GCSeleniumSpider
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest
# logging.basicConfig(level=logging.INFO)

class GAOSpider(GCSpider):
    name = 'gao'
    display_org = "Government Accountability Office"
    data_source = "GAO Reports & Testimonies"
    source_title = "GAO Reports"
    cac_login_required = False
    rotate_user_agent = True
    allowed_domains = ['www.gao.gov']
    start_urls = ['https://www.gao.gov/reports-testimonies']
    file_type = 'pdf'

    def __init__(self, *args, **kwargs):
        super(GAOSpider, self).__init__(*args, **kwargs)
        self.start_date = "2014-01-01"  # Hardcoded start date
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%d")  # Today's date

    def start_requests(self):
        start = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")

        start = round(datetime.datetime.timestamp(start))
        end = round(datetime.datetime.timestamp(end))

        params = {
            "page": 0,
            "f[0]": f"rt_date_range_gui:(min:{start},max:{end})",
        }

        yield scrapy.Request(self.start_urls[0], callback=self.parse, meta={'params': params})

    def parse(self, response):
        params = response.meta['params']
        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(response.url)

        rows = page.xpath("//div[contains(@class,'gao-filter')]//div[contains(@class,'views-row')]")
        for row in rows:
            doc_item = self.process_item(row)
            if doc_item:
                yield doc_item

        next_page = params['page'] + 1
        params['page'] = next_page
        next_page_url = f"{self.start_urls[0]}?{self.encode_params(params)}"

        if len(rows) > 0:
            yield scrapy.Request(next_page_url, callback=self.parse, meta={'params': params})

    def process_item(self, item):
        link = item.xpath(".//h4[contains(@class,'c-search-result__header')]/a")[0]
        title = link.xpath("string(.)")[0]
        title = re.sub(r"\s+", " ", title)
        url = link.xpath("@href")[0]

        gao_id = item.xpath(".//span[contains(@class, 'd-block')]/text()")[0].strip()

        # some items have an empty d-block, e.g. d25791
        if gao_id == "":
            gao_id = re.findall(f"gao\.gov/products/(.*)", url, flags=re.IGNORECASE)[0]

        if gao_id == "":
            logging.error("Unable to fetch GAO ID.")
            return None

        gao_id = self.clean_id(gao_id)

        summary = item.xpath(
            "string(.//div[contains(@class, 'c-search-result__summary')])"
        ).strip()

        published = item.xpath(".//span[contains(@class,'text-small')]/time/@datetime")[0]
        released = item.xpath(".//span[contains(@class,'text-small')]/time/@datetime")[1]

        logging.info("%s %s %s", gao_id, title, url)

        product_url = f"https://www.gao.gov/products/{gao_id}"
        logging.info("GET %s", product_url)

        product_page = requests.get(product_url).content
        product_page = lxml.html.fromstring(product_page)
        product_page.make_links_absolute(product_url)

        links = []
        versions = product_page.xpath(
            "//section[contains(@class, 'js-endpoint-full-report')]//a"
        )
        for version in versions:
            version_name = version.xpath("string(.)").strip()
            version_url = version.xpath("@href")[0]
            links.append({"doc_type": self.file_type, "download_url": version_url, "compression_type": None})

        topics = set()

        # GAO uses two sets of tags, a "topic" and zero or more "subjects"
        primary = product_page.xpath(
            "//div[contains(@class,'views-field-field-topic')]/div/a"
        )
        for tag in primary:
            topics.add(tag.xpath("text()")[0].strip())

        tags = product_page.xpath(
            "//div[contains(@class,'views-field-field-subject-term')]/div/span"
        )
        for tag in tags:
            topics.add(tag.xpath("text()")[0].strip())

        # can't json serialize the set
        topics = list(topics)

        is_revoked = False

        version_hash_fields = {
            "item_currency": gao_id,
            "document_title": title,
            "publication_date": published,
        }

        doc_name = f"GAO;{gao_id}"
        display_doc_type = "Report"
        display_source = self.data_source + " - " + self.source_title
        display_title = f"GAO {gao_id}: {title}"
        source_fqdn = urlparse(url).netloc
        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=title,
            doc_type="GAO",
            doc_num=gao_id,
            display_doc_type=display_doc_type,
            publication_date=published,
            cac_login_required=self.cac_login_required,
            crawler_used=self.name,
            downloadable_items=links,
            source_page_url=url,
            source_fqdn=source_fqdn,
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

    def clean_id(self, id: str) -> str:
        # NSIAD/AIMD-00-329 is nsiadaimd-00-329
        return id.replace("/", "")

    def encode_params(self, params):
        encoded_params = []
        for key, value in params.items():
            encoded_params.append(f"{key}={value}")
        return "&".join(encoded_params)