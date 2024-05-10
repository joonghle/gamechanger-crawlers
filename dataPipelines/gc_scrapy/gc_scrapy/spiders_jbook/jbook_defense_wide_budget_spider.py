# JBOOK CRAWLER
# Defense Wide Budget Spider

from gc import callbacks
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
import scrapy
from scrapy import Selector
from urllib.parse import urljoin, urlparse
from datetime import datetime
import re
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import dict_to_sha256_hex_digest

class JBOOKDefenseWideBudgetSpider(GCSpider):
    name = 'jbook_defense_wide_budget'
    display_org = "Dept. of Defense"
    data_source = "Defense Wide Budget Materials"
    source_title = "Defense Wide Budget"
    
    cac_login_required = False
    rotate_user_agent = True 
    root_url = 'https://comptroller.defense.gov/Budget-Materials/'   
    urls = ['https://comptroller.defense.gov/Budget-Materials/Budget{}/',
           'https://comptroller.defense.gov/Budget-Materials/FY{}BudgetJustification/']
    
    allowed_domains = ['comptroller.defense.gov/']
    
    file_type = 'pdf'
    latest_year = int(datetime.today().year) + 2
    years = range(2014, latest_year)
 
    def start_requests(self):
        for year in self.years:
            for url in self.urls:
                yield scrapy.Request(url.format(year))
        
    def parse(self, response):
        content = response.css("a[href*='.pdf']")
        for c in content:
            
            is_revoked = False

            doc_title_raw = c.css("::text").get()
            doc_title = self.ascii_clean(str(doc_title_raw))
            doc_url = c.css('::attr(href)').get()
            
            web_url = urljoin(response.url, doc_url)
            url_tags = ['02_Procurement', '_p1r', '_p1', '_r1', '03_RDT_and_E']
            
            if doc_url is None or not any(tag in doc_url for tag in url_tags):
                continue
            
            doc_type = ''
            for tag in url_tags:
                if tag in doc_url:
                    doc_type = tag
                else:
                    doc_type is None
                    
            doctype_mapping = {'02_Procurement': 'procurement', '_p1r':'procurement', '_p1':'procurement', '_r1':'rdte', '03_RDT_and_E':'rdte'}
            for key, value in doctype_mapping.items():
                doc_type = doc_type.replace(key, value)
            
            publication_year = re.search('[2][0-9]{3}', doc_url)
            publication_date = '01/01/' + publication_year.group()
            
            doc_name = doc_url.split('/')[-1][:-4]
            doc_name = doc_name.replace('%20', '_')
            if doc_title == 'None':
                doc_title = doc_name.replace('_', ' ')
                        
            amendment_search = re.search("amend\w*", doc_url, re.IGNORECASE)
            year_search = re.search('(fy|pb)[2][0-9]{1,3}|(fy|pb)_[2][0-9]{1,3}', doc_name, re.IGNORECASE) 
            rdte_tags = ['r1', 'rdte']
            procurement_tags = ['p1r', 'p1', 'procurement']
            
            if amendment_search:
                amendment_tag = doc_url.split('/')[-2]
                doc_name = doc_name  + '_' + amendment_tag    
            year = publication_year.group()    

            doc_name = f'{doc_type};{year};{doc_name}' 
       
            download_url = web_url
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
            doc_title=doc_title,
            doc_type=doc_type,
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