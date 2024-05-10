from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.utils import parse_timestamp, dict_to_sha256_hex_digest, get_pub_date
import scrapy
import json

class GAOSpider(GCSpider):
    name = "government_accountability_office"  # Crawler name
    rotate_user_agent = True

    start_urls = ["https://www.gao.gov/reports-testimonies"]

    def parse(self, response):
        # Adjust the xpath to match GAO's report listing page structure
        report_links = response.xpath("//h4[contains(@class,'c-search-result__header')]/a/@href").extract()
        for link in report_links:
            url = response.urljoin(link)
            yield response.follow(url, self.parse_report)

    def parse_report(self, response):
        gao_id = response.url.split('/')[-1]
        title = response.xpath("//h1/text()").get().strip()
        summary = response.xpath("string(//div[contains(@class, 'c-search-result__summary')])").get().strip()
        published = response.xpath("//time/@datetime")[0].get()
        released = response.xpath("//time/@datetime")[1].get()
        pdf_links = response.xpath("//a[contains(@href, '.pdf')]/@href").extract()

        fields = {
            "gao_id": gao_id,
            "title": title,
            "summary": summary,
            "published": published,
            "released": released,
            "pdf_urls": pdf_links
        }

        yield self.create_doc_item(fields)

    def create_doc_item(self, fields):
        file_type = 'pdf'
        display_org = "Legislative Branch"
        data_source = "U.S. Government Accountability Office"
        source_title = "Reports and Testimonies"
        
        doc_name = fields['title']
        doc_num = fields['gao_id']
        doc_type = "GAO Report"
        display_doc_type = doc_type
        display_source = f"{data_source} - {source_title}"
        display_title = f"{doc_type}: {doc_name}"
        source_page_url = f"https://www.gao.gov/products/{doc_num}"
        publication_date = get_pub_date(fields['published'])
        
        downloadable_items = [{
            "doc_type": file_type,
            "download_url": fields['pdf_urls'],
            "compression_type": None
        }]

        version_hash_fields = {
            "doc_num": doc_num,
            "doc_name": doc_name,
            "download_url": fields['pdf_urls'],
            "publication_date": publication_date
        }

        version_hash = dict_to_sha256_hex_digest(version_hash_fields)

        return DocItem(
            doc_name=doc_name,
            doc_title=doc_name,
            doc_num=doc_num,
            doc_type=doc_type,
            display_doc_type=display_doc_type,
            publication_date=publication_date,
            cac_login_required=False,
            crawler_used=self.name,
            downloadable_items=downloadable_items,
            source_page_url=source_page_url,
            source_fqdn="gao.gov",
            download_url=fields['pdf_urls'],
            version_hash_raw_data=version_hash_fields,
            version_hash=version_hash,
            display_org=display_org,
            data_source=data_source,
            source_title=source_title,
            display_source=display_source,
            display_title=display_title,
            file_ext=file_type,
            is_revoked=False
        )

