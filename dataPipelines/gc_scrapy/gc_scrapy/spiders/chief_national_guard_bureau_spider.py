import scrapy
from dataPipelines.gc_scrapy.gc_scrapy.items import DocItem
from dataPipelines.gc_scrapy.gc_scrapy.GCSpider import GCSpider


class CNGBISpider(GCSpider):
    """
        Parser for Chief National Guard Bureau Instructions
    """

    name = "National_Guard"  # Crawler name
    display_org = "National Guard"  # Level 1: GC app 'Source' filter for docs from this crawler
    data_source = "National Guard Bureau Publications & Forms Library"  # Level 2: GC app 'Source' metadata field for docs from this crawler
    source_title = "Unlisted Source"  # Level 3 filter

    allowed_domains = ['ngbpmc.ng.mil']
    start_urls = [
        'https://www.ngbpmc.ng.mil/publications1/cngbi/'
    ]

    file_type = "pdf"
    doc_type = "CNGBI"
    rotate_user_agent = True

    def parse(self, response):
        rows = response.css('div.WordSection1 table tbody tr')

        for row in rows:
            href_raw = row.css('td:nth-child(1) a::attr(href)').get()

            if not href_raw.startswith('/'):
                cac_login_required = True
            else:
                cac_login_required = False

            web_url = self.ensure_full_href_url(href_raw, self.start_urls[0])

            file_type = self.get_href_file_extension(href_raw)

            downloadable_items = [
                {
                    "doc_type": file_type,
                    "web_url": web_url.replace(' ', '%20'),
                    "compression_type": None
                }
            ]

            # a lot of the docs have the space unicode \xa0 in them. replacing it before getting doc_num
            doc_name_raw = row.css('td:nth-child(1) a::text').get().replace(u'\xa0', ' ')

            # one row returns empty for doc_name_raw, so skip that case
            if not doc_name_raw:
                continue

            doc_num_raw = doc_name_raw.replace('CNGBI ', '')

            publication_date = row.css('td:nth-child(2) span::text').get()

            doc_title_raw = row.css('td:nth-child(3) a::text').get()
            if doc_title_raw is None:
                doc_title_raw = row.css('td:nth-child(3) span::text').get()

            doc_title = self.ascii_clean(doc_title_raw)

            version_hash_fields = {
                "item_currency": href_raw,
                "document_title": doc_title,
                "document_number": doc_num_raw
            }

            yield DocItem(
                doc_name=doc_name_raw,
                doc_title=doc_title,
                doc_num=doc_num_raw,
                publication_date=publication_date,
                cac_login_required=cac_login_required,
                downloadable_items=downloadable_items,
                version_hash_raw_data=version_hash_fields,
            )
