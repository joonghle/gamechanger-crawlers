# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import Union
from itemadapter import ItemAdapter
from datetime import datetime
import os
import json
from scrapy.exceptions import DropItem
from jsonschema.exceptions import ValidationError

from .validators import DefaultOutputSchemaValidator, SchemaValidator
from . import OUTPUT_FOLDER_NAME

from dataPipelines.gc_crawler.utils import dict_to_sha256_hex_digest, get_fqdn_from_web_url

import scrapy
from scrapy.pipelines.media import MediaPipeline

from pathlib import Path

SUPPORTED_FILE_EXTENSIONS = [
    "pdf",
    "html",
    "zip"
]


class FileDownloadPipeline(MediaPipeline):
    MEDIA_ALLOW_REDIRECTS = True
    previous_hashes = set()
    output_dir: Path
    previous_manifest_path: Path
    job_manifest_path: Path
    dont_filter_previous_hashes: bool

    def open_spider(self, spider):
        super().open_spider(spider)
        self.output_dir = Path(spider.download_output_dir).resolve()
        self.job_manifest_path = Path(
            self.output_dir, 'manifest.json').resolve()

        self.previous_manifest_path = Path(
            spider.previous_manifest_location).resolve()

        if not spider.dont_filter_previous_hashes:
            self.load_hashes_from_cumulative_manifest(
                self.previous_manifest_path, spider.name)

    def load_hashes_from_cumulative_manifest(self, previous_manifest_path, spider_name):
        file_location = (
            Path(previous_manifest_path).resolve()
            if previous_manifest_path
            else None
        )

        if not file_location or not os.path.isfile(file_location):
            print(
                f"\n\nPrevious manifest at {file_location} is not a file! Nothing will be filtered!\n\n")
            if self.dont_filter_previous_hashes:
                return
            else:
                exit(1)

        print('Reading in previous manifest')
        count = 0
        with file_location.open(mode="r") as f:
            for line in f.readlines():
                count += 1
                if count % 1000 == 0:
                    print(f"{count} lines read in")
                if not line.strip():
                    continue

                jdoc = json.loads(line)
                crawler_used = jdoc.get('crawler_used')
                # covers old manifest items with no crawler info
                if not crawler_used:
                    self.previous_hashes.add(jdoc['version_hash'])
                # skips adding hashes for other spiders for combined manifest files with that info
                elif crawler_used == spider_name:
                    self.previous_hashes.add(jdoc['version_hash'])

        num_hashes = len(self.previous_hashes)
        print(f"Previous manifest loaded, will filter {num_hashes} hashes")

    @staticmethod
    def get_first_supported_downloadable_item(downloadable_items: list) -> Union[dict, None]:
        """Get first supported downloadable item corresponding to doc, has correct type and is not cac blocked"""
        return next((item for item in downloadable_items if item["doc_type"] in SUPPORTED_FILE_EXTENSIONS), None)

    def get_media_requests(self, item, info):
        """Called per DocItem from spider output, yields the media requests to download, response sent to media_downloaded"""

        # info = SpiderInfo
        # class SpiderInfo:
        # self.spider = spider
        # self.downloading = set()
        # self.downloaded = {}
        # self.waiting = defaultdict(list)

        doc_name = item["doc_name"]
        if item["version_hash"] in self.previous_hashes:
            # dont download anything just send item to crawl output
            print(
                f"Skipping download of {item.get('doc_name')} because it was in previous_hashes")
            return item

        if item["cac_login_required"]:
            print(
                f"Skipping download of {item.get('doc_name')} because it requires cac login")
            return item

        # currently we only associate 1 file with each doc, this gets the first we know how to parse
        file_item = self.get_first_supported_downloadable_item(
            item["downloadable_items"])

        if file_item:
            url = file_item['web_url']
            extension = file_item['doc_type']
            output_file_name = f"{doc_name}.{extension}"

            try:
                yield scrapy.Request(url, meta={"output_file_name": output_file_name})
            except Exception as probably_url_error:
                print('~~~~ REQUEST ERR', probably_url_error)
        else:
            print(f"No supported downloadable item for {item['doc_name']}")
            return item

    def media_downloaded(self, response, request, info):
        """Called for each completed response from get_media_requests, returned to item_completed"""
        # I dont know why this isnt being handled automatically here
        # Just filtering by response code
        if 200 <= response.status < 300:
            return (True, response, None)
        elif not len(response.body):
            return (False, response, "Response has empty body")
        else:
            return (False, response, None)

    def media_failed(self, failure, request, info):
        # I have never seen this called
        print("**** MEDIA FAILED")
        print(failure)
        print(info.spider)
        return (False, failure, "Pipeline Media Request Failed")

    def add_to_dead_queue(self, item, reason):
        path = f'{self.output_dir}dead_queue.json'
        if isinstance(reason, int):
            reason_text = f"HTTP Response Code {reason}"
        elif isinstance(reason, str):
            reason_text = reason
        else:
            reason_text = "Unknown failure"

        with open(path, 'a+') as f:
            dead_dict = {"document": dict(item), "failure_reason": reason_text}
            try:
                f.write(json.dumps(dead_dict))
                f.write('\n')

            except Exception as e:
                print('Failed to write to dead_queue file',
                      path, e)

    def add_to_manifest(self, item):
        path = self.job_manifest_path
        with open(self.job_manifest_path, 'a') as f:
            try:
                f.write(
                    json.dumps(
                        {
                            "version_hash": item["version_hash"],
                            "doc_name": item["doc_name"],
                            "crawler_used": item["crawler_used"],
                            "access_timestamp": item["access_timestamp"]
                        }
                    )
                )
                f.write('\n')

            except Exception as e:
                print('Failed to write to manifest file',
                      path, e)

    def item_completed(self, results, item, info):
        """Called per item when all media requests have been processed"""
        # return item for crawler output if download was skipped
        if not info.downloaded:
            return item

        # first in results is supposed to be ok status but it always returns true b/c 404 doesnt cause failure for some reason :(
        # so I added it in the media downloaded part as a sub tuple in return
        file_downloads = []
        for (_, (okay, response, reason)) in results:
            if not okay:
                self.add_to_dead_queue(
                    item, reason if reason else int(response.status))
            else:
                output_file_name = response.meta["output_file_name"]

                file_download_path = Path(self.output_dir, output_file_name)
                metadata_download_path = f"{file_download_path}.metadata"

                with open(file_download_path, 'wb') as f:
                    try:
                        f.write(response.body)
                        print('downloaded', file_download_path)
                        file_downloads.append(file_download_path)

                    except Exception as e:
                        print('Failed to write file', file_download_path, e)

                with open(metadata_download_path, 'w') as f:
                    try:
                        f.write(json.dumps(dict(item)))

                    except Exception as e:
                        print('Failed to write metadata',
                              file_download_path, e)

        # if nothing was downloaded so don't add to manifest, just return item to crawl output
        if file_downloads:
            self.add_to_manifest(item)

        return item


class DeduplicaterPipeline():
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if not item['doc_name']:
            raise DropItem("No doc_name")

        elif item['doc_name'] in self.ids_seen:
            raise DropItem("Duplicate doc_name found")

        else:
            self.ids_seen.add(item['doc_name'])

        return item


class AdditionalFieldsPipeline:
    def process_item(self, item, spider):

        if item.get('crawler_used') is None:
            item['crawler_used'] = spider.name

        source_page_url = item.get('source_page_url')
        if source_page_url is None:
            if spider.source_page_url:
                item["source_page_url"] = spider.source_page_url
            else:
                source_page_url = spider.start_urls[0]
                item['source_page_url'] = source_page_url

        if item.get('source_fqdn') is None:
            item['source_fqdn'] = get_fqdn_from_web_url(source_page_url)

        if item.get('version_hash') is None:
            # ensure doc_name is part of hash
            item["version_hash_raw_data"]["doc_name"] = item["doc_name"]
            item['version_hash'] = dict_to_sha256_hex_digest(
                item["version_hash_raw_data"]
            )

        if item.get('access_timestamp') is None:
            item['access_timestamp'] = datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S.%f')

        if item.get('publication_date') is None:
            item['publication_date'] = "N/A"

        if item.get('cac_login_required') is None:
            item['cac_login_required'] = spider.cac_login_required

        if item.get('doc_type') is None:
            item['doc_type'] = spider.doc_type

        if item.get('doc_num') is None:
            item['doc_num'] = ""

        return item


class ValidateJsonPipeline:
    """Validates json as Scrapy passes each item to be validated to self.process_item
    :param validator: output validator"""

    def __init__(self, validator: SchemaValidator = DefaultOutputSchemaValidator()):

        if not isinstance(validator, SchemaValidator):
            raise TypeError("arg: validator must be of type SchemaValidator")

        self.validator = validator

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        name = item_dict.get('doc_name', str(item_dict))

        try:
            self.validator.validate_dict(item_dict)
            return item
        except ValidationError as ve:
            raise DropItem(f'Dropped Item: {name} failed validation: {ve}')


class JsonWriterPipeline(object):
    def open_spider(self, spider):
        if not os.path.exists(OUTPUT_FOLDER_NAME):
            os.makedirs(OUTPUT_FOLDER_NAME)
        json_name = './' + OUTPUT_FOLDER_NAME + '/' + spider.name + '.json'

        self.file = open(json_name, 'w')
        # Your scraped items will be saved in the file 'scraped_items.json'.
        # You can change the filename to whatever you want.

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        doc = item['document']

        validator = DefaultOutputSchemaValidator()
        validator.validate(doc)
        self.file.write(doc + '\n')
        return doc
