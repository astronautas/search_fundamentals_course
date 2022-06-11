# From https://github.com/dshvadskiy/search_with_machine_learning_course/blob/main/index_products.py
import requests
from lxml import etree

import click
import glob
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import logging
import time
from functools import lru_cache
from opensearchpy.helpers import bulk
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# NOTE: this is not a complete list of fields.  If you wish to add more, put in the appropriate XPath expression.
# TODO: is there a way to do this using XPath/XSL Functions so that we don't have to maintain a big list?
mappings = [
    "sku/text()", "sku", # SKU is the unique ID, productIds can have multiple skus
    "productId/text()", "productId",
    "name/text()", "name",
    "type/text()", "type",
    "regularPrice/text()", "regularPrice",
    "salePrice/text()", "salePrice",
    "onSale/text()", "onSale",
    "salesRankShortTerm/text()", "salesRankShortTerm",
    "salesRankMediumTerm/text()", "salesRankMediumTerm",
    "salesRankLongTerm/text()", "salesRankLongTerm",
    "bestSellingRank/text()", "bestSellingRank",
    "url/text()", "url",
    "categoryPath/*/name/text()", "categoryPath",  # Note the match all here to get the subfields
    "categoryPath/*/id/text()", "categoryPathIds",  # Note the match all here to get the subfields
    "categoryPath/category[last()]/id/text()", "categoryLeaf",
    "count(categoryPath/*/name)", "categoryPathCount",
    "customerReviewCount/text()", "customerReviewCount",
    "customerReviewAverage/text()", "customerReviewAverage",
    "inStoreAvailability/text()", "inStoreAvailability",
    "onlineAvailability/text()", "onlineAvailability",
    "releaseDate/text()", "releaseDate",
    "shortDescription/text()", "shortDescription",
    "class/text()", "class",
    "classId/text()", "classId",
    "department/text()", "department",
    "departmentId/text()", "departmentId",
    "bestBuyItemId/text()", "bestBuyItemId",
    "description/text()", "description",
    "manufacturer/text()", "manufacturer",
    "modelNumber/text()", "modelNumber",
    "image/text()", "image",
    "longDescription/text()", "longDescription",
    "longDescriptionHtml/text()", "longDescriptionHtml",
    "features/*/text()", "features"  # Note the match all here to get the subfields
]

@lru_cache()
def get_opensearch():
    host = 'localhost'
    port = 9200
    auth = ('admin', 'admin')

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True,
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
    )


    return client

def index_file(file, index_name):
    client = get_opensearch()

    logger.info(f'Processing file : {file}')
    tree = etree.parse(file)
    root = tree.getroot()
    children = root.findall("./product")
    docs = []

    for child in children:
        doc = {}
        for idx in range(0, len(mappings), 2):
            xpath_expr = mappings[idx]
            key = mappings[idx + 1]
            doc[key] = child.xpath(xpath_expr)

        if not 'productId' in doc or len(doc['productId']) == 0:
            continue

        doc["_index"] = index_name
        docs.append(doc)

    logger.info(f"Indexing: {len(docs)}")
    batched_docs = list(chunks(docs, 2000))

    for batch in batched_docs:
        response = bulk(client, docs)

    return file

@click.command()
@click.option('--source_dir', '-s', default="/workspace/datasets/product_data/products", help='XML files source directory')
@click.option('--index_name', '-i', default="bbuy_products", help="The name of the index to write to")
def main(source_dir: str, index_name: str):
    pool = ProcessPoolExecutor(max_workers=12)

    files = glob.glob(source_dir + "/*.xml")
    futures = []

    # Files are independent, worth processing them in parallel for speed-up (~5x)
    for file in files:
        future = pool.submit(index_file, file, index_name)
        futures.append(future)

    for future in tqdm(futures):
        logger.info(f"Indexed: {future.result()}")

if __name__ == "__main__":
    main()
