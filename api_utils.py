import requests
import json
import polars as pl
import logging
import time

from postgres_utils import copy_to_postgres

def get_data_from_api_endpoint(url):
    data = requests.get(url).json()
    columns = data[0]
    rows = data[1:]
    pl_df = pl.DataFrame(rows, schema=columns)
    return pl_df

def process_json(url, engine, table_name):
    logging.info(f'\nAttempting to GET data from census API endpoint...')
    json_download_start = time.perf_counter()
    data = get_data_from_api_endpoint(url)
    json_download_end = time.perf_counter()
    total_records = data.shape[0]
    if data is not None:
        json_copy_total_start = time.perf_counter()
        copy_to_postgres(data, engine, table_name)
        json_copy_total_end = time.perf_counter()
    return {
        "file": url,
        "records": total_records,
        "batches": 1,
        "download_time": json_download_end - json_download_start,
        "copy_time": json_copy_total_end - json_copy_total_start,
    }
        