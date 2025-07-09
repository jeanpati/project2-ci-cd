import polars as pl
import pyarrow.parquet as pq
import logging
import time
from datetime import datetime
from io import BytesIO

from utils.postgres_utils import copy_to_postgres


def download_parquet_from_blob(blob_service_client, container_name, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        stream = blob_client.download_blob()
        blob_data = stream.readall()
        parquet_buffer = BytesIO(blob_data)
        parquet_file = pq.ParquetFile(parquet_buffer)
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            pl_df = pl.from_arrow(table)
            yield pl_df
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")


def process_parquet(blob_service_client, container_name, blob_name, engine, table_name):
    logging.info(
        f'\nAttempting to download "{blob_name}" from container "{container_name}"...'
    )
    parquet_download_start = time.perf_counter()
    parquet_batches = download_parquet_from_blob(
        blob_service_client, container_name, blob_name
    )
    parquet_download_end = time.perf_counter()
    if parquet_batches is not None:
        parquet_copy_total_start = time.perf_counter()
        batch_num = 1
        total_records = 0
        for pl_df in parquet_batches:
            parquet_copy_start = time.perf_counter()
            copy_to_postgres(pl_df, engine, table_name)
            parquet_copy_end = time.perf_counter()
            logging.info(
                f"Batch {batch_num}: Records: {pl_df.shape[0]} Duration: {parquet_copy_end - parquet_copy_start:.2f}s"
            )
            batch_num += 1
            total_records += pl_df.shape[0]
        parquet_copy_total_end = time.perf_counter()
        return {
            "file": blob_name,
            "records": total_records,
            "batches": batch_num - 1,
            "download_time": float(parquet_download_end - parquet_download_start),
            "copy_time": float(parquet_copy_total_end - parquet_copy_total_start),
            "timestamp": datetime.now().isoformat(),
        }
