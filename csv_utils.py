import polars as pl
import tempfile
import time
import logging
import csv

from postgres_utils import copy_to_postgres

def download_csv_from_blob_batched(blob_service_client, container_name, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp:
            stream = blob_client.download_blob()
            for chunk in stream.chunks():
                tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            header = next(csv.reader([tmp.readline().decode("utf-8").strip()]))
            schema = {col: pl.Utf8 for col in header}
            tmp.seek(0)
            batches = pl.read_csv_batched(
                tmp.name,
                encoding="utf8",
                truncate_ragged_lines=False,
                batch_size=1000,
                schema_overrides=schema,
            )
        return batches
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")

def process_csv(blob_service_client, container_name, blob_name, engine, table_name):
    logging.info(f'Attempting to download "{blob_name}" from container "{container_name}"...')
    csv_download_start = time.perf_counter()
    csv_batches = download_csv_from_blob_batched(blob_service_client, container_name, blob_name)
    csv_download_end = time.perf_counter()
    logging.info(f"Total download time: {csv_download_end - csv_download_start:.2f}s")
    if csv_batches is not None:
        csv_copy_total_start = time.perf_counter()       
        batch_num = 1
        total_records = 0
        while True:
            csv_batch_list = csv_batches.next_batches(1)
            if not csv_batch_list:
                break
            for pl_df in csv_batch_list:
                csv_copy_start = time.perf_counter()
                copy_to_postgres(pl_df, engine, table_name)
                csv_copy_end = time.perf_counter()
                logging.info(
                    f"Batch {batch_num}: Records: {pl_df.shape[0]} Duration: {csv_copy_end - csv_copy_start:.2f}s"
                )            
                batch_num += 1
                total_records += pl_df.shape[0]
                del pl_df
        csv_copy_total_end = time.perf_counter()        
        return {
            "file": blob_name,
            "records": total_records,
            "batches": batch_num - 1,
            "download_time": csv_download_end - csv_download_start,
            "copy_time": csv_copy_total_end - csv_copy_total_start,
        }