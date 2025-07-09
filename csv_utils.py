import polars as pl
import tempfile
import time
import logging
import csv
from datetime import datetime

from postgres_utils import copy_to_postgres

def batch_from_csv_file(csv_path, batch_size=1000):
    with open(csv_path, "r", encoding="utf-8") as f:
        header = next(csv.reader([f.readline().strip()]))
    schema = {col: pl.Utf8 for col in header}
    return pl.read_csv_batched(
        csv_path,
        encoding="utf8",
        truncate_ragged_lines=False,
        batch_size=batch_size,
        schema_overrides=schema,
    )

def download_csv_from_blob_batched(blob_service_client, container_name, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        if blob_name.endswith(".xlsx"):
            with tempfile.NamedTemporaryFile(delete=True, suffix=".xlsx") as tmp_xlsx, \
                 tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp_csv:
                stream = blob_client.download_blob()
                for chunk in stream.chunks():
                    tmp_xlsx.write(chunk)
                tmp_xlsx.flush()
                sheet_one = pl.read_excel(tmp_xlsx.name, sheet_id=1)
                sheet_one.write_csv(tmp_csv.name)
                tmp_csv.flush()
                return batch_from_csv_file(tmp_csv.name)
        else:
            with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp:
                stream = blob_client.download_blob()
                for chunk in stream.chunks():
                    tmp.write(chunk)
                tmp.flush()
                return batch_from_csv_file(tmp.name)
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")

def process_csv(blob_service_client, container_name, blob_name, engine, table_name):
    logging.info(f'\nAttempting to download "{blob_name}" from container "{container_name}"...')
    csv_download_start = time.perf_counter()
    csv_batches = download_csv_from_blob_batched(blob_service_client, container_name, blob_name)
    csv_download_end = time.perf_counter()
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
            "download_time": float(csv_download_end - csv_download_start),
            "copy_time": float(csv_copy_total_end - csv_copy_total_start),
            "timestamp": datetime.now().isoformat(),
        }