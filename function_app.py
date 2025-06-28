import os
import polars as pl
import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import csv
import tempfile
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_blob_service_client():
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_string)

def download_file_from_blob_all(blob_service_client, container_name, blob_name):
    try:
        start_time = time.perf_counter()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_bytes = blob_client.download_blob().readall()
        df = pl.read_csv(blob_bytes, encoding='utf8', batch_size=250, schema_overrides={
            "Provider License Number_6": pl.Utf8,
            "Other Provider Identifier_8": pl.Utf8
        })
        end_time = time.perf_counter()
        print(f'{end_time - start_time}s')
        return df
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")

def download_file_from_blob_batched(blob_service_client, container_name, blob_name):
    # Add an IF to be able to handle .xlsx and .csv (part3)
    try:
        start_time = time.perf_counter()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_bytes = blob_client.download_blob().readall()
        header = next(csv.reader([blob_bytes.decode('utf-8').split('\n', 1)[0]]))
        schema = {col: pl.Utf8 for col in header}
        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp:
            tmp.write(blob_bytes)
            tmp.flush()
            batches = pl.read_csv_batched(
                tmp.name,
                encoding='utf8',
                truncate_ragged_lines=False,
                batch_size=250,
                schema_overrides=schema
            )
        end_time = time.perf_counter()
        print(f'{end_time - start_time}s')            
        return batches
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")

def push_to_postgres(dataframe, engine, blob_name):
    table_name = str(f'{blob_name}').replace('.csv','')
    dataframe.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    dataframe.to_sql(name=table_name, con=engine, if_exists='append', index=False)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Connecting to Azure Blob Service Client...')
        blob_service_client = get_blob_service_client()
        container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME')
    except Exception as e:
        logging.error(f"Error fetching connection to Azure Blob Service Client: {e}")

    files_to_process = [
        ("nppes_sample.csv")
    ]

    # No Batching
    try:
        for blob_name in files_to_process:
            df = download_file_from_blob_all(blob_service_client, container_name, blob_name)
    except Exception as e:
        logging.error(f"Error downloading {blob_name} from Azure Blob Storage: {e}")
        return func.HttpResponse("Failed", status_code=400)

    # Batching
    try:
        for blob_name in files_to_process:
            logging.info(f'Attempting to download "{blob_name}" from container "{container_name}"...')
            batches = download_file_from_blob_batched(blob_service_client, container_name, blob_name)
            logging.info(f'Beginning batch processing...')
            if batches is not None:
                i = 1
                total_records = 0
                while True:
                    batch_list = batches.next_batches(1)
                    if not batch_list:
                        break
                    for batch in batch_list:
                        logging.info(f'Batch {i}: {batch.shape[0]} records')
                        total_records += batch.shape[0]
                        i += 1
                logging.info(f"{total_records} records processed in {i - 1} batches")
    except Exception as e:
        logging.error(f"Error downloading {blob_name} from Azure Blob Storage: {e}")
        return func.HttpResponse("Failed", status_code=400)
    
    return func.HttpResponse("Success", status_code=200) 

    
