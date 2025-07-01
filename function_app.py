import os
import polars as pl
import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.orm import sessionmaker
import csv
import tempfile
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def create_postgres_engine():
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    host = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    db = os.getenv("DATABASE_DB")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


engine = create_engine(create_postgres_engine(), echo=True)
metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)
session_local = sessionmaker(bind=engine)


def get_blob_service_client():
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_string)


def download_file_from_blob_batched(blob_service_client, container_name, blob_name):
    try:
        start_time = time.perf_counter()
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        with tempfile.NamedTemporaryFile(delete=True, suffix=".csv") as tmp:
            # Stream download to file
            stream = blob_client.download_blob()
            for chunk in stream.chunks():
                tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            # Read header for schema
            header = next(csv.reader([tmp.readline().decode("utf-8").strip()]))
            schema = {col: pl.Utf8 for col in header}
            tmp.seek(0)
            batches = pl.read_csv_batched(
                tmp.name,
                encoding="utf8",
                truncate_ragged_lines=False,
                batch_size=10000,
                schema_overrides=schema,
            )
        end_time = time.perf_counter()
        print(f"{end_time - start_time}s")
        return batches
    except Exception as e:
        print(f"Error processing {blob_name} from Azure Blob Storage: {e}")


def push_to_postgres(dataframe, engine, blob_name):
    table_name = str(f"{blob_name}").replace(".csv", "")
    insp = inspect(engine)
    db_tables = insp.get_table_names()
    if table_name not in db_tables:
        dataframe.head(0).to_sql(
            name=table_name, con=engine, if_exists="replace", index=False
        )
    with engine.begin() as conn:
        dataframe.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Connecting to Azure Blob Service Client...")
        blob_service_client = get_blob_service_client()
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    except Exception as e:
        logging.error(f"Error fetching connection to Azure Blob Service Client: {e}")

    files_to_process = ["nppes_sample.csv"]

    try:
        for blob_name in files_to_process:
            logging.info(
                f'Attempting to download "{blob_name}" from container "{container_name}"...'
            )
            batches = download_file_from_blob_batched(
                blob_service_client, container_name, blob_name
            )
            logging.info(f"Beginning batch processing...")
            if batches is not None:
                i = 1
                total_records = 0
                postgres_count = 0
                while True:
                    batch_list = batches.next_batches(1)
                    if not batch_list:
                        break
                    for batch in batch_list:
                        logging.info(f"Batch {i}: {batch.shape[0]} records")
                        try:
                            push_to_postgres(batch.to_pandas(), engine, blob_name)
                            logging.info(
                                f"Uploaded batch: {i} to postgres sucessfully!"
                            )
                            postgres_count += 1
                        except Exception as e:
                            logging.error(
                                f"Error uploading batch: {i} to postgres: {e}"
                            )
                        total_records += batch.shape[0]
                        i += 1
                logging.info(f"{total_records} records processed in {i - 1} batches")
                logging.info(f"{postgres_count} batches uploaded to Postgres")
    except Exception as e:
        logging.error(f"Error downloading {blob_name} from Azure Blob Storage: {e}")
        return func.HttpResponse("Failed", status_code=400)

    return func.HttpResponse("Success", status_code=200)
