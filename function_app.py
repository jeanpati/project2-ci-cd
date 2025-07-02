import os
import io
import polars as pl
import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import csv
import tempfile
import time
from io import BytesIO
import pyarrow.parquet as pq


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def create_postgres_engine():
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    host = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    db = os.getenv("DATABASE_DB")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


engine = create_engine(create_postgres_engine(), echo=False)
session_local = sessionmaker(bind=engine)


def get_blob_service_client():
    conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_string)


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


def generate_ddl_from_polars(df: pl.DataFrame, table_name: str):
    type_mapping = {
        pl.Utf8: "TEXT",
        pl.Int64: "BIGINT",
        pl.Int32: "INTEGER",
        pl.Float64: "DOUBLE PRECISION",
        pl.Boolean: "BOOLEAN",
        pl.Datetime: "TIMESTAMP",
    }
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    for name, dtype in df.schema.items():
        sql_type = type_mapping.get(dtype, "TEXT")
        # Quote column names to handle spaces and special characters
        ddl += f'  "{name}" {sql_type},\n'
    ddl = ddl.rstrip(",\n") + "\n);"
    return ddl


def copy_to_postgres(polars_df, engine, table_name):
    insp = inspect(engine)
    db_tables = insp.get_table_names()
    if table_name not in db_tables:
        ddl = generate_ddl_from_polars(polars_df, table_name)
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()

    buffer = io.StringIO()
    polars_df.write_csv(buffer, include_header=False)
    buffer.seek(0)
    with engine.begin() as conn:
        raw_conn = conn.connection
        with raw_conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)

def process_parquet(blob_service_client, container_name, blob_name, engine, table_name):
    logging.info(f'Attempting to download "{blob_name}" from container "{container_name}"...')
    parquet_download_start = time.perf_counter()
    parquet_batches = download_parquet_from_blob(blob_service_client, container_name, blob_name)
    parquet_download_end = time.perf_counter()
    logging.info(f"Total download time: {parquet_download_end - parquet_download_start:.2f}s")
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
            "download_time": parquet_download_end - parquet_download_start,
            "copy_time": parquet_copy_total_end - parquet_copy_total_start,
        }

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


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure.storage").setLevel(logging.WARNING)

    overall_start = time.perf_counter()

    files_to_process = [
        "nppes_raw.parquet",
        "nppes_sample.csv"
    ]

    try:
        logging.info("Connecting to Azure Blob Service Client...")
        blob_service_client = get_blob_service_client()
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    except Exception as e:
        logging.error(f"Error fetching connection to Azure Blob Service Client: {e}")
        return func.HttpResponse("Failed", status_code=400)

    try:
        for blob_name in files_to_process:           
            table_name = (
                str(f"{blob_name}")
                .replace("-", "_")
                .replace(".csv", "")
                .replace(".parquet", "")
            )
            if blob_name.endswith(".parquet"):
                process_parquet(blob_service_client, container_name, blob_name, engine, table_name)
            elif blob_name.endswith(".csv"):
                process_csv(blob_service_client, container_name, blob_name, engine, table_name)
    except Exception as e:
        logging.error(f"Error processing {blob_name}: {e}")
        return func.HttpResponse("Failed", status_code=400)
    
    overall_end = time.perf_counter()
    logging.info(f"Total Function Time: {overall_end - overall_start:.2f}s")

    return func.HttpResponse("Success", status_code=200)
