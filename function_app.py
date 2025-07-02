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
from io import BytesIO, StringIO
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
        start_time = time.perf_counter()
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
        end_time = time.perf_counter()
        print(f"{end_time - start_time}s")
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
                batch_size=5000,
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


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    # Mutes azure function logs
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
        logging.WARNING
    )
    logging.getLogger("azure.storage").setLevel(logging.WARNING)

    files_to_process = ["nppes_raw.parquet"]
    # nppes_raw.parquet
    # nppes_sample.csv
    # npidata_pfile_20050523-20250413.csv

    try:
        logging.info("Connecting to Azure Blob Service Client...")
        blob_service_client = get_blob_service_client()
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    except Exception as e:
        logging.error(f"Error fetching connection to Azure Blob Service Client: {e}")

    try:
        for blob_name in files_to_process:
            table_name = (
                str(f"{blob_name}")
                .replace("-", "_")
                .replace(".csv", "")
                .replace(".parquet", "")
            )
            if blob_name.endswith(".parquet"):
                logging.info(
                    f'Attempting to download "{blob_name}" from container "{container_name}"...'
                )
                overall_start = time.perf_counter()
                parquet_download_start = time.perf_counter()
                parquet_batches = download_parquet_from_blob(
                    blob_service_client, container_name, blob_name
                )
                parquet_download_end = time.perf_counter()
                logging.info(
                    f"Total download time: {parquet_download_end - parquet_download_start:.2f} seconds"
                )
                batch_num = 1
                batch_times = []
                for pl_df in parquet_batches:
                    batch_start = time.perf_counter()
                    copy_to_postgres(pl_df, engine, table_name)
                    batch_end = time.perf_counter()
                    batch_times.append(batch_end - batch_start)
                    logging.info(
                        f"Batch {batch_num} start: {batch_start:.2f} end: {batch_end:.2f} duration: {batch_end - batch_start:.2f} seconds"
                    )
                    batch_num += 1
                overall_end = time.perf_counter()
                logging.info(
                    f"Total overall time: {overall_end - overall_start:.2f} seconds"
                )
            elif blob_name.endswith(".csv"):
                logging.info(
                    f'Attempting to download "{blob_name}" from container "{container_name}"...\n'
                )
                start_time_batching = time.perf_counter()
                batches = download_csv_from_blob_batched(
                    blob_service_client, container_name, blob_name
                )
                end_time_batching = time.perf_counter()

                logging.info(f"Beginning batch processing...")
                if batches is not None:
                    start_time_copying = time.perf_counter()
                    i = 1
                    total_records = 0
                    postgres_count = 0
                    width = 50
                    fixed_offset = 25
                    while True:
                        batch_list = batches.next_batches(1)
                        if not batch_list:
                            break
                        for polars_df in batch_list:
                            pos = i % (2 * width)
                            if pos >= width:
                                pos = 2 * width - pos
                            zigzag = " " * fixed_offset + " " * pos + "˚∆˚"
                            logging.info(
                                f"Batch {i}: {polars_df.shape[0]} records {zigzag}"
                            )
                            try:
                                if i == 1:
                                    copy_to_postgres(polars_df, engine, table_name)
                                    logging.info(
                                        f"Uploaded batch: {i} to postgres with headers sucessfully!"
                                    )
                                    postgres_count += 1
                                else:
                                    copy_to_postgres(polars_df, engine, table_name)
                                    logging.info(
                                        f"Uploaded batch: {i} to postgres sucessfully!"
                                    )
                                    postgres_count += 1

                            except Exception as e:
                                return func.HttpResponse(
                                    f"Error copying batch: {i} to postgres: {e}",
                                    status_code=400,
                                )
                            total_records += polars_df.shape[0]
                            i += 1
                            del polars_df

                    end_time_copying = time.perf_counter()
                    logging.info(
                        f"""Summary:\n
                                Records: {total_records}\n
                                Batches: {i - 1}\n
                                Download & Batching: {(end_time_batching - start_time_batching)/60} min\n
                                Copying to Postgres: {(end_time_copying - start_time_copying)/60} min\n"""
                    )
    except Exception as e:
        logging.error(f"Error downloading {blob_name} from Azure Blob Storage: {e}")
        return func.HttpResponse("Failed", status_code=400)

    return func.HttpResponse("Success", status_code=200)
